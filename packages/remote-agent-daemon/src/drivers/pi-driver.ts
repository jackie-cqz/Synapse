import { execFileSync } from "node:child_process"
import process from "node:process"
import {
  DefaultResourceLoader,
  SessionManager,
  createAgentSession,
  defineTool,
  getAgentDir,
} from "@earendil-works/pi-coding-agent"
import type {
  AgentSession as PiSdkAgentSession,
  AgentSessionEvent as PiSdkSessionEvent,
  ToolDefinition as PiToolDefinition,
} from "@earendil-works/pi-coding-agent"
import type { McpServerConfig as PiMcpServerConfig } from "@earendil-works/pi-coding-agent"
import { Type } from "typebox"
import type { McpServerConfig } from "@anthropic-ai/claude-agent-sdk"
import type {
  AgentDriver,
  AgentSession,
  AgentSessionEvent,
  DaemonRuntimeCapabilities,
  PermissionDecision,
  RuntimeCatalogEntry,
  SendPromptOptions,
  SessionSpec,
} from "./types.js"
import { RUNTIME_KIND } from "./types.js"
import { EventQueue as EventQueueBase, whichBinary } from "./async-channel.js"
import {
  buildAskQuestionToolResult,
  extractPiAssistantText,
  parseAskUserQuestionInput,
} from "./pi-driver-events.js"

/**
 * pi driver — bridges the pi coding agent (https://pi.dev) into a Synapse
 * conversation through the in-process TypeScript SDK
 * (`@earendil-works/pi-coding-agent`), the same pattern the Claude driver
 * uses for claude-agent-sdk. No subprocess is spawned for the agent itself;
 * the optional `pi` binary on PATH is only a catalog signal (version probe).
 *
 * Event mapping:
 *   session_started      ← sdk session id after createAgentSession resolves
 *   assistant_message    ← message_end (assistant messages with text content)
 *   turn_completed       ← agent_end (willRetry=false), one per dispatched
 *                          prompt — retry continuations (agent_end with
 *                          willRetry=true) and stray runs are not terminals
 *                          (§5d exactly-once turn completion)
 *   user_input_requested ← the ask_question custom tool suspends until
 *                          respondPermission resolves it
 *   error                ← prompt() rejection / auto_retry_end(success=false)
 *
 * MCP: spec.mcpServers is wired via an inline DefaultResourceLoader extension
 * calling pi.registerMcpServer (exposure "direct" so the reverse-MCP tools are
 * declared to the model, not just codemode-reachable).
 *
 * Sessions persist as pi session files under ~/.pi/agent/sessions/<encoded-
 * cwd>/ (cwd is per-conversation, so sessions never collide). resumeSessionId
 * reopens the previous session file via SessionManager.findById/open.
 */

function trimFirstLine(value: string) {
  return value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find(Boolean)
}

function detectPiBinary(): { path?: string; version?: string } {
  const explicit = process.env.SYNAPSE_PI_PATH?.trim()
  const path = explicit || whichBinary("pi")
  if (!path) return {}
  let version: string | undefined
  try {
    version = trimFirstLine(
      execFileSync(path, ["--version"], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "pipe"],
        shell: process.platform === "win32",
      })
    )
  } catch {
    // version probe is best-effort
  }
  return { path, version }
}

class EventQueue extends EventQueueBase<AgentSessionEvent> {}

/** Map a claude-sdk-shaped McpServerConfig to pi's McpServerConfig. */
function toPiMcpServerConfig(config: McpServerConfig): PiMcpServerConfig {
  // exposure "direct" declares the server's tools to the model (pi's default
  // "codemode" exposure hides them from the LLM).
  if (config.type === "stdio") {
    return {
      type: "stdio",
      command: config.command,
      args: config.args as string[] | undefined,
      env: config.env as Record<string, string> | undefined,
      exposure: "direct",
    }
  }
  if (config.type === "http") {
    return {
      type: "http",
      url: config.url,
      headers: config.headers as Record<string, string> | undefined,
      exposure: "direct",
    }
  }
  // SSE / in-process SDK servers never appear in the daemon's spec (the
  // reverse-MCP bridge is always http); disable anything else rather than
  // guessing a transport.
  return { type: "http", url: "", enabled: false, exposure: "hidden" }
}

type PendingInputWaiter = (decision: PermissionDecision) => void

export class PiAgentSession implements AgentSession {
  readonly runtimeKind = RUNTIME_KIND.PI

  private readonly eventQueue = new EventQueue()
  private readonly pendingInputs = new Map<string, PendingInputWaiter>()
  private sdk: PiSdkAgentSession | null = null
  private unsubscribe: (() => void) | null = null
  private piSessionId: string | undefined
  private closed = false
  /**
   * Turns the runtime dispatched (seed prompt + every send()) that have not
   * yet reached their terminal. Each dispatched turn expects exactly one
   * agent_end(willRetry=false); stray runs (e.g. a queued followUp pi
   * delivered on its own) must not fabricate terminals (§5d).
   */
  private dispatchedTurns = 0

  constructor(
    readonly remoteAgentId: string,
    readonly conversationId: string,
    initialSessionId: string | undefined
  ) {
    this.piSessionId = initialSessionId
  }

  get sessionId() {
    return this.piSessionId
  }

  attach(sdk: PiSdkAgentSession) {
    this.sdk = sdk
    this.unsubscribe = sdk.subscribe((event) => {
      try {
        this.handleSdkEvent(event)
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        this.eventQueue.push({
          kind: "error",
          message: `pi event handler threw: ${message}`,
        })
      }
    })
    if (sdk.sessionId && sdk.sessionId !== this.piSessionId) {
      this.piSessionId = sdk.sessionId
      this.eventQueue.push({
        kind: "session_started",
        sessionId: sdk.sessionId,
      })
    }
  }

  private handleSdkEvent(event: PiSdkSessionEvent) {
    switch (event.type) {
      case "message_end": {
        // Only assistant text is conversation-visible; user echoes, tool
        // results, and thinking-only messages are not.
        const text = extractPiAssistantText(event.message)
        if (text) {
          this.eventQueue.push({ kind: "assistant_message", text })
        }
        return
      }
      case "agent_end": {
        // A retry continuation keeps the turn alive — its terminal is the
        // FINAL attempt's agent_end (willRetry=false).
        if (event.willRetry) return
        if (this.dispatchedTurns > 0) {
          this.dispatchedTurns -= 1
          this.eventQueue.push({ kind: "turn_completed" })
        }
        // A stray run with no dispatched turn is ignored — the runtime's
        // terminal guard tolerates nothing here, and fabricating a terminal
        // would advance the gate past a live turn.
        return
      }
      case "auto_retry_end": {
        if (!event.success) {
          const message = event.finalError ?? "unknown error"
          this.eventQueue.push({
            kind: "error",
            message: `pi auto-retry exhausted: ${message}`,
          })
        }
        break
      }
      default:
        break
    }
  }

  async send(prompt: string, _options?: SendPromptOptions) {
    if (!prompt.trim()) return
    const sdk = this.sdk
    if (!sdk || this.closed) {
      throw new Error("pi session is not active")
    }
    this.dispatchedTurns += 1
    // The conversation runtime serializes turns, so a dispatch normally lands
    // while the session is idle. followUp covers the race where the previous
    // run is still settling: pi queues it and runs it as the next agent run,
    // whose agent_end becomes this turn's terminal.
    try {
      if (sdk.isStreaming) {
        await sdk.followUp(prompt)
      } else {
        await sdk.prompt(prompt)
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      this.eventQueue.push({
        kind: "error",
        message: `pi prompt failed: ${message}`,
      })
      if (this.dispatchedTurns > 0) {
        this.dispatchedTurns -= 1
        this.eventQueue.push({ kind: "turn_completed" })
      }
    }
  }

  async respondPermission(
    requestId: string,
    decision: PermissionDecision
  ): Promise<void> {
    const waiter = this.pendingInputs.get(requestId)
    if (!waiter) {
      throw new Error(`No pending user-input request with id ${requestId}`)
    }
    this.pendingInputs.delete(requestId)
    waiter(decision)
  }

  events(): AsyncIterable<AgentSessionEvent> {
    return this.eventQueue.iterator()
  }

  async close(_reason: string) {
    if (this.closed) return
    this.closed = true
    for (const [id, waiter] of this.pendingInputs) {
      waiter({ behavior: "deny", message: "session closed" })
      this.pendingInputs.delete(id)
    }
    this.unsubscribe?.()
    this.unsubscribe = null
    const sdk = this.sdk
    this.sdk = null
    try {
      await sdk?.abort()
    } catch {
      // ignore — abort is best-effort teardown of the in-flight run
    }
    sdk?.dispose()
    this.eventQueue.close()
  }

  /**
   * The ask_question tool: suspends the agent run until the Synapse-side task
   * card resolves, then returns the user's answers as the tool result.
   * (Exported for unit tests; production callers reach it via createSession.)
   */
  buildAskQuestionTool(): PiToolDefinition {
    return defineTool({
      name: "ask_question",
      label: "Ask the user",
      description:
        "Ask the human user one or more questions (max 4) and wait for their " +
        "answers. Use this whenever you need a decision, clarification, or " +
        "confirmation that only the user can provide — do not guess. Each " +
        "question has a short `question` text and may offer `options` " +
        "({label, description}) the user can pick from.",
      promptSnippet:
        "ask_question: ask the user questions and wait for answers",
      parameters: Type.Object({
        questions: Type.Array(
          Type.Object({
            question: Type.String({ description: "The question to ask" }),
            options: Type.Optional(
              Type.Array(
                Type.Object({
                  label: Type.String(),
                  description: Type.Optional(Type.String()),
                })
              )
            ),
            multiple: Type.Optional(
              Type.Boolean({
                description: "Allow selecting multiple options",
              })
            ),
          }),
          { minItems: 1, maxItems: 4 }
        ),
      }),
      execute: async (_toolCallId, params) => {
        const parsed = parseAskUserQuestionInput(params)
        const requestId = `pi-ask-${this.conversationId}-${Math.random()
          .toString(36)
          .slice(2, 10)}`
        const decision = await new Promise<PermissionDecision>((resolve) => {
          this.pendingInputs.set(requestId, resolve)
          this.eventQueue.push({
            kind: "user_input_requested",
            requestId,
            title: parsed.title,
            questions: parsed.questions,
            toolName: "ask_question",
            originalInput: parsed.originalInput,
          })
        })
        return buildAskQuestionToolResult(decision)
      },
    })
  }
}

export class PiDriver implements AgentDriver {
  readonly runtimeKind = RUNTIME_KIND.PI
  readonly label = "pi"
  readonly capabilities: DaemonRuntimeCapabilities = {
    supportsRequestUserInput: true,
    supportsPersistentSession: true,
  }

  detect(): RuntimeCatalogEntry {
    const probe = detectPiBinary()
    // The agent runs in-process via the bundled SDK; the `pi` binary is only
    // a convenience signal that the operator has pi set up on this machine.
    // Without it we still report available (auth lives in ~/.pi/agent), but
    // carry whatever probe metadata we have.
    return {
      runtimeKind: this.runtimeKind,
      executablePath: probe.path,
      status: "available",
      version: probe.version,
      metadata: probe.path
        ? undefined
        : { note: "in-process SDK; pi binary not on PATH" },
    }
  }

  async createSession(spec: SessionSpec): Promise<AgentSession> {
    const session = new PiAgentSession(
      spec.remoteAgentId,
      spec.conversationId,
      spec.resumeSessionId
    )

    const sessionManager = this.resolveSessionManager(spec)

    // Reverse-MCP (and any other injected servers) ride an inline extension:
    // pi.registerMcpServer with exposure "direct" declares the tools to the
    // model. No servers → no extension, keep the loader default.
    const mcpEntries = Object.entries(spec.mcpServers ?? {})
    const resourceLoader = new DefaultResourceLoader({
      cwd: spec.workingDirectory,
      agentDir: getAgentDir(),
      ...(mcpEntries.length > 0
        ? {
            extensionFactories: [
              (pi) => {
                for (const [name, config] of mcpEntries) {
                  pi.registerMcpServer(name, toPiMcpServerConfig(config))
                }
              },
            ],
          }
        : {}),
    })

    // pi runs in-process: the proxy overlay can only take effect via
    // process.env (pi-ai's HTTP stack reads it). The proxy is machine-level
    // config, so mutating the daemon-wide env matches its semantics.
    applyEnvOverlay(spec.childEnvOverlay)

    const { session: sdk } = await createAgentSession({
      cwd: spec.workingDirectory,
      sessionManager,
      customTools: [session.buildAskQuestionTool()],
      resourceLoader,
    })
    session.attach(sdk)

    if (spec.initialPrompt.trim()) {
      await session.send(spec.initialPrompt)
    }
    return session
  }

  private resolveSessionManager(spec: SessionSpec): SessionManager {
    if (spec.resumeSessionId) {
      const path = SessionManager.findById(
        spec.workingDirectory,
        spec.resumeSessionId
      )
      if (path) {
        // cwdOverride keeps the session bound to the (per-conversation)
        // working directory even if the file's header carries an older cwd.
        return SessionManager.open(path, undefined, spec.workingDirectory)
      }
      // Session file gone (cleaned up / different machine): fall through to a
      // fresh session rather than failing the wake — the new session id is
      // published back to the api via session_started.
    }
    return SessionManager.create(spec.workingDirectory)
  }
}

function applyEnvOverlay(
  overlay: Record<string, string | undefined> | undefined
) {
  if (!overlay) return
  for (const [key, value] of Object.entries(overlay)) {
    if (value !== undefined) {
      process.env[key] = value
    }
  }
}

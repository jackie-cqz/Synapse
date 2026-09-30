import test from "node:test"
import assert from "node:assert/strict"
import { mkdtempSync, writeFileSync, chmodSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { randomUUID } from "node:crypto"
import type {
  AgentSessionEvent,
  PermissionDecision,
} from "./types.js"
import { PiDriver } from "./pi-driver.js"

/**
 * The PiAgentSession maps a duck-typed pi SDK session (subscribe/prompt/
 * followUp/isStreaming/abort/dispose/sessionId) onto AgentSessionEvents. The
 * tests drive the captured subscribe listener by hand — no real pi runtime
 * (auth/models under ~/.pi/agent) is touched.
 */
type SdkEvent = Record<string, unknown> & { type: string }

class FakeSdkSession {
  sessionId = `sess-${randomUUID().slice(0, 8)}`
  isStreaming = false
  promptImpl: (text: string) => Promise<void> = async () => {}
  followUps: string[] = []
  prompts: string[] = []
  aborted = 0
  disposed = false
  private listener: ((event: SdkEvent) => void) | null = null

  subscribe(listener: (event: SdkEvent) => void) {
    this.listener = listener
    return () => {
      this.listener = null
    }
  }

  emit(event: SdkEvent) {
    this.listener?.(event)
  }

  async prompt(text: string) {
    this.prompts.push(text)
    await this.promptImpl(text)
  }

  async followUp(text: string) {
    this.followUps.push(text)
  }

  async abort() {
    this.aborted += 1
  }

  dispose() {
    this.disposed = true
  }
}

type SessionLike = InstanceType<typeof PiAgentSession>

import { PiAgentSession } from "./pi-driver.js"

const tick = () => new Promise((resolve) => setTimeout(resolve, 0))

class EventCollector {
  private readonly events: AgentSessionEvent[] = []
  private done: (() => void) | null = null
  private readonly finished: Promise<void>
  constructor(session: { events(): AsyncIterable<AgentSessionEvent> }) {
    this.finished = new Promise((resolve) => {
      this.done = resolve
    })
    void (async () => {
      for await (const event of session.events()) {
        this.events.push(event)
      }
      this.done?.()
    })()
  }
  async snapshot() {
    await tick()
    return [...this.events]
  }
  async end() {
    await this.finished
    return [...this.events]
  }
}

/** Narrowed view of the ask_question tool for tests (full signature has 5 args). */
type AskTool = {
  execute: (
    toolCallId: string,
    params: unknown
  ) => Promise<{
    content: Array<{ type: "text"; text: string }>
    isError?: boolean
  }>
}

function attachSession(initialSessionId?: string) {
  const session = new PiAgentSession("agent-1", "conv-1", initialSessionId)
  const sdk = new FakeSdkSession()
  session.attach(sdk as unknown as Parameters<typeof session.attach>[0])
  return { session, sdk, collector: new EventCollector(session) }
}

test("PiDriver metadata: kind, label, capabilities", () => {
  const driver = new PiDriver()
  assert.equal(driver.runtimeKind, "pi")
  assert.equal(driver.label, "pi")
  assert.deepEqual(driver.capabilities, {
    supportsRequestUserInput: true,
    supportsPersistentSession: true,
  })
})

test("PiDriver.detect prefers SYNAPSE_PI_PATH and probes --version", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "pi-detect-"))
  const script = path.join(dir, "pi")
  writeFileSync(script, "#!/bin/sh\necho 'pi 0.99.1-test'\n")
  chmodSync(script, 0o755)
  const original = process.env.SYNAPSE_PI_PATH
  process.env.SYNAPSE_PI_PATH = script
  try {
    const entry = new PiDriver().detect()
    assert.equal(entry.runtimeKind, "pi")
    // In-process SDK: always available, binary is metadata only.
    assert.equal(entry.status, "available")
    assert.equal(entry.executablePath, script)
    assert.equal(entry.version, "pi 0.99.1-test")
  } finally {
    if (original === undefined) delete process.env.SYNAPSE_PI_PATH
    else process.env.SYNAPSE_PI_PATH = original
  }
})

test("PiDriver.detect stays available without any pi binary", () => {
  const originalPath = process.env.PATH
  const originalSynapse = process.env.SYNAPSE_PI_PATH
  process.env.PATH = "/tmp/__nonexistent_path_segment_for_test"
  delete process.env.SYNAPSE_PI_PATH
  try {
    const entry = new PiDriver().detect()
    assert.equal(entry.status, "available")
    assert.equal(entry.executablePath, undefined)
    assert.equal(entry.metadata?.note, "in-process SDK; pi binary not on PATH")
  } finally {
    if (originalPath !== undefined) process.env.PATH = originalPath
    if (originalSynapse !== undefined)
      process.env.SYNAPSE_PI_PATH = originalSynapse
  }
})

test("attach emits session_started for a fresh session id only", async () => {
  const { session, sdk, collector } = attachSession()
  await session.close("done")
  const events = await collector.end()
  const starts = events.filter((event) => event.kind === "session_started")
  assert.equal(starts.length, 1)
  assert.deepEqual(starts[0], {
    kind: "session_started",
    sessionId: sdk.sessionId,
  })
})

test("attach does not re-emit session_started when resuming the same id", async () => {
  const sdk = new FakeSdkSession()
  const session = new PiAgentSession("agent-1", "conv-1", sdk.sessionId)
  const collector = new EventCollector(session)
  session.attach(sdk as unknown as Parameters<typeof session.attach>[0])
  await session.close("done")
  const events = await collector.end()
  assert.equal(
    events.filter((event) => event.kind === "session_started").length,
    0
  )
})

test("message_end maps assistant text, ignores thinking/user/tool messages", async () => {
  const { session, sdk, collector } = attachSession()
  sdk.emit({
    type: "message_end",
    message: {
      role: "assistant",
      content: [
        { type: "thinking", thinking: "hmm" },
        { type: "text", text: "Working on it" },
      ],
    },
  })
  sdk.emit({
    type: "message_end",
    message: { role: "user", content: [{ type: "text", text: "echo" }] },
  })
  sdk.emit({
    type: "message_end",
    message: {
      role: "assistant",
      content: [{ type: "thinking", thinking: "only thoughts" }],
    },
  })
  await session.close("done")
  const events = await collector.end()
  const messages = events.filter((e) => e.kind === "assistant_message")
  assert.deepEqual(messages, [
    { kind: "assistant_message", text: "Working on it" },
  ])
})

test("agent_end emits exactly one turn_completed per dispatched turn", async () => {
  const { session, sdk, collector } = attachSession()
  await session.send("do something")
  sdk.emit({ type: "agent_start" })
  // A retry continuation must NOT terminal the turn — check mid-sequence so
  // an early terminal (the willRetry guard removed) cannot hide behind the
  // final agent_end's suppression.
  sdk.emit({ type: "agent_end", messages: [], willRetry: true })
  const midSequence = await collector.snapshot()
  assert.equal(
    midSequence.filter((event) => event.kind === "turn_completed").length,
    0
  )
  sdk.emit({ type: "auto_retry_start", attempt: 1, maxAttempts: 3, delayMs: 1, errorMessage: "x" })
  sdk.emit({ type: "agent_end", messages: [], willRetry: false })
  await session.close("done")
  const events = await collector.end()
  const terminals = events.filter((event) => event.kind === "turn_completed")
  assert.equal(terminals.length, 1)
  assert.equal(sdk.prompts.length, 1)
})

test("stray agent_end with no dispatched turn is suppressed", async () => {
  const { session, sdk, collector } = attachSession()
  sdk.emit({ type: "agent_end", messages: [], willRetry: false })
  await session.close("done")
  const events = await collector.end()
  assert.equal(
    events.filter((event) => event.kind === "turn_completed").length,
    0
  )
})

test("compaction continuation defers the terminal to the LAST agent_end", async () => {
  const { session, sdk, collector } = attachSession()
  await session.send("long turn")
  sdk.emit({ type: "agent_start" })
  // First low-level run ends, then the SDK's post-run hook compacts and
  // calls agent.continue() — a second run follows within the SAME turn.
  sdk.emit({ type: "agent_end", messages: [], willRetry: false })
  // No compaction_end yet: this is a plain terminal.
  let events = await collector.snapshot()
  assert.equal(
    events.filter((event) => event.kind === "turn_completed").length,
    1
  )

  // Now the compaction path: run 1 ends after a compaction_end (the SDK
  // compacted mid-run and continued) — that intermediate agent_end must NOT
  // terminal the turn while run 2 is still going.
  await session.send("another turn")
  sdk.emit({ type: "compaction_end", reason: "threshold", result: undefined, aborted: false, willRetry: false })
  sdk.emit({ type: "agent_end", messages: [], willRetry: false })
  events = await collector.snapshot()
  assert.equal(
    events.filter((event) => event.kind === "turn_completed").length,
    1,
    "intermediate agent_end after compaction must not terminal the turn"
  )
  // The continuation run finishes — only now is the second terminal allowed.
  sdk.emit({ type: "agent_end", messages: [], willRetry: false })
  await session.close("done")
  events = await collector.end()
  assert.equal(
    events.filter((event) => event.kind === "turn_completed").length,
    2
  )
})

test("send while streaming routes through followUp", async () => {
  const { session, sdk, collector } = attachSession()
  sdk.isStreaming = true
  await session.send("next instruction")
  sdk.emit({ type: "agent_end", messages: [], willRetry: false })
  await session.close("done")
  const events = await collector.end()
  assert.deepEqual(sdk.followUps, ["next instruction"])
  assert.deepEqual(sdk.prompts, [])
  assert.equal(
    events.filter((event) => event.kind === "turn_completed").length,
    1
  )
})

test("a rejected prompt emits error + terminal for that turn", async () => {
  const { session, sdk, collector } = attachSession()
  sdk.promptImpl = async () => {
    throw new Error("model 503")
  }
  await session.send("try this")
  await session.close("done")
  const events = await collector.end()
  const error = events.find((event) => event.kind === "error")
  assert.ok(error && error.kind === "error")
  assert.match(error.message, /model 503/)
  assert.equal(
    events.filter((event) => event.kind === "turn_completed").length,
    1
  )
})

test("auto_retry_end failure emits an error event", async () => {
  const { session, sdk, collector } = attachSession()
  sdk.emit({
    type: "auto_retry_end",
    success: false,
    attempt: 3,
    finalError: "provider down",
  })
  await session.close("done")
  const events = await collector.end()
  const error = events.find((event) => event.kind === "error")
  assert.ok(error && error.kind === "error")
  assert.match(error.message, /provider down/)
})

test("ask_question suspends, surfaces user_input_requested, and resolves with answers", async () => {
  const { session, collector } = attachSession()
  const tool = session.buildAskQuestionTool() as unknown as AskTool
  const pending = tool.execute("call-1", {
    questions: [{ question: "Ship on Friday?", options: [{ label: "yes" }] }],
  })
  const snapshot = await collector.snapshot()
  const request = snapshot.find((event) => event.kind === "user_input_requested")
  assert.ok(request && request.kind === "user_input_requested")
  assert.equal(request.title, "Ship on Friday?")
  assert.equal(request.toolName, "ask_question")
  assert.equal(request.questions.length, 1)

  await session.respondPermission(request.requestId, {
    behavior: "allow",
    updatedInput: {
      questions: request.questions,
      answers: { "Ship on Friday?": "no, wait for Monday" },
    },
  })
  const result = await pending
  assert.equal(result.isError, undefined)
  assert.deepEqual(result.content, [
    { type: "text", text: "- Ship on Friday?: no, wait for Monday" },
  ])
  await session.close("done")
})

test("ask_question deny resolves as an error tool result", async () => {
  const { session, collector } = attachSession()
  const tool = session.buildAskQuestionTool() as unknown as AskTool
  const pending = tool.execute("call-2", {
    questions: [{ question: "Proceed?" }],
  })
  const snapshot = await collector.snapshot()
  const request = snapshot.find((event) => event.kind === "user_input_requested")
  assert.ok(request && request.kind === "user_input_requested")
  await session.respondPermission(request.requestId, {
    behavior: "deny",
    message: "user declined",
  })
  const result = await pending
  assert.equal(result.isError, true)
  assert.match(result.content[0].text, /user declined/)
  await session.close("done")
})

test("respondPermission throws for an unknown request id", async () => {
  const { session } = attachSession()
  await assert.rejects(
    session.respondPermission("nope", { behavior: "deny", message: "x" }),
    /No pending user-input request/
  )
  await session.close("done")
})

test("close denies pending inputs, aborts + disposes the sdk, ends the iterator", async () => {
  const { session, sdk, collector } = attachSession()
  const tool = session.buildAskQuestionTool() as unknown as AskTool
  const pending = tool.execute("call-3", {
    questions: [{ question: "Still there?" }],
  })
  const snapshot = await collector.snapshot()
  const request = snapshot.find((event) => event.kind === "user_input_requested")
  assert.ok(request && request.kind === "user_input_requested")
  await session.close("conversation ended")
  const result = await pending
  assert.equal(result.isError, true)
  assert.match(result.content[0].text, /session closed/)
  assert.equal(sdk.aborted, 1)
  assert.equal(sdk.disposed, true)
  const events = await collector.end()
  assert.ok(Array.isArray(events))
})

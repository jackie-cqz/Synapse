import type {
  AgentDriver,
  AgentSession,
  RuntimeCatalogEntry,
  SessionSpec,
} from "./types.js"
import { RUNTIME_KIND } from "./types.js"

/**
 * Stub driver for the `pi` coding agent (https://pi.dev).
 *
 * TODO(issue #10): implement via the in-process TypeScript SDK
 * (`@earendil-works/pi-coding-agent`):
 *   - detect(): probe `SYNAPSE_PI_PATH` / `which pi` for version metadata; the
 *     session itself runs in-process through the SDK.
 *   - createSession(): `createAgentSession({ cwd: spec.workingDirectory, ... })`,
 *     map `message_update`(text_delta) -> assistant_message (accumulate until
 *     message_end), `agent_end`/`turn_end` -> turn_completed, `ask_question`
 *     tool -> user_input_requested, session.sessionId -> session_started.
 *   - resume: `SessionManager` + spec.resumeSessionId.
 * Event mapping reference: packages/remote-agent-daemon/src/drivers/pi-driver-events.ts (planned).
 */
export class PiDriver implements AgentDriver {
  readonly runtimeKind = RUNTIME_KIND.PI
  readonly label = "pi"
  readonly capabilities = {
    supportsRequestUserInput: true,
    supportsPersistentSession: true,
  }

  detect(): RuntimeCatalogEntry {
    return {
      runtimeKind: this.runtimeKind,
      status: "missing_binary",
      lastError: "pi driver not implemented yet (issue #10)",
    }
  }

  async createSession(_spec: SessionSpec): Promise<AgentSession> {
    throw new Error("pi driver not implemented yet (issue #10)")
  }
}

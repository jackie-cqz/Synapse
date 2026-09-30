import type {
  AgentDriver,
  AgentSession,
  RuntimeCatalogEntry,
  SessionSpec,
} from "./types.js"
import { RUNTIME_KIND } from "./types.js"

/**
 * Stub driver for `opencode` (https://opencode.ai).
 *
 * TODO(issue #10): implement via subprocess NDJSON:
 *   - detect(): probe `SYNAPSE_OPENCODE_PATH` / `which opencode`, `<path> --version`.
 *   - createSession(): spawn `opencode run --format json [-s <resumeSessionId>]`
 *     with cwd = spec.workingDirectory; parse NDJSON events on stdout into
 *     AgentSessionEvent (session id -> session_started, assistant message
 *     chunks -> assistant_message, turn finish -> turn_completed, permission
 *     request -> user_input_requested). Never pass --auto.
 * Event parsing reference: packages/remote-agent-daemon/src/drivers/opencode-driver-events.ts (planned).
 */
export class OpencodeDriver implements AgentDriver {
  readonly runtimeKind = RUNTIME_KIND.OPENCODE
  readonly label = "opencode"
  readonly capabilities = {
    supportsRequestUserInput: true,
    supportsPersistentSession: true,
  }

  detect(): RuntimeCatalogEntry {
    return {
      runtimeKind: this.runtimeKind,
      status: "missing_binary",
      lastError: "opencode driver not implemented yet (issue #10)",
    }
  }

  async createSession(_spec: SessionSpec): Promise<AgentSession> {
    throw new Error("opencode driver not implemented yet (issue #10)")
  }
}

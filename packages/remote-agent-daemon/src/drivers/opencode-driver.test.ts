import test from "node:test"
import assert from "node:assert/strict"
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { OpencodeDriver } from "./opencode-driver.js"
import type { AgentSessionEvent } from "./types.js"

const FAKE_SESSION_ID = "ses_fake_1234"

function writeFakeOpencode(dir: string, name: string): string {
  const script = `#!/bin/sh
# Fake opencode for driver tests. Emits a canned --format json turn.
echo '{"type":"step_start","timestamp":1,"sessionID":"${FAKE_SESSION_ID}","part":{"type":"step-start"}}'
echo '{"type":"text","timestamp":2,"sessionID":"${FAKE_SESSION_ID}","part":{"type":"text","text":"fake reply","time":{"start":1,"end":2}}}'
echo '{"type":"step_finish","timestamp":3,"sessionID":"${FAKE_SESSION_ID}","part":{"type":"step-finish","reason":"stop"}}'
exit 0
`
  const filePath = path.join(dir, name)
  writeFileSync(filePath, script, { mode: 0o755 })
  return filePath
}

/**
 * Fake opencode that records the argv it was invoked with (one arg per line)
 * before emitting the canned NDJSON turn. Used to pin the resume contract:
 * the first run must NOT pass --session; after the session id is captured,
 * every later send must pass --session <id>.
 */
function writeArgRecordingOpencode(dir: string, name: string, argvLog: string): string {
  const script = `#!/bin/sh
printf '%s\n' "$@" > "${argvLog}"
echo '{"type":"step_start","timestamp":1,"sessionID":"${FAKE_SESSION_ID}","part":{"type":"step-start"}}'
echo '{"type":"text","timestamp":2,"sessionID":"${FAKE_SESSION_ID}","part":{"type":"text","text":"fake reply","time":{"start":1,"end":2}}}'
echo '{"type":"step_finish","timestamp":3,"sessionID":"${FAKE_SESSION_ID}","part":{"type":"step-finish","reason":"stop"}}'
exit 0
`
  const filePath = path.join(dir, name)
  writeFileSync(filePath, script, { mode: 0o755 })
  return filePath
}

async function collectSessionEvents(
  session: { events(): AsyncIterable<AgentSessionEvent>; send(p: string): Promise<void>; close(r: string): Promise<void> },
  deadlineMs = 10_000
): Promise<AgentSessionEvent[]> {
  const events: AgentSessionEvent[] = []
  const iterator = session.events()[Symbol.asyncIterator]()
  const deadline = Date.now() + deadlineMs
  while (true) {
    const remaining = deadline - Date.now()
    if (remaining <= 0) throw new Error("timed out waiting for turn_completed")
    const next = await Promise.race([
      iterator.next(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("event wait timeout")), remaining)
      ),
    ])
    if (next.done) break
    events.push(next.value)
    if (next.value.kind === "turn_completed") break
  }
  return events
}

test("OpencodeDriver metadata", () => {
  const driver = new OpencodeDriver()
  assert.equal(driver.runtimeKind, "opencode")
  assert.equal(driver.label, "opencode")
  // Honest capability advertising: no interactive permission support in run mode.
  assert.equal(driver.capabilities.supportsRequestUserInput, undefined)
  assert.equal(driver.capabilities.supportsPersistentSession, true)
})

test("OpencodeDriver.detect reports missing_binary when nothing on PATH", () => {
  const originalPath = process.env.PATH
  const originalSynapse = process.env.SYNAPSE_OPENCODE_PATH
  process.env.PATH = "/tmp/__nonexistent_path_segment_for_test_opencode"
  delete process.env.SYNAPSE_OPENCODE_PATH
  try {
    const entry = new OpencodeDriver().detect()
    assert.equal(entry.runtimeKind, "opencode")
    assert.equal(entry.status, "missing_binary")
    assert.equal(entry.executablePath, undefined)
  } finally {
    if (originalPath !== undefined) process.env.PATH = originalPath
    if (originalSynapse !== undefined)
      process.env.SYNAPSE_OPENCODE_PATH = originalSynapse
  }
})

test("OpencodeDriver.detect honors SYNAPSE_OPENCODE_PATH with a fake script", () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-detect-"))
  const fake = path.join(dir, "fake-opencode")
  writeFileSync(
    fake,
    "#!/bin/sh\necho '1.2.3-fake'\n",
    { mode: 0o755 }
  )
  const originalSynapse = process.env.SYNAPSE_OPENCODE_PATH
  process.env.SYNAPSE_OPENCODE_PATH = fake
  try {
    const entry = new OpencodeDriver().detect()
    assert.equal(entry.status, "available")
    assert.equal(entry.executablePath, fake)
    assert.equal(entry.version, "1.2.3-fake")
  } finally {
    if (originalSynapse !== undefined)
      process.env.SYNAPSE_OPENCODE_PATH = originalSynapse
    else delete process.env.SYNAPSE_OPENCODE_PATH
  }
})

test("OpencodeDriver.createSession runs the bootstrap turn and maps events", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-session-"))
  const fakeBinary = writeFakeOpencode(dir, "fake-opencode-run")
  const driver = new OpencodeDriver()

  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-1",
    workingDirectory: dir,
    initialPrompt: "hello bootstrap",
    runtimePath: fakeBinary,
  })

  try {
    assert.equal(session.runtimeKind, "opencode")
    const events = await collectSessionEvents(session)
    const kinds = events.map((event) => event.kind)
    assert.ok(kinds.includes("session_started"), `saw ${kinds.join(",")}`)
    assert.ok(kinds.includes("assistant_message"))
    assert.equal(kinds[kinds.length - 1], "turn_completed")
    const started = events.find((event) => event.kind === "session_started")
    assert.equal(
      started && started.kind === "session_started" ? started.sessionId : undefined,
      FAKE_SESSION_ID
    )
    assert.equal(session.sessionId, FAKE_SESSION_ID)
    const text = events.find((event) => event.kind === "assistant_message")
    assert.equal(text && text.kind === "assistant_message" ? text.text : "", "fake reply")
  } finally {
    await session.close("test done")
  }
})

test("OpencodeAgentSession.send() spawns a follow-up turn reusing the cached session id", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-send-"))
  const fakeBinary = writeFakeOpencode(dir, "fake-opencode-send")
  const driver = new OpencodeDriver()

  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-2",
    workingDirectory: dir,
    initialPrompt: "first",
    resumeSessionId: FAKE_SESSION_ID,
    runtimePath: fakeBinary,
  })

  try {
    // Bootstrap turn consumes the initial events; session id came pre-cached
    // from resumeSessionId, so the fake's matching sessionID must NOT emit a
    // duplicate session_started.
    const first = await collectSessionEvents(session)
    assert.ok(
      !first.some((event) => event.kind === "session_started"),
      "resumed session must not re-emit session_started for a known id"
    )
    assert.equal(session.sessionId, FAKE_SESSION_ID)

    // Follow-up send: a fresh child runs and completes another turn.
    const sendPromise = session.send("second")
    const events = await collectSessionEvents(session)
    await sendPromise
    const kinds = events.map((event) => event.kind)
    assert.ok(kinds.includes("assistant_message"))
    assert.equal(kinds[kinds.length - 1], "turn_completed")
  } finally {
    await session.close("test done")
  }
})

test("OpencodeAgentSession.respondPermission honestly refuses: run mode cannot answer permissions", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-perm-"))
  const fakeBinary = writeFakeOpencode(dir, "fake-opencode-perm")
  const driver = new OpencodeDriver()
  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-3",
    workingDirectory: dir,
    initialPrompt: "hi",
    runtimePath: fakeBinary,
  })
  try {
    await assert.rejects(
      () =>
        session.respondPermission("req-1", {
          behavior: "deny",
          message: "no",
        }),
      /not available|auto-rejects/i
    )
  } finally {
    await session.close("test done")
  }
})

test("a failing child surfaces as error + turn_completed exactly once", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-fail-"))
  const failing = path.join(dir, "failing-opencode")
  writeFileSync(
    failing,
    `#!/bin/sh\necho '{"type":"error","timestamp":1,"sessionID":"${FAKE_SESSION_ID}","error":{"name":"APIError","data":{"message":"boom"}}}'\nexit 1\n`,
    { mode: 0o755 }
  )
  const driver = new OpencodeDriver()
  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-4",
    workingDirectory: dir,
    initialPrompt: "hi",
    runtimePath: failing,
  })
  try {
    const events = await collectSessionEvents(session)
    const errors = events.filter((event) => event.kind === "error")
    const terminals = events.filter((event) => event.kind === "turn_completed")
    assert.equal(errors.length, 1)
    assert.equal(
      errors[0].kind === "error" ? errors[0].message : "",
      "boom"
    )
    assert.equal(terminals.length, 1)
  } finally {
    await session.close("test done")
  }
})

test("non-zero exit with only stderr (no JSON error) reports the stderr tail", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-stderr-"))
  const failing = path.join(dir, "stderr-opencode")
  // Mirrors the real stale-session behavior: "Session not found" on stderr, exit 1.
  writeFileSync(
    failing,
    "#!/bin/sh\necho 'Error: Session not found' >&2\nexit 1\n",
    { mode: 0o755 }
  )
  const driver = new OpencodeDriver()
  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-5",
    workingDirectory: dir,
    initialPrompt: "hi",
    runtimePath: failing,
  })
  try {
    const events = await collectSessionEvents(session)
    const error = events.find((event) => event.kind === "error")
    assert.ok(error, "expected an error event")
    if (error && error.kind === "error") {
      assert.match(error.message, /Session not found/)
      assert.match(error.message, /exited with code 1/)
    }
    assert.equal(
      events.filter((event) => event.kind === "turn_completed").length,
      1
    )
  } finally {
    await session.close("test done")
  }
})

test("missing binary surfaces as a spawn error terminal, not a wedge", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-enoent-"))
  const driver = new OpencodeDriver()
  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-6",
    workingDirectory: dir,
    initialPrompt: "hi",
    runtimePath: path.join(dir, "definitely-not-installed-opencode"),
  })
  try {
    const events = await collectSessionEvents(session)
    const kinds = events.map((event) => event.kind)
    assert.ok(kinds.includes("error"), `saw ${kinds.join(",")}`)
    assert.equal(kinds[kinds.length - 1], "turn_completed")
  } finally {
    await session.close("test done")
  }
})

test("resume contract: first run has no --session, follow-up sends pass --session <captured id>", async () => {
  const dir = mkdtempSync(path.join(tmpdir(), "opencode-argv-"))
  const argvLog = path.join(dir, "argv.log")
  const fakeBinary = writeArgRecordingOpencode(dir, "fake-opencode-argv", argvLog)
  const driver = new OpencodeDriver()

  const session = await driver.createSession({
    remoteAgentId: "agent-1",
    conversationId: "conv-7",
    workingDirectory: dir,
    initialPrompt: "first turn",
    runtimePath: fakeBinary,
  })

  try {
    // Bootstrap turn: session id is unknown, so --session must NOT be passed.
    await collectSessionEvents(session)
    const firstArgv = readArgvLog(argvLog)
    assert.ok(
      !firstArgv.includes("--session"),
      `first run must not pass --session, saw ${firstArgv.join(" ")}`
    )
    // Sanity: the run subcommand and json format are always present.
    assert.ok(firstArgv.includes("run"))
    assert.ok(firstArgv.includes("json"))
    assert.equal(session.sessionId, FAKE_SESSION_ID)

    // Follow-up send: the captured session id must ride along as --session <id>.
    const sendPromise = session.send("second turn")
    await collectSessionEvents(session)
    await sendPromise
    const secondArgv = readArgvLog(argvLog)
    const sessionFlagIndex = secondArgv.indexOf("--session")
    assert.ok(
      sessionFlagIndex !== -1,
      `follow-up run must pass --session, saw ${secondArgv.join(" ")}`
    )
    assert.equal(
      secondArgv[sessionFlagIndex + 1],
      FAKE_SESSION_ID,
      `--session must carry the captured id, saw ${secondArgv.join(" ")}`
    )
  } finally {
    await session.close("test done")
  }
})

function readArgvLog(argvLog: string): string[] {
  return readFileSync(argvLog, "utf8")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
}

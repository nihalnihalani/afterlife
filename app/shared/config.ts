import { existsSync, readFileSync } from "node:fs"
import { homedir } from "node:os"
import { resolve } from "node:path"

// Per-run state lives in afterlife/.run (gitignored): run.json, tokens/, pids/, logs/.
export const RUN_DIR = resolve(import.meta.dir, "../../.run")

export type RunConfig = {
    runId: string
    channel: string
    tasks: { id: string; title: string }[]
    workers: string[]
    standbys: string[]
}

export function loadRun(): RunConfig {
    const file = `${RUN_DIR}/run.json`
    if (!existsSync(file)) throw new Error("No run yet. Run `bun run ../scripts/reset.ts` first.")
    return JSON.parse(readFileSync(file, "utf8"))
}

const num = (name: string, fallback: number) => Number(process.env[name] ?? fallback)

export const env = {
    relayBaseUrl: process.env.RELAY_BASE_URL ?? "http://127.0.0.1:8787",
    transport: (process.env.TRANSPORT ?? "relay") as "relay" | "poll",
    llmMode: (process.env.LLM_MODE ?? (process.env.OPENAI_API_KEY ? "openai" : "canned")) as "openai" | "canned",
    openaiModel: process.env.OPENAI_MODEL ?? "gpt-6-luna",
    leaseMs: num("LEASE_MS", 10000),
    heartbeatMs: num("HEARTBEAT_MS", 2000),
    pollMs: num("POLL_MS", 1000),
    // No draft growth or checkpoint for this long means the agent is stuck, even if it heartbeats.
    progressMs: num("PROGRESS_MS", 15000),
    maxAttempts: num("MAX_ATTEMPTS", 3),
    stepPaceMs: num("STEP_PACE_MS", 6000),
    bridgePort: num("BRIDGE_PORT", 3000),
    controlPort: num("CONTROL_PORT", 3001),
    reviewer: process.env.REVIEWER !== "0",
    totalSteps: 6,
}

// The workspace key never leaves the server side and is never logged.
export function workspaceKey(): string {
    if (process.env.RELAY_WORKSPACE_KEY) return process.env.RELAY_WORKSPACE_KEY
    const file = `${homedir()}/.agentworkforce/relay/workspaces.json`
    return JSON.parse(readFileSync(file, "utf8")).workspaces.local.key
}

// Actor ids may only contain [A-Za-z0-9._-]; Relay idempotency keys may also contain ':'.
export const actorId = (runId: string, taskId: string) => `${runId}-${taskId}`
export const agentName = (runId: string, role: string) => `${runId}-${role}`
export const relayKey = (runId: string, taskId: string, attempt: number, kind: string) => `${runId}:${taskId}:${attempt}:${kind}`

export type RelayMeta = {
    v: 1
    type: "CKPT" | "LOST" | "HANDOFF" | "DONE" | "STEER" | "TOOK" | "TOMBSTONE" | "ACK" | "REVIEW"
    runId: string
    taskId: string
    attempt: number
    stepId?: number
    directiveId?: string
    from: string
    correlationId: string
}

export const correlationId = (runId: string, taskId: string, attempt: number) => `${runId}:${taskId}:${attempt}`

export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

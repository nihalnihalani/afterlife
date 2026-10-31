// Starts a fresh run: new RUN_ID (so actor ids, Relay agent names and idempotency keys never
// collide with earlier runs), clears tokens and pids, and initialises the three Task actors.
import { mkdirSync, rmSync, writeFileSync } from "node:fs"
import { actors } from "../generated/index.js"
import { RUN_DIR, actorId, env, type RunConfig } from "../shared/config"
import { FACT_SHEETS } from "../worker/data"

const runId = `r${Date.now().toString(36).slice(-6)}`
rmSync(`${RUN_DIR}/tokens`, { recursive: true, force: true })
mkdirSync(`${RUN_DIR}/logs`, { recursive: true })
const config: RunConfig = {
    runId,
    channel: `${runId}-fleet`,
    tasks: FACT_SHEETS.map(s => ({ id: s.id, title: s.title })),
    workers: ["w1", "w2", "w3"],
    standbys: ["s1", "s2"],
}
writeFileSync(`${RUN_DIR}/run.json`, JSON.stringify(config, null, 2))
for (const t of config.tasks) {
    await actors.Task.get(actorId(runId, t.id)).init({ taskId: t.id, title: t.title, totalSteps: env.totalSteps, leaseMs: env.leaseMs, maxAttempts: env.maxAttempts })
}
console.log(`run ${runId} ready: ${config.tasks.map(t => t.id).join(", ")}`)

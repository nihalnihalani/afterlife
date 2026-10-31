// Demo control: lets the board inject faults with buttons instead of terminal commands.
// Localhost only, fixed actions, agent names checked against the pid files of this run.
// It is not part of the system under test; it only sends signals and runs scripts/demo.sh.
//   bun run scripts/control.ts        (scripts/demo.sh up starts it once and leaves it running)
import { existsSync, readdirSync, readFileSync } from "node:fs"
import { resolve } from "node:path"
import { RUN_DIR, env } from "../shared/config"

const DEMO = resolve(import.meta.dir, "demo.sh")
const PIDS = `${RUN_DIR}/pids`
const BOARD = [`http://127.0.0.1:${env.bridgePort}`, `http://localhost:${env.bridgePort}`]
const hung = new Set<number>()

const SIGNALS = { kill: "SIGKILL", freeze: "SIGSTOP", resume: "SIGCONT", hang: "SIGUSR1" } as const
type Action = keyof typeof SIGNALS | "bridge-start" | "new-run"

function pidOf(name: string): number | null {
    if (!/^[a-z0-9]+$/.test(name)) return null
    const file = `${PIDS}/${name}`
    return existsSync(file) ? Number(readFileSync(file, "utf8").trim()) || null : null
}

// alive | frozen | hung | dead, from the process table.
function stateOf(pid: number): string {
    const r = Bun.spawnSync(["ps", "-o", "stat=", "-p", String(pid)])
    const stat = r.stdout.toString().trim()
    if (!stat || stat.startsWith("Z")) return "dead"
    if (stat.startsWith("T")) return "frozen"
    return hung.has(pid) ? "hung" : "alive"
}

function agents() {
    const names = existsSync(PIDS) ? readdirSync(PIDS).filter(n => /^[a-z0-9]+$/.test(n)) : []
    const run = existsSync(`${RUN_DIR}/run.json`) ? JSON.parse(readFileSync(`${RUN_DIR}/run.json`, "utf8")) : null
    return { runId: run?.runId ?? null, agents: names.sort().map(name => ({ name, state: pidOf(name) ? stateOf(pidOf(name)!) : "dead" })) }
}

function demo(...args: string[]) {
    Bun.spawn(["bash", DEMO, ...args], { stdout: "ignore", stderr: "ignore", env: process.env }).unref()
}

function act(action: Action, agent?: string) {
    if (action === "bridge-start") return demo("bridge"), { ok: true }
    if (action === "new-run") return hung.clear(), demo("up"), { ok: true }
    if (!(action in SIGNALS) || !agent) return { ok: false, reason: "bad-action" }
    const pid = pidOf(agent)
    if (!pid) return { ok: false, reason: "unknown-agent" }
    try {
        process.kill(pid, SIGNALS[action as keyof typeof SIGNALS])
    } catch {
        return { ok: false, reason: "not-running" }
    }
    if (action === "hang") hung.add(pid)
    return { ok: true }
}

Bun.serve({
    port: env.controlPort,
    hostname: "127.0.0.1",
    async fetch(req) {
        const host = req.headers.get("host") ?? ""
        if (host !== `127.0.0.1:${env.controlPort}` && host !== `localhost:${env.controlPort}`) return new Response("forbidden", { status: 403 })
        const origin = req.headers.get("origin") ?? ""
        const cors: Record<string, string> = BOARD.includes(origin) ? { "Access-Control-Allow-Origin": origin, "Access-Control-Allow-Headers": "Content-Type", "Access-Control-Allow-Methods": "GET, POST" } : {}
        if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors })
        const url = new URL(req.url)
        if (url.pathname === "/agents") return Response.json(agents(), { headers: { ...cors, "Cache-Control": "no-store" } })
        if (url.pathname === "/chaos" && req.method === "POST") {
            if (!origin || !BOARD.includes(origin)) return new Response("forbidden", { status: 403 })
            const body = (await req.json().catch(() => ({}))) as { action?: Action; agent?: string }
            return Response.json(act(body.action as Action, body.agent), { headers: cors })
        }
        return new Response("not found", { status: 404, headers: cors })
    },
})
console.log(`control on http://127.0.0.1:${env.controlPort}`)

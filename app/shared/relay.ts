import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs"
import { AgentRelay, type RelayAgentClient } from "@agent-relay/sdk"
import { RUN_DIR, env, sleep, workspaceKey, type RelayMeta } from "./config"

export const relay = () => new AgentRelay({ workspaceKey: workspaceKey(), baseUrl: env.relayBaseUrl })

// Registration is create-only (409 name_conflict on reuse), so the first boot registers and
// persists the token, and every later boot reconnects with it.
export async function joinAs(r: AgentRelay, name: string): Promise<RelayAgentClient> {
    mkdirSync(`${RUN_DIR}/tokens`, { recursive: true })
    const file = `${RUN_DIR}/tokens/${name}.token`
    if (existsSync(file)) return r.workspace.reconnect({ apiToken: readFileSync(file, "utf8").trim() })
    const client = await r.workspace.register({ name, type: "agent" })
    writeFileSync(file, client.token ?? "", { mode: 0o600 })
    return client
}

export async function joinChannel(client: RelayAgentClient, channel: string) {
    try {
        await client.channels.create({ name: channel })
    } catch {
        // Already exists.
    }
    await client.channels.join(channel).catch(() => {})
}

// Sends with an idempotency key. A 409 means the same key was already used, which we
// treat as "already sent"; we never retry with changed text.
export async function send(client: RelayAgentClient, to: string, text: string, metadata: RelayMeta, idempotencyKey: string) {
    for (const delay of [0, 500, 1000, 2000]) {
        if (delay) await sleep(delay)
        try {
            const sent = client.sendMessage({ to, text, metadata, idempotencyKey })
            return await Promise.race([sent, sleep(4000).then(() => Promise.reject(new Error("relay send timeout")))])
        } catch (e: any) {
            if (e?.status === 409) return null
            if (delay === 2000) throw e
        }
    }
    return null
}

// dm.received events arrive without metadata (G2), so the receiver reads the message back.
export async function readMeta(client: RelayAgentClient, messageId: string): Promise<(RelayMeta & Record<string, unknown>) | null> {
    for (const delay of [0, 200, 400, 800]) {
        if (delay) await sleep(delay)
        try {
            const m = await client.messages.get(messageId)
            if (m.metadata) return m.metadata as any
        } catch {
            // Retry below.
        }
    }
    return null
}

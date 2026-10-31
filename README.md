# Afterlife

**When an agent dies, its work doesn't.** Durable task leases and context handoff for long-running AI agents, built at Haunted Hacks: Real-Time AI Edition (31 Oct 2026) with Terse Durable Actors and Agent Relay.

Each long-running agent task is a Durable Actor (`Task`) that owns three things: a lease (the actor's single alarm), an attempt counter used as a fencing token, and a ledger of completed steps. Worker agents are separate processes. They renew the lease directly on their task actor and post checkpoints to an Agent Relay channel. When a worker dies or freezes, the lease alarm fires inside the actor even with no supervisor running, the task becomes `lost` with `attempt + 1`, and the bridge hands it to an idle standby agent with a Relay DM. The standby claims the new attempt and continues from the last checkpoint, with the dead worker's summaries in its prompt. If the frozen worker comes back, its stale writes are rejected and shown on the board.

## What is shown

| Beat | What happens |
|---|---|
| Kill a worker and the bridge | The task's lease expires inside the actor; the card turns red `LOST`, attempt 2, while the board shows the bridge offline |
| Restart the bridge | A `HANDOFF` DM goes to a standby over Agent Relay; the standby resumes at the next step; steps it writes are marked and show which earlier details they reused |
| Freeze, then resume a worker | Its heartbeat and checkpoint come back with `stale-attempt`; the rejection is recorded on the card and the zombie exits |
| Optional steer (`STEER_ENABLED=1`) | A human instruction is sent as a Relay DM to the current worker, recorded in the actor, and carried into the handoff |

Honest limits: the in-flight step is re-run after a handoff (at-least-once; completed steps are not redone). A crash of the actor runtime itself drops pending alarms in local dev, so the bridge also expires overdue leases and re-arms them on start. Data is synthetic.

## Run it

Requirements: Bun 1.4.2+, a running Agent Relay local server (`npx -y agent-relay@15.0.1 init`, serves `http://127.0.0.1:8787`), optional `OPENAI_API_KEY` (without it the workers use a labeled canned mode).

```bash
cd actor && bun install && bunx durable-actors dev          # terminal 1: actor runtime on :7100
cd app && bun install && bunx durable-actors generate --out-dir generated
cd app && scripts/demo.sh up                                 # bridge, standbys s1 s2, workers w1-w3
open http://127.0.0.1:3000
scripts/demo.sh kill-w2      # kill -9 worker w2 and the bridge
scripts/demo.sh stop-w3      # freeze worker w3
scripts/demo.sh bridge       # restart the bridge -> handoffs
scripts/demo.sh cont-w3      # resume the zombie -> stale write rejected
scripts/demo.sh down
```

`app/.env`: `DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100`, `RELAY_BASE_URL=http://127.0.0.1:8787`. The Relay workspace key is read from `~/.agentworkforce/relay/workspaces.json` (or `RELAY_WORKSPACE_KEY`) and never sent to the browser. Tunables: `LEASE_MS` (10000), `HEARTBEAT_MS` (2000), `STEP_PACE_MS` (6000), `LLM_MODE` (`openai`|`canned`), `OPENAI_MODEL`, `TRANSPORT` (`relay`|`poll`).

## Layout

- `actor/src/task.ts`: the `Task` Durable Actor (lease, fencing, ledger, handoff, directives).
- `app/worker/`: worker and standby agent process, LLM step writer, synthetic fact sheets.
- `app/bridge/`: stateless supervisor, Relay handoffs, board server, WebSocket grants, feed.
- `app/board/index.html`: live board (actor WebSockets plus the Relay feed).
- `app/shared/`: config, ids, Relay helpers. `app/scripts/`: reset and demo control.

## Attribution

All code in this repository was written at the event. Dependencies: [durable-actors](https://github.com/TerseAI/durable-actors) 0.14.6 (MIT), [@agent-relay/sdk](https://github.com/AgentWorkforce/relay) 15.0.1 (Apache-2.0 repository), [openai](https://github.com/openai/openai-node) 7.47.0 (Apache-2.0), [zod](https://github.com/colinhacks/zod) 4.6.7 (MIT). The Agent Relay local server (relaycast-engine 11.1.0) is run via the official `agent-relay` CLI. The fact sheets are synthetic and describe no real places.

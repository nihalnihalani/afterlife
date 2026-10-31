# Afterlife

**When an agent dies, its work doesn't.** Durable task leases and context handoff for long-running AI agents, built at Haunted Hacks: Real-Time AI Edition (31 Oct 2026) with Terse Durable Actors and Agent Relay.

Each long-running agent task is a Durable Actor (`Task`) that owns four things: a lease (the actor's single alarm), an attempt counter used as a fencing token, a ledger of completed steps, and a live draft of the step being written. Worker agents are separate processes. They renew the lease on their task actor and stream each step into it as they write. When a worker dies or freezes, the lease alarm fires inside the actor, the task becomes `lost` with `attempt + 1`, and **the actor itself pushes an offer to every standby agent connected to its WebSocket**. The first claim wins; no supervisor process is involved. The dead worker's half-written step is kept as a cut-off sentence, and the standby finishes that sentence before continuing. If the frozen worker comes back, its stale writes are rejected and shown on the board.

Over Agent Relay, agents tell each other what happened: the new owner posts `TOOK` on the `#fleet` channel and DMs a `TOMBSTONE` to the agent it replaced, the bridge announces `LOST` and `DONE`, and a human can steer the current worker by DM. A Claude Code session can join as a standby: it receives the `HANDOFF` as a Relay DM and takes the task over with a small CLI (below).

## What is shown

| Beat | What happens |
|---|---|
| Watch the agents write | Each step is typed out live on its card, streamed into the actor |
| Kill a worker **and** the bridge | The lease expires inside the actor, which offers the task to the standbys; one claims it within about 100 ms while the bridge is still dead. The dead worker's half-written sentence turns red and struck through, and the standby finishes it mid-word |
| Freeze, then resume a worker | A standby takes the task over; when the zombie wakes, its first write comes back `stale-attempt`, the rejection is recorded on the card and the zombie exits |
| A Claude Code session takes over | With `CLAUDE_STANDBY` set, the bridge DMs a `HANDOFF` prompt to that Claude Code session over Relay; the session claims the task with `app/tools/afterlife.ts` and finishes it under the same fencing |
| Optional steer (`STEER_ENABLED=1`) | A human instruction is sent as a Relay DM to the current worker, recorded in the actor, and carried into the takeover |

Honest limits: delivery is at-least-once; completed steps are never redone, and the cut-off sentence is continued, not rewritten. A crash of the actor runtime itself drops pending alarms in local dev, so standbys and the bridge also expire overdue leases, and the bridge re-arms them on start. Data is synthetic.

## Run it

Requirements: Bun 1.4.2+, a running Agent Relay local server (`npx -y agent-relay@15.0.1 init`, serves `http://127.0.0.1:8787`), optional `OPENAI_API_KEY` (without it the workers use a labeled canned mode).

```bash
cd actor && bun install && bunx durable-actors dev          # terminal 1: actor runtime on :7100
cd app && bun install && bunx durable-actors generate --out-dir generated
cd app && scripts/demo.sh up                                 # bridge, standbys s1 s2, workers w1-w3
open http://127.0.0.1:3000
scripts/demo.sh kill-w2      # kill -9 worker w2 and the bridge -> a standby takes t2 over anyway
scripts/demo.sh stop-w3      # freeze worker w3 -> a standby takes t3 over
scripts/demo.sh bridge       # restart the bridge (only the board feed needs it)
scripts/demo.sh cont-w3      # resume the zombie -> stale write rejected
scripts/demo.sh down
```

### A Claude Code session as the standby

Start the bridge with the session's Agent Relay name, and stop the scripted standbys so the session is the only one:

```bash
CLAUDE_STANDBY=<relay name of the Claude Code session> scripts/demo.sh up
kill $(cat ../.run/pids/s1) $(cat ../.run/pids/s2)
kill -9 $(cat ../.run/pids/w2)      # the bridge DMs the HANDOFF prompt to the session
```

The session receives the DM as a turn and uses the CLI, which fences every write by attempt exactly like a worker:

```bash
bun run app/tools/afterlife.ts offers                      # lost tasks waiting for a claim
bun run app/tools/afterlife.ts wait                        # or block on the actors' offers (no bridge needed)
bun run app/tools/afterlife.ts claim t2 2 --as cc          # fact sheet, saved steps, cut-off sentence; 120 s lease
bun run app/tools/afterlife.ts checkpoint t2 2 --as cc --step 2 --summary "..." --text "<cut-off text + continuation>"
bun run app/tools/afterlife.ts complete t2 2 --as cc       # any fenced write prints STOP
```

`app/.env`: `DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100`, `RELAY_BASE_URL=http://127.0.0.1:8787`. The Relay workspace key is read from `~/.agentworkforce/relay/workspaces.json` (or `RELAY_WORKSPACE_KEY`) and never sent to the browser. Tunables: `LEASE_MS` (10000), `HEARTBEAT_MS` (2000), `STEP_PACE_MS` (6000), `LLM_MODE` (`openai`|`canned`), `OPENAI_MODEL`, `CLAUDE_STANDBY`.

## Layout

- `actor/src/task.ts`: the `Task` Durable Actor (lease, fencing, ledger, live draft and cut-off sentence, offers to standby sockets, directives).
- `app/worker/`: worker and standby agent process, streaming LLM step writer, synthetic fact sheets.
- `app/bridge/`: observer: announces LOST and DONE on Relay, board server, WebSocket grants, feed, HANDOFF DM to a Claude Code standby.
- `app/tools/afterlife.ts`: CLI for agents that cannot run the worker loop, such as a Claude Code session.
- `app/board/index.html`: live board (actor WebSockets plus the Relay feed).
- `app/shared/`: config, ids, Relay helpers. `app/scripts/`: reset and demo control.

## Attribution

All code in this repository was written at the event. Dependencies: [durable-actors](https://github.com/TerseAI/durable-actors) 0.14.6 (MIT), [@agent-relay/sdk](https://github.com/AgentWorkforce/relay) 15.0.1 (Apache-2.0 repository), [openai](https://github.com/openai/openai-node) 7.47.0 (Apache-2.0), [zod](https://github.com/colinhacks/zod) 4.6.7 (MIT). The Agent Relay local server (relaycast-engine 11.1.0) is run via the official `agent-relay` CLI. The fact sheets are synthetic and describe no real places.

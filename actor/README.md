# Actors

Requires Bun 1.4.2+ on your PATH.

```sh
bun install
bunx durable-actors dev
```

Define your actors in `src/actors.ts`. The starter contains a counter with persisted state and `read()` and `increment()` methods. Each counter ID has its own saved value.

`bunx durable-actors dev` watches your actor source and stores local state in `.durable-actors/`. It defaults to project ID `local`; choose another with `DURABLE_ACTORS_PROJECT_ID=my-project bunx durable-actors dev`.

## Connect your application

In your separate application project's directory:

1. Run `bun add --dev durable-actors`.
2. Use the local defaults: project `local`, URL `http://127.0.0.1:7100`, and no secret. If you configure a different project, port, or optional secret on the actor server, put matching settings in your application’s `.env` or `.env.local` file.
3. Run `bunx durable-actors generate`; the CLI loads both environment files automatically. Exported environment variables take precedence over `.env.local`, which takes precedence over `.env`.

Your application backend can then use the generated client:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

Use `bun run --bun check` to check types. Production deployment integrations register actor images through `PUT /v1/projects/{project_id}/deployment`; see the [HTTP API](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/openapi.yaml).

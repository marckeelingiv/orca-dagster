# Architecture & Contracts — Dagster Orca Plugin

This document defines the technical contracts for agents implementing the `dagster` Orca plugin.

## 1. Plugin Manifest Contract (`orca-plugin.json`)

- **Plugin ID**: `dagster` (IDs starting with `orca-` are reserved by Orca).
- **Manifest Version**: `2`
- **Execution Profile**: `trusted-worker`
- **Engines**: `engines.orca: ">=1.4.259"` (covers `statusBarItems`, `components`, `editor.open`, `terminal.*`, `agent.launch`, `secrets.protection`).
- **Runtime**: Pure TypeScript (`main: "src/worker.ts"`, panel `entry: "src/panel.tsx"`, `ui: "components"`). Orca transpiles `.ts`/`.tsx` automatically.

### Required Capabilities

Keep capabilities minimal and justified:

| Capability | Purpose |
| --- | --- |
| `workspace:read` | Read focused workspace path/id (`workspace.readContext`, `workspace.list`) for `.orca-dagster.json`, local symbol lookup, and workspace-pinned profiles |
| `workspace:write` | Open resolved asset/job/schedule source files (`editor.open`) and focus local Dagster terminal (`terminal.focus`) |
| `terminal:send` | Launch, send commands to, and stop the managed local Dagster terminal (`terminal.create`, `terminal.sendText`, `terminal.close`) |
| `terminal:read` | Read recent output from the managed local Dagster terminal (`terminal.read`) for startup diagnostics |
| `agent:read` | List running workspace agents (`agents.list`) for optional "Send Error to Existing Agent" |
| `agent:launch` | Launch an Orca coding agent (`agent.launch`) on "Send Error to Agent" |
| `storage:own` | Persist connection profiles, workspace pins, and managed terminal IDs |
| `secrets:own` | Store and read encrypted credentials in OS keyring when auth source is `orca-secret` |
| `settings:own` | Read/write plugin settings (default agent, polling intervals, log limit) |
| `notifications:show` | Optional desktop notifications on run completion/failure |

### Contributed Settings (`contributes.settings`)

- `defaultAgent` (`string`, default `"claude"`, enum of Orca agent kinds) — Agent used by **Send Error to Agent**.
- `pollIntervalIdleSec` (`integer`, default `15`, min `5`, max `300`) — Polling interval when no runs are active.
- `pollIntervalActiveSec` (`integer`, default `3`, min `1`, max `60`) — Polling interval while runs are in progress.
- `notifyOnRunFailure` (`boolean`, default `true`) — Show desktop notification when a monitored run fails.
- `maxRunLogLines` (`integer`, default `200`, min `50`, max `1000`) — Maximum log entries fetched into the sidebar Run Detail view.

### Contributed Events (`contributes.events`)

- `workspace.changed` — Re-evaluate workspace-pinned profile and `.orca-dagster.json` when the user switches Orca workspaces.

### Contributed Status Bar (`contributes.statusBarItems`)

- `{ "id": "dagster.status", "text": "Dagster", "icon": "database", "command": "dagster.openPanel" }`
- Note: Declaring `statusBarItems` with `main` keeps the worker alive while enabled so it can maintain connection health and update the status bar.

---

## 2. Module Layout (Target Plugin Source)

```text
orca-plugin.json
tsconfig.json              # extends ./.orca/types/tsconfig.json
src/
  types.ts                 # Shared domain types + `declare module 'orca'` PluginRequests/PluginPanelEvents
  worker.ts                # `export default function activate(orca)` wiring handlers, commands, events, status bar
  panel.tsx                # Native `ui: "components"` TSX sidebar panel
  panel.check.ts           # Render states for `orca plugin check`
  profiles/
    store.ts               # Merge `.orca-dagster.json` + `orca.storage` + workspace pinning
    credentials.ts         # Dynamic resolver (`orca-secret` | `env-var`) + `${ENV_VAR}` interpolation
  dagster/
    client.ts              # Pure TS fetch-based GraphQL client (OSS + Dagster+)
    queries.ts             # GraphQL query & mutation documents
    types.ts               # Typed GraphQL response mappers
  local/
    controller.ts          # Managed Orca terminal lifecycle + port health probe / auto-attach
  source/
    resolver.ts            # 3-tier jump-to-source (`code_references` + prefix map -> symbol scanner -> fallback)
    scanner.ts             # Fast local Python `@asset`/`@job`/`@schedule`/`@sensor` scanner
  agent/
    handoff.ts             # Structured failure prompt builder + `agent.launch` / `terminal.sendText`
test/
  credentials.test.ts
  client.test.ts
  resolver.test.ts
  controller.test.ts
  panel.test.ts
```

---

## 3. Panel ↔ Worker IPC Contract (`src/types.ts`)

Use Orca's declaration merging (`declare module 'orca'`) so `plugin.request` and `orca.panels.handle` are strictly typed end to end.

### Requests (`PluginRequests`)

| Method | Input | Output |
| --- | --- | --- |
| `profiles.list` | `{}` | `{ profiles: ConnectionProfile[]; activeProfileId: string | null; workspaceId: string | null; keyringStatus: 'os-keyring' | 'unavailable' }` |
| `profiles.save` | `{ profile: ConnectionProfile; pinToWorkspace?: boolean; secretValue?: string }` | `{ profile: ConnectionProfile }` |
| `profiles.delete` | `{ profileId: string }` | `{ ok: true }` |
| `profiles.select` | `{ profileId: string; pinToWorkspace?: boolean }` | `{ activeProfileId: string }` |
| `profiles.test` | `{ profile: ConnectionProfile; secretValue?: string }` | `{ ok: boolean; version?: string; error?: string; latencyMs: number }` |
| `dagster.snapshot` | `{ profileId?: string }` | `{ status: ConnectionStatus; localState?: LocalState; locations: CodeLocation[]; assets: AssetSummary[]; jobs: JobSummary[]; schedules: ScheduleSummary[]; sensors: SensorSummary[]; runs: RunSummary[]; syncedAt: number }` |
| `dagster.reload` | `{ locationNames?: string[] }` | `{ results: { locationName: string; ok: boolean; error?: string }[] }` |
| `dagster.assetPartitions` | `{ assetKey: string[]; limit?: number }` | `{ kind: 'none' | 'single' | 'multi'; keys: string[]; dimensionName?: string }` |
| `dagster.materialize` | `{ assetKeys: string[][]; partitionKey?: string; runConfigYaml?: string; tags?: Record<string, string> }` | `{ runId: string }` |
| `dagster.launchJob` | `{ locationName: string; repositoryName: string; jobName: string; runConfigYaml?: string; tags?: Record<string, string> }` | `{ runId: string }` |
| `dagster.toggleSchedule` | `{ locationName: string; repositoryName: string; scheduleName: string; running: boolean }` | `{ status: 'RUNNING' | 'STOPPED' }` |
| `dagster.toggleSensor` | `{ locationName: string; repositoryName: string; sensorName: string; running: boolean }` | `{ status: 'RUNNING' | 'STOPPED' }` |
| `dagster.runDetail` | `{ runId: string; errorsOnly?: boolean; limit?: number }` | `{ run: RunDetail }` |
| `dagster.reexecuteRun` | `{ runId: string; strategy: 'ALL_STEPS' | 'FROM_FAILURE' }` | `{ runId: string }` |
| `dagster.terminateRun` | `{ runId: string }` | `{ ok: boolean; message?: string }` |
| `local.start` | `{ profileId?: string }` | `{ state: LocalState }` |
| `local.stop` | `{ profileId?: string }` | `{ state: LocalState }` |
| `local.restart` | `{ profileId?: string }` | `{ state: LocalState }` |
| `local.focusLogs` | `{ profileId?: string }` | `{ focused: boolean }` |
| `source.open` | `{ kind: 'asset' | 'job' | 'schedule' | 'sensor'; name: string; assetKey?: string[]; codeReferences?: CodeReference[] }` | `{ opened: boolean; resolvedPath?: string; externalUrl?: string; reason?: string }` |
| `agent.listTargets` | `{}` | `{ defaultAgent: string; terminals: { terminalId: string; title: string; agent: string; state: string }[] }` |
| `agent.sendRunError` | `{ runId: string; mode: 'launch' | 'existing'; terminalId?: string }` | `{ launched: boolean; terminalId?: string }` |

### Worker → Panel Events (`PluginPanelEvents`)

- `dagster.stateChanged`: `{ activeProfileId: string | null; status: ConnectionStatus; activeRunCount: number; syncedAt: number }`
- `dagster.workspaceChanged`: `{ workspaceId: string | null; activeProfileId: string | null }`

---

## 4. Dynamic Credential & Profile Model

```ts
export type ProfileKind = 'local' | 'remote-oss' | 'dagster-plus';

export type AuthMode =
  | 'none'
  | 'bearer'
  | 'basic'
  | 'dagster-plus-token'
  | 'custom-header';

export type CredentialSource = 'orca-secret' | 'env-var';

export interface AuthConfig {
  mode: AuthMode;
  source?: CredentialSource;
  /** Key in orca.secrets (e.g. "profile:<id>:token") when source === 'orca-secret' */
  secretKey?: string;
  /** Environment variable name (e.g. "DAGSTER_CLOUD_API_TOKEN") when source === 'env-var' */
  envVar?: string;
  /** Username for HTTP Basic auth (supports ${ENV_VAR} interpolation) */
  username?: string;
  /** Header name when mode === 'custom-header' */
  headerName?: string;
}

export interface LocalConfig {
  command: string;                       // default: "uv run dagster dev"
  port: number;                          // default: 3000
  cwd?: string;                          // relative to workspace root
  pathPrefixMap?: Record<string, string>;// e.g. { "/app/src": "docker/sample_project/src" }
}

export interface ConnectionProfile {
  id: string;
  name: string;
  kind: ProfileKind;
  url: string;                           // supports ${ENV_VAR}, GraphQL path defaults to /graphql
  auth: AuthConfig;
  local?: LocalConfig;
  origin?: 'repo' | 'user';              // runtime tag, not persisted in repo file
}
```

### Resolution Rules

1. **URL interpolation**: Replace `${VAR_NAME}` in `profile.url`, `auth.username`, and `auth.headerName` using `process.env[VAR_NAME]`. Fail fast with a typed `MissingEnvVarError(varName)` if undefined.
2. **GraphQL endpoint normalization**: If `url` does not end in `/graphql`, append `/graphql` (stripping trailing slash).
3. **Header construction**:
   - `none`: no auth header.
   - `bearer`: `Authorization: Bearer <secret>`
   - `basic`: `Authorization: Basic <base64(username + ":" + secret)>`
   - `dagster-plus-token`: `Dagster-Cloud-Api-Token: <secret>`
   - `custom-header`: `<headerName>: <secret>`
4. **Secret lookup**:
   - `source === 'env-var'`: read `process.env[auth.envVar]`. If missing/empty, return structured error naming `auth.envVar` (never log value).
   - `source === 'orca-secret'`: read `await orca.secrets.get(auth.secretKey)`. If missing, return structured error prompting the user to set the secret in the Profile Editor.
5. **Repo config merge**: Read `<workspacePath>/.orca-dagster.json` if present. Validate against schema rules (reject any raw token fields). User profiles in `orca.storage` with the same `id` override repo defaults; repo profiles remain visible with `origin: 'repo'`.

---

## 5. Dagster GraphQL Catalog

All requests are `POST <graphqlUrl>` with `Content-Type: application/json`, configurable timeout (`15s` default), and AbortSignal support.

| Operation | GraphQL Field | Notes |
| --- | --- | --- |
| Health / Version | `query { version }` | Used by `profiles.test` and local port auto-attach probe |
| Code Locations & Repos | `workspaceOrError { ... on Workspace { locationEntries { name loadStatus locationOrLoadError { ... on RepositoryLocation { repositories { name pipelines { name isJob description } schedules { name cronSchedule scheduleState { status } } sensors { name sensorState { status } } } } ... on PythonError { message stack } } } } }` | Powers Jobs & Automation tab + load error callout |
| Assets | `assetNodes { id assetKey { path } groupName description isPartitioned partitionDefinition { description dimensionTypes { name } } repository { name location { name } } assetMaterializations(limit: 1) { timestamp runId } metadataEntries { label ... on CodeReferencesMetadataEntry { codeReferences { ... on LocalFileCodeReference { filePath lineNumber label } ... on UrlCodeReference { url label } } } } }` | Powers Assets tree + Tier 1 Jump-to-Source |
| Partition Keys | `assetNodeOrError(assetKey: $assetKey) { ... on AssetNode { partitionKeysByDimension { name partitionKeys } } }` | Populates single-dimension partition picker (slice to latest 200 keys) |
| Runs List | `runsOrError(filter: $filter, limit: $limit, cursor: $cursor) { ... on Runs { results { runId pipelineName status startTime endTime } } }` | Powers Runs table with status filtering & pagination |
| Run Detail & Logs | `runOrError(runId: $runId)` + `logsForRun(runId: $runId, limit: $limit)` | Step statuses (`ExecutionStepStartEvent`, `ExecutionStepSuccessEvent`, `ExecutionStepFailureEvent`) + error stack traces |
| Materialize / Launch | `launchRun(executionParams: $executionParams)` | Supports `assetSelection`, `runConfigData`, `executionMetadata.tags` (including `dagster/partition`) |
| Re-execute | `launchRunReexecution(reexecutionParams: $reexecutionParams)` | `ALL_STEPS` or `FROM_FAILURE` |
| Terminate | `terminateRun(runId: $runId)` | Stops in-flight runs |
| Toggle Schedule | `startSchedule` / `stopRunningSchedule` | Takes `scheduleSelector` or `scheduleOriginId` / `instigationStateId` |
| Toggle Sensor | `startSensor` / `stopSensor` | Takes `sensorSelector` or `instigationStateId` |
| Reload Location | `reloadRepositoryLocation(repositoryLocationName: $name)` | Reloads definitions after code edits |

---

## 6. 3-Tier Jump-to-Source Resolver

When the user clicks an asset, job, schedule, or sensor:

1. **Tier 1 — Dagster `code_references` metadata**:
   - Inspect `LocalFileCodeReference.filePath` from `CodeReferencesMetadataEntry`.
   - Check if `filePath` exists directly inside the active workspace path.
   - If not, apply `profile.local?.pathPrefixMap` (e.g., `/app/src` → `docker/sample_project/src`) and verify file existence.
2. **Tier 2 — Fast Local Workspace Symbol Scanner**:
   - Triggered when `code_references` is absent (common for jobs, schedules, sensors, or assets without `with_source_code_references`) or Tier 1 path does not exist locally.
   - Walks `.py` files under the active workspace (skipping `.git`, `.venv`, `node_modules`, `__pycache__`, `.dagster`; capped at 2,000 files, cached in worker memory with mtime invalidation).
   - Matches decorator + definition patterns for:
     - `@asset` / `@dg.asset` / `@multi_asset` (`name="<name>"` or `def <name>(`)
     - `@job` / `define_asset_job(name="<name>"` / `def <name>(`
     - `@schedule` / `ScheduleDefinition(name="<name>"` / `def <name>(`
     - `@sensor` / `SensorDefinition(name="<name>"` / `def <name>(`
3. **Open in Main Editor**:
   - Convert resolved absolute path to workspace-relative path and call `orca.host.call('editor.open', { workspaceId, path })`.
   - Note: Orca Host API v1.10 `editor.open` opens the file tab without a line-number parameter; return `lineNumber` in the response payload so the sidebar can display `file.py:54` until Orca adds line navigation.
4. **Tier 3 — Remote-Only Fallback**:
   - If no local file matches, return `{ opened: false, externalUrl }` (preferring `UrlCodeReference.url` if present, otherwise the Dagster UI asset/job URL) so the sidebar shows a clickable external link without throwing an error.

---

## 7. Managed Local Dagster Controller

- Persists `{ [profileId]: { terminalId, workspaceId, startedAt } }` in `orca.storage` under `local.terminals` so state survives worker restarts.
- **Probe / Auto-Attach**:
  - `GET/POST <url>/graphql` with `{ version }` (2s timeout).
  - If reachable AND a tracked `terminalId` still exists in `terminal.list({ workspaceId })` → state is `managed-running`.
  - If reachable AND no tracked terminal exists → state is `attached` (external process or Docker Compose).
  - If unreachable AND tracked terminal exists → state is `starting` (within 30s grace) or `crashed` (read last 50 lines via `terminal.read` for error display).
  - Otherwise → `stopped`.
- **Start**: Probe first; if already reachable, transition to `attached`. Otherwise call `terminal.create({ workspaceId, title: "Dagster: " + profile.name, command: profile.local.command })` and poll until healthy.
- **Stop**: Only closes terminals created by this plugin (`terminal.close({ terminalId })`); never kills an externally attached process.
- **Focus Logs**: Calls `terminal.focus({ terminalId })`.

---

## 8. Agent Handoff (`Send Error to Agent`)

Prompt template built by `src/agent/handoff.ts`:

```markdown
Investigate and fix this failing Dagster run:

- **Run ID**: `<runId>`
- **Target**: `<pipelineName>`
- **Profile**: `<profileName>` (`<url>`)
- **Resolved Source File**: `<workspaceRelativePath>` (line `<lineNumber>` if known)
- **Failing Step(s)**: `<stepKeys>`

### Error Traceback
```
<truncated stack trace / error message, max 4000 chars>
```

Inspect the source file, identify the root cause, and propose or implement a fix.
```

- `mode === 'launch'`: calls `orca.host.call('agent.launch', { workspaceId, agent: defaultAgent, prompt })`.
- `mode === 'existing'`: calls `orca.host.call('terminal.sendText', { terminalId, text: prompt, enter: true })`.

---

## 9. Verification & Testing Strategy

Every milestone must pass:
1. `orca plugin validate . --json` — exit code `0`, zero errors.
2. `orca plugin test . --json` — unit tests for credential resolver, GraphQL client, source resolver, local controller, and panel rendering (`mount`, `mountWorker`, `mockHost` from `orca/testing`).
3. `orca plugin check . --json` — validates manifest and renders every state in `src/panel.check.ts` (empty, local stopped, connected with assets/jobs/runs, failed run detail, profile editor, auth error).
4. Live integration test against `docker/compose.yaml` (`:3000` unauthenticated OSS + `:3001` token-authenticated remote proxy).

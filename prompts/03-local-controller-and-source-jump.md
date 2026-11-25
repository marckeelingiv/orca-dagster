# Prompt 03 — Local Controller, Jump-to-Source & Agent Handoff (Milestone 3)

## Goal
Implement the three worker-side Orca integrations:
1. Managed local Dagster lifecycle controller (Start/Stop/Restart/Focus Logs/Auto-Attach in a dedicated Orca terminal).
2. 3-tier Jump-to-Source resolver (opening defining Python files in Orca's main editor via `editor.open`).
3. Failing run **Send Error to Agent** handoff (`agent.launch` and optional existing agent terminal targeting).

## Required Reading Before Editing
1. Read `docs/architecture.md` (Sections 6, 7, 8).
2. Read `docs/user-stories.md` (US-04, US-05, US-06, US-07, US-13).
3. Run `orca skills get orca-plugin-authoring --reference references/host-api.md` to verify exact params for `terminal.*`, `editor.open`, `agents.list`, and `agent.launch`.

## Tasks
1. **Managed Local Dagster Controller (`src/local/controller.ts`)**:
   - Persist managed terminal metadata in `orca.storage` under `local.terminals`.
   - Implement `probeState(profile, workspaceId)`:
     - Probes `<url>/graphql` (`{ version }`, 2s timeout) and checks `terminal.list({ workspaceId })` for any tracked `terminalId`.
     - Returns `'managed-running' | 'attached' | 'starting' | 'crashed' | 'stopped'` plus diagnostic tail (`terminal.read`, last 50 lines) on crash.
   - Implement `start(profile, workspaceId)`:
     - Probes first; if already reachable, returns `attached` without spawning a duplicate terminal.
     - Otherwise calls `terminal.create({ workspaceId, title: "Dagster: " + profile.name, command })`, persists `terminalId`, and polls until healthy (or timeout).
   - Implement `stop(profile, workspaceId)`:
     - Closes only plugin-managed terminals (`terminal.close`); refuses/no-ops on externally attached instances.
   - Implement `restart(profile, workspaceId)` and `focusLogs(profile, workspaceId)` (`terminal.focus`).
2. **3-Tier Jump-to-Source Resolver (`src/source/scanner.ts`, `src/source/resolver.ts`)**:
   - **Tier 1 (`code_references`)**: Inspect `LocalFileCodeReference.filePath`; check direct existence inside active workspace root, then apply `profile.local?.pathPrefixMap` (longest prefix first) and check existence.
   - **Tier 2 (Workspace Symbol Scanner)**:
     - Walk `.py` files under workspace root (skip `.git`, `.venv`, `node_modules`, `__pycache__`, `.dagster`; cap at 2,000 files; cache by `(workspacePath, mtime)`).
     - Match `@asset` / `@dg.asset` / `@multi_asset`, `@job` / `define_asset_job`, `@schedule` / `ScheduleDefinition`, `@sensor` / `SensorDefinition` by explicit `name=` or function `def <name>(`.
   - **Editor Open**: Convert resolved path to workspace-relative POSIX path and call `orca.host.call('editor.open', { workspaceId, path })`. Return `{ opened: true, resolvedPath, lineNumber }`.
   - **Tier 3 (Remote Fallback)**: If not found locally, return `{ opened: false, externalUrl, reason }` using `UrlCodeReference.url` or the constructed Dagster UI URL.
3. **Agent Handoff (`src/agent/handoff.ts`)**:
   - Fetch run detail + error traceback (`max 4000 chars`) and attempt source resolution for the failing target.
   - Format the structured prompt defined in `docs/architecture.md` Section 8.
   - Implement `listTargets(workspaceId)` using `agents.list({ workspaceId })` + `settings.get('defaultAgent')`.
   - Implement `sendRunError({ runId, mode, terminalId })`:
     - `mode === 'launch'`: `agent.launch({ workspaceId, agent: defaultAgent, prompt })`.
     - `mode === 'existing'`: `terminal.sendText({ terminalId, text: prompt, enter: true })`.
4. **Wire into `src/worker.ts`**:
   - Connect `local.*`, `source.open`, and `agent.*` request handlers.
   - Connect command palette handlers: `dagster.local.start`, `dagster.local.stop`, `dagster.local.restart`, `dagster.reloadDefinitions`.
5. **Unit Tests**:
   - `test/controller.test.ts`: test auto-attach when port is reachable, managed start/stop/restart, focusLogs, and crash tail capture using `mockHost`.
   - `test/resolver.test.ts`: test Tier 1 direct path, Tier 1 `pathPrefixMap` (`/app/src` → `docker/sample_project/src`), Tier 2 symbol fallback against `docker/sample_project/src`, and Tier 3 remote-only fallback.

## Exit Criteria (Run & Verify)
1. `orca plugin validate . --json` exits `0`.
2. `orca plugin test . --json` exits `0` with all controller, resolver, and handoff tests passing.
3. Resolver resolves `raw_orders` (Tier 1 with `pathPrefixMap`) and `daily_orders_schedule` (Tier 2 symbol scan) inside `docker/sample_project/src`.

# Prompt 01 — Scaffold Plugin & Shared Contracts (Milestone 1)

## Goal
Scaffold the `dagster` Orca plugin in this repository (`orca-plugin.json`, TypeScript config, Orca typings, shared IPC types, minimal worker/panel stubs, and test harness) so all downstream milestones build on a validated foundation.

## Required Reading Before Editing
1. Run `orca skills get orca-plugin-authoring` and read the manifest/workflow rules.
2. Read `docs/architecture.md` (Sections 1, 2, 3, 4).
3. Read `docs/user-stories.md` for milestone context.

## Tasks
1. **Create `orca-plugin.json`** at the repository root following `docs/architecture.md` Section 1:
   - `manifestVersion: 2`
   - `id: "dagster"` (do NOT prefix with `orca-`; omit `publisher`)
   - `execution: "trusted-worker"`
   - `engines: { "orca": ">=1.4.259" }`
   - `main: "src/worker.ts"`
   - `capabilities`: `workspace:read`, `workspace:write`, `terminal:send`, `terminal:read`, `agent:read`, `agent:launch`, `storage:own`, `secrets:own`, `settings:own`, `notifications:show`
   - `contributes.panels`: single sidebar panel `{ "id": "dagster.sidebar", "title": "Dagster", "icon": "database", "entry": "src/panel.tsx", "ui": "components" }`
   - `contributes.statusBarItems`: `{ "id": "dagster.status", "text": "Dagster", "icon": "database", "command": "dagster.openPanel" }`
   - `contributes.commands`: `dagster.openPanel`, `dagster.reloadDefinitions`, `dagster.switchProfile`, `dagster.local.start`, `dagster.local.stop`, `dagster.local.restart`
   - `contributes.events`: `[{ "on": "workspace.changed" }]`
   - `contributes.settings`: `defaultAgent`, `pollIntervalIdleSec`, `pollIntervalActiveSec`, `notifyOnRunFailure`, `maxRunLogLines`
2. **Generate Orca TypeScript typings**:
   - Run `orca plugin types . --json`.
   - Create `tsconfig.json` extending `./.orca/types/tsconfig.json`.
   - Add `.orca/` to `.gitignore` (never commit generated `.orca/` files).
3. **Create `src/types.ts`**:
   - Define all domain types from `docs/architecture.md` Sections 3 & 4 (`ConnectionProfile`, `AuthConfig`, `LocalConfig`, `CodeLocation`, `AssetSummary`, `JobSummary`, `ScheduleSummary`, `SensorSummary`, `RunSummary`, `RunDetail`, `LocalState`, `ConnectionStatus`, `CodeReference`).
   - Add `declare module 'orca'` declaration merging for `PluginRequests` and `PluginPanelEvents` covering every method in `docs/architecture.md` Section 3.
4. **Create minimal working stubs**:
   - `src/worker.ts`: `export default function activate(orca)` registering placeholder handlers for all `PluginRequests` and command stubs so `orca plugin check` succeeds.
   - `src/panel.tsx`: minimal valid `components` panel rendering a `Heading` and `EmptyState`.
   - `src/panel.check.ts`: initial check state exporting a valid render fixture.
   - `test/smoke.test.ts`: smoke test using `mount` and `mountWorker` from `orca/testing`.

## Constraints
- Pure TypeScript only; zero runtime npm dependencies unless strictly required.
- Do not modify `docs/`, `prompts/`, or `docker/`.
- Never call `orca plugin enable --approve` without user consent.

## Exit Criteria (Run & Verify)
1. `orca plugin validate . --json` exits `0` with zero errors.
2. `orca plugin test . --json` exits `0` with smoke tests passing.
3. `orca plugin check . --json` exits `0` and renders the panel without warnings.

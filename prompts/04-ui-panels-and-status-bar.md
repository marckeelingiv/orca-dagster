# Prompt 04 — Native Components UI, Status Bar & Adaptive Polling (Milestone 4)

## Goal
Build the complete native Orca `components` sidebar panel (`src/panel.tsx`), check states (`src/panel.check.ts`), status bar updates, command palette integration, and adaptive polling loop matching every wireframe in `docs/wireframes.md`.

## Required Reading Before Editing
1. Read `docs/wireframes.md` (every section — this is the UI specification).
2. Read `docs/architecture.md` (Sections 1, 3, 9).
3. Run `orca skills get orca-plugin-authoring --reference references/components.md` to verify every component prop and atom rule (`update` vs `set`, keyed atoms, `<RelativeTime />`, render limits).

## Tasks
1. **Sidebar Panel (`src/panel.tsx`)**:
   - Use keyed atoms (`atom('key', ...)`) for UI state that should survive hot reloads (active tab, search queries, selected asset/run, profile editor open state).
   - Implement `export function setup(ctx)` for:
     - Initial load of `profiles.list` + `dagster.snapshot`.
     - Adaptive polling timer: `pollIntervalActiveSec` (default `3s`) while any run is active (`STARTED`/`STARTING`/`QUEUED`), `pollIntervalIdleSec` (default `15s`) when idle, **paused when `read(ctx.visible)` is false**, cleaned up on `ctx.signal` abort.
     - Listening to `plugin.on('dagster.workspaceChanged')` and `plugin.on('dagster.stateChanged')`.
   - Implement all 6 views from `docs/wireframes.md`:
     1. **Persistent Header & Local Controls** (profile `Select`, connection `Badge`, local `Toolbar` with Start/Stop/Restart/Reload/Logs, last sync `<RelativeTime />`, refresh button, settings gear).
     2. **Assets Tab** (search `Input`, `Tree` grouped `Location -> Group -> Asset`, single-click triggers `source.open` + selects asset, bottom Selected Asset Card with resolved path, single-dimension partition `Select`, multi-dimension Launchpad `Link`, collapsible Advanced Config `Textarea`, Materialize & Open in Dagster UI buttons).
     3. **Jobs & Automation Tab** (search `Input`, collapsible Jobs, Schedules, Sensors sections; single-click triggers `source.open`; Launch button for jobs; `Switch` toggles for schedules/sensors; inline tick error `Callout`).
     4. **Runs Tab + Inline Run Detail** (status filter `Select`, search `Input`, paginated `Table` with `<RelativeTime />`; clicking a row opens Run Detail with `KeyValue`, steps list, level-filtered `CodeView`, Re-execute, Terminate, Open in Dagster UI, and **Send Error to Agent** + optional existing agent target).
     5. **Connection Profiles Editor** (list of `repo` + `user` profiles, Add/Edit/Delete form, dynamic Auth Mode + Credential Source `orca-secret` vs `env-var`, keyring availability check, `local` command/port/pathPrefixMap fields, workspace pin checkbox, **Test Connection** button).
     6. **Empty, Loading & Error States** (No profile, Auth 401/403, Code location traceback, Local stopped, Initial `Skeleton`).
2. **Status Bar & Commands (`src/worker.ts`)**:
   - Update `dagster.status` via `orca.host.call('statusBar.update', ...)` whenever connection state or active/failed run counts change.
   - Implement `dagster.openPanel` and `dagster.switchProfile` commands.
   - Subscribe to `workspace.changed` event to re-load workspace `.orca-dagster.json` and pinned profile, posting `dagster.workspaceChanged` to open panels.
   - Optional desktop notification (`notifications.show`) when a tracked run transitions to `FAILURE` and `notifyOnRunFailure` is enabled.
3. **Panel Check States (`src/panel.check.ts`) & Panel Tests (`test/panel.test.ts`)**:
   - Export render states in `src/panel.check.ts` covering: `empty`, `local-stopped`, `connected-assets`, `automation`, `runs-list`, `run-detail-failed`, `profile-editor`, `auth-error`.
   - Add `test/panel.test.ts` using `mount` from `orca/testing` verifying:
     - Clicking an asset calls `source.open`.
     - Switching tabs renders Jobs/Schedules/Sensors and Runs.
     - Failed run detail renders **Send Error to Agent** and triggers `agent.sendRunError`.
     - Profile editor switches cleanly between `env-var` and `orca-secret` fields.

## Constraints
- Only use components and props documented in `references/components.md` (no DOM tags, no `className`, no `style`, no hooks).
- Never call `set()` or `update()` during render.
- `shell.openExternal` must only be called from an explicit button/link press handler.
- Keep node count well under 5,000 and text props under 16 KiB.

## Exit Criteria (Run & Verify)
1. `orca plugin validate . --json` exits `0`.
2. `orca plugin test . --json` exits `0` with all panel and worker tests passing.
3. `orca plugin check . --json` exits `0` and renders every state in `src/panel.check.ts` without warnings or dropped props.

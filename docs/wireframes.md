# Wireframes — Dagster Orca Plugin

All views use Orca's native **Components Panel** (`"ui": "components"`, `src/panel.tsx`).
Every UI element below maps strictly to components in Orca Host API v1.10 (`Stack`, `Row`, `Box`, `Toolbar`, `Tabs`, `Tab`, `Tree`, `Table`, `List`, `ListItem`, `Card`, `Collapsible`, `KeyValue`, `CodeView`, `Input`, `Textarea`, `Select`, `Switch`, `Button`, `IconButton`, `Badge`, `RelativeTime`, `Callout`, `EmptyState`, `Spinner`, `Separator`, `Text`, `Heading`, `Link`).

Design constraints:
- Respect Orca's render limits: `< 5,000` nodes, `< 512 KiB` payload, `< 16 KiB` per text prop, `< 500` Select options.
- Truncate/paginate run tables and log buffers (`maxLines: 200` in sidebar, with "Errors Only" filter).
- Never poll for clock time; always use `<RelativeTime at={ms} />`.

---

## 1. Persistent Header & Local Controls

Rendered at the top of the sidebar panel across all tabs.

```text
+-------------------------------------------------------+
| [Select: local (Docker)        v]  (o) Connected  [*] |
|                                                       |
| -- Shown only when active profile.kind === 'local' -- |
| [> Start] [[] Stop] [~ Restart] [@ Reload] [>_ Logs]  |
| Status: Attached (:3000) · Last sync: 4s ago     [@]  |
+-------------------------------------------------------+
```

**Component Mapping:**
- Row 1 (`Row justify="between" align="center" gap="xs"`):
  - `Select` (`label="Connection profile"`, `labelHidden`) — options from merged `.orca-dagster.json` + `orca.storage` profiles.
  - `Badge` (`tone`: `success` Connected, `warning` Connecting/Degraded, `danger` Error/Auth Failed, `neutral` Stopped).
  - `IconButton` (`icon="settings"`, `label="Manage profiles"`) — toggles inline **Profiles View** (Section 6).
- Row 2 (`Toolbar`, shown only for `kind === 'local'`):
  - `Button`/`IconButton` controls: `play` Start (disabled when running/attached), `stop` Stop (disabled unless managed), `refresh` Restart (disabled unless managed), `refresh` Reload Definitions, `terminal` Focus Logs (disabled unless managed terminal exists).
- Row 3 (`Row justify="between" align="center"`):
  - `Text muted size="xs"`: `Managed (:3000)` / `Attached (:3000)` / `Remote OSS` / `Dagster+` + `<RelativeTime at={lastSyncAt} muted />`.
  - `IconButton` (`icon="refresh"`, `label="Refresh now"`).

---

## 2. Tab 1: Assets

```text
+-------------------------------------------------------+
| [ Assets ]   [ Jobs & Auto ]   [ Runs (1) ]           |
+-------------------------------------------------------+
| [ Search assets...                          ] [x]     |
|                                                       |
| v sample_project (code location)                      |
|   v commerce (group)                                  |
|     [*] raw_orders                       [OK] 2m ago  |
|     [*] cleaned_orders                   [OK] 2m ago  |
|     [*] daily_revenue  (partitioned)     [OK] 1h ago  |
|   v debug (group)                                     |
|     [!] flaky_enrichment               [FAIL] 5m ago  |
+-------------------------------------------------------+
| Selected: daily_revenue                               |
| Source: docker/sample_project/src/.../assets.py  [->] |
| Partition: [ 2026-11-24 (latest)         v ]          |
| > Advanced Config (optional YAML/JSON)                |
| [> Materialize]  [Open in Dagster UI ^]               |
+-------------------------------------------------------+
```

**Interactions:**
- **Single-click asset in `Tree` (`onSelect`)**:
  1. Updates `selectedAssetKey` atom so the bottom **Selected Asset Action Card** updates.
  2. Sends `plugin.request('source.open', { kind: 'asset', key, codeReferences })` to worker, which resolves the local file and calls `editor.open({ workspaceId, path })`.
- **Selected Asset Action Card (`Card` / `Box border radius="md" padding="sm"`)**:
  - Shows resolved local path (or `Remote-only` badge + external link if not in local workspace).
  - If asset is single-dimension partitioned: renders `Select` for partition key (populated on demand via `dagster.assetPartitions`, defaulting to latest).
  - If asset is multi-dimension partitioned: renders `Callout tone="info"` with `Link` to open Dagster Launchpad externally (`shell.openExternal`).
  - `Collapsible title="Advanced Config"`: `Textarea` for optional run config YAML/JSON + tags.
  - Primary `Button variant="solid" tone="accent" icon="play"`: **Materialize**.
  - Secondary `Button variant="ghost" icon="open-external"`: **Open in Dagster UI**.

**Component Mapping:**
- `Tabs` (`value={activeTabAtom}`) → `Tab value="assets" label="Assets" icon="layers"`
- `Input type="search" placeholder="Search assets..."`
- `Tree` (`items` built as `Location -> Group -> Asset`, `maxHeight="md"`, `onSelect`)
- `Card` + `Select` + `Collapsible` + `Textarea` + `Row` of `Button`s.

---

## 3. Tab 2: Jobs & Automation

```text
+-------------------------------------------------------+
| [ Assets ]   [ Jobs & Auto ]   [ Runs (1) ]           |
+-------------------------------------------------------+
| [ Search jobs, schedules, sensors...        ]         |
|                                                       |
| v Jobs (2)                                            |
|   +-------------------------------------------------+ |
|   | orders_pipeline_job            [> Launch]       | |
|   |   sample_project · 3 assets                     | |
|   +-------------------------------------------------+ |
|   | flaky_debug_job                [> Launch]       | |
|   |   sample_project · 1 asset                      | |
|   +-------------------------------------------------+ |
|                                                       |
| v Schedules (1)                                       |
|   +-------------------------------------------------+ |
|   | daily_orders_schedule          [ON / off]       | |
|   |   0 6 * * * · Last tick: Success 3h ago         | |
|   +-------------------------------------------------+ |
|                                                       |
| v Sensors (1)                                         |
|   +-------------------------------------------------+ |
|   | new_orders_sensor              [on / OFF]       | |
|   |   Every 30s · Last tick: Skipped 1m ago         | |
|   +-------------------------------------------------+ |
+-------------------------------------------------------+
```

**Interactions:**
- **Single-click any Job, Schedule, or Sensor row**: selects it and calls `plugin.request('source.open', { kind, name, locationName })` to open its defining Python file in Orca's main editor.
- **Job `Launch` button**: launches the job (or expands inline optional config box when selected).
- **Schedule / Sensor `Switch`**: calls `dagster.toggleSchedule` / `dagster.toggleSensor` with optimistic disabled state while in flight.
- **Tick errors**: if a schedule/sensor's latest tick failed, renders a compact `Callout tone="danger"` under that item with the error summary.

**Component Mapping:**
- `Tab value="automation" label="Jobs & Auto" icon="zap"`
- `Input type="search"`
- Three `Collapsible` sections (`defaultOpen`): **Jobs**, **Schedules**, **Sensors**, each containing `List` / `ListItem` rows with `Button` or `Switch`.

---

## 4. Tab 3: Runs List & Inline Run Detail

### 4A. Runs List View

```text
+-------------------------------------------------------+
| [ Assets ]   [ Jobs & Auto ]   [ Runs (1) ]           |
+-------------------------------------------------------+
| Filter: [ All v ]  [ Search run ID or job... ]        |
|                                                       |
| Run       Target                Status    Started     |
| ----------------------------------------------------- |
| a1b2c3d4  flaky_debug_job       [FAIL]    5m ago      |
| e5f6g7h8  orders_pipeline_job   [RUN ]    12s ago     |
| 9i0j1k2l  daily_revenue         [OK  ]    1h ago      |
|                                                       |
| [ Load more ]                                         |
+-------------------------------------------------------+
```

**Component Mapping:**
- `Row`: `Select` (`All`, `Active`, `Success`, `Failure`, `Canceled`) + `Input type="search"`.
- `Table` (`columns`: Run, Target, Status badge cell, RelativeTime cell; `selectionMode="single"`, `onRowAction` / `onSelectionChange` opens Run Detail; `hasMore` + `onLoadMore` for pagination).

### 4B. Inline Run Detail View (replaces Runs List while `selectedRunId` is set)

```text
+-------------------------------------------------------+
| [< Back to Runs]   Run a1b2c3d4              [FAIL]   |
+-------------------------------------------------------+
| Target:    flaky_debug_job                            |
| Started:   5m ago · Duration: 1.4s                    |
| Source:    docker/sample_project/src/.../assets.py    |
|                                                       |
| [~ Re-execute] [Open UI ^] [Bot: Send Error to Agent] |
|                            [v Send to Existing Agent] |
+-------------------------------------------------------+
| v Steps (1)                                           |
|   [!] flaky_enrichment                        1.2s    |
|                                                       |
| v Logs   [ Errors Only (1) v ]                        |
| +---------------------------------------------------+ |
| | 15:02:11 ERROR flaky_enrichment                   | |
| | RuntimeError: Simulated enrichment failure...     | |
| |   File "/app/src/sample_project/assets.py", l.54  | |
| +---------------------------------------------------+ |
+-------------------------------------------------------+
```

**Interactions:**
- **Active runs** (`STARTED`/`STARTING`/`QUEUED`): shows `[[] Terminate]` button; hides **Send Error to Agent**.
- **Failed runs** (`FAILURE`): highlights **Send Error to Agent** (`Button variant="solid" tone="accent" icon="bot"`).
  - Primary click calls `plugin.request('agent.sendRunError', { runId, mode: 'launch' })`.
  - Secondary `Button` / `Select` (shown when `agents.list` reports active agent terminals) allows **Send to Existing Agent** (`mode: 'existing'`, picking `terminalId`).
- Clicking the resolved `Source` link opens the file in Orca's main editor.

**Component Mapping:**
- `Row`: `Button icon="collapse" variant="ghost"` Back + `Heading level={3}` + `Badge`.
- `KeyValue` for Target, Started (`RelativeTime`), Duration, Source file.
- `Toolbar` for Re-execute, Terminate (if active), Open in Dagster UI, Send Error to Agent.
- `Collapsible title="Steps"` → `List` of steps with status `Badge`.
- `Collapsible title="Logs" defaultOpen` → `Select` (`Errors Only`, `All`) + `CodeView language="text" lineNumbers wrap maxHeight="md"`.

---

## 5. Connection Profiles & Dynamic Credential Editor

Opened by clicking the header gear `IconButton` (replaces tab content until **Done** is pressed).

```text
+-------------------------------------------------------+
| [< Done]   Connection Profiles             [+ New]    |
+-------------------------------------------------------+
| Saved Profiles                                        |
|  * local (Docker)       http://localhost:3000  [repo] |
|    remote-dev (Proxy)   http://localhost:3001  [repo] |
|    prod-cloud           https://acme.dagster.. [user] |
+-------------------------------------------------------+
| Edit Profile: remote-dev (Proxy)                      |
|                                                       |
| Name:       [ remote-dev (Proxy)                  ]   |
| Kind:       [ Remote OSS (GraphQL)              v ]   |
| URL:        [ http://localhost:3001               ]   |
|                                                       |
| Auth Mode:  [ Bearer Token                      v ]   |
| Cred Source:[ Environment Variable              v ]   |
| Env Var:    [ DAGSTER_DEV_REMOTE_TOKEN            ]   |
|   (Resolved: present ✓ — value never shown)           |
|                                                       |
| -- If Cred Source === 'Orca Encrypted Secret' --      |
| Secret:     [ ••••••••••••••••••••  ] [Save Secret]   |
|   (Keyring: os-keyring ✓)                             |
|                                                       |
| -- If Kind === 'Local' --                             |
| Start Cmd:  [ uv run dagster dev                  ]   |
| Port:       [ 3000                                ]   |
| Path Map:   [ /app/src=docker/sample_project/src  ]   |
|                                                       |
| [x] Pin as default for this workspace                 |
| [Test Connection]   [Save Profile]   [Delete]         |
+-------------------------------------------------------+
```

**Component Mapping:**
- `List` of profiles with `Badge` (`repo` from `.orca-dagster.json` vs `user` from `orca.storage`).
- `Field` + `Input` / `Select` / `Checkbox` form controls.
- For `Cred Source === 'orca-secret'`: checks `secrets.protection` — if `unavailable`, displays a `Callout tone="warning"` recommending `Environment Variable` mode instead.
- `Test Connection` button calls `plugin.request('profiles.test', { profile })` and renders a `Callout` (`success` with Dagster version or `danger` with HTTP/auth error).

---

## 6. Empty, Loading, and Error States

1. **No Profile Configured**: `EmptyState icon="database" title="No Dagster connection"` + `Button` **Add Profile** / **Start Local Dagster**.
2. **Auth Error (401/403)**: `Callout tone="danger" title="Authentication failed"` naming the auth mode and missing env var or secret key + `Button` **Edit Profile**.
3. **Code Location Load Error**: `Callout tone="warning" title="Code location failed to load"` with collapsible `CodeView` of the Python traceback + `Button` **Reload**.
4. **Local Stopped**: `EmptyState icon="play" title="Local Dagster is stopped"` + `Button` **Start Local Dagster**.
5. **Initial Load**: `Skeleton shape="text" lines={6}` (never blocks the header or profile switcher).

---

## 7. Status Bar Item & Command Palette

**Status Bar (`contributes.statusBarItems`, id: `dagster.status`):**
- Connected & idle: `database` icon, `tone="neutral"`, text: `Dagster: local`
- Active runs: `activity` icon, `tone="accent"`, text: `Dagster: local · 1 running`
- Recent failure: `warning` icon, `tone="danger"`, text: `Dagster: local · failed`
- Disconnected / stopped: `plug` icon, `tone="neutral"`, text: `Dagster: stopped`
- Clicking status bar item runs `dagster.openPanel`.

**Command Palette (`contributes.commands`):**
- `dagster.openPanel` — `Dagster: Open Sidebar`
- `dagster.reloadDefinitions` — `Dagster: Reload Code Locations`
- `dagster.switchProfile` — `Dagster: Switch Connection Profile`
- `dagster.local.start` — `Dagster: Start Local Instance`
- `dagster.local.stop` — `Dagster: Stop Local Instance`
- `dagster.local.restart` — `Dagster: Restart Local Instance`

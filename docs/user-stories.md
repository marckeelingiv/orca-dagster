# User Stories — Dagster Orca Plugin

Target plugin ID: `dagster` (Orca Host API v1.10+, pure TypeScript `trusted-worker` + native `components` sidebar panel).

## Epic 1: Connection Profiles & Dynamic Credentials (Milestone 2 & 4)

### US-01: Manage Multiple Dagster Connection Profiles
**As a** developer,
**I want** to create, edit, delete, and switch between multiple saved Dagster profiles (`local`, `remote-oss`, `dagster-plus`),
**So that** I can inspect local dev, staging, and production Dagster instances from one sidebar.

**Acceptance Criteria:**
- Users can add, edit, duplicate, and delete profiles in the sidebar's Profiles view.
- Switching profiles updates the active connection, clears stale data, and refreshes assets, jobs, and runs.
- Each workspace can pin a default profile; switching Orca workspaces restores that workspace's pinned profile if set.

### US-02: Dynamic Credential Resolution (Orca Secrets or Environment Variables)
**As a** developer,
**I want** to choose per profile whether authentication credentials come from Orca's encrypted secret store or a named environment variable,
**So that** I can use OS-keyring storage for personal tokens or existing shell/CI env vars without duplicating secrets.

**Acceptance Criteria:**
- Supported auth modes: `none`, `bearer`, `basic`, `dagster-plus-token` (`Dagster-Cloud-Api-Token`), and `custom-header`.
- Each credential field supports `source: "orca-secret"` (stored via `secrets:own`) or `source: "env-var"` (read dynamically from `process.env[envVar]`).
- `${ENV_VAR}` placeholders in profile URLs and custom headers are interpolated at request time.
- Missing env vars or unavailable keyring produce an actionable inline warning (never leaking secret values in logs or UI).

### US-03: Shareable Repo Defaults via `.orca-dagster.json`
**As a** team lead,
**I want** to commit a secret-free `.orca-dagster.json` at the repo root with default profile URLs, env var names, local start commands, and path-prefix mappings,
**So that** teammates get working Dagster connections as soon as they open the repository in Orca.

**Acceptance Criteria:**
- Worker reads `<workspace>/.orca-dagster.json` when present and merges it with user profiles in `orca.storage` (user overrides win by profile `id`).
- Raw secret values are rejected/ignored by the schema; only `env-var` references or `orca-secret` key names are allowed.
- JSON Schema at `schemas/orca-dagster.schema.json` validates `.orca-dagster.json`.

---

## Epic 2: Local Dagster Lifecycle Control (Milestone 3 & 4)

### US-04: Start, Stop, and Restart Local Dagster in an Orca Terminal
**As a** developer,
**I want** to start, stop, and restart my local Dagster instance directly from the sidebar,
**So that** I do not have to manually manage terminal commands while iterating on pipelines.

**Acceptance Criteria:**
- Local profiles configure a `command` (default: `uv run dagster dev`), `port` (default: `3000`), and optional `cwd`.
- Pressing **Start** opens a dedicated Orca terminal (`terminal.create`, titled `Dagster: <profile>`) running the command and polls `/graphql` until healthy.
- Pressing **Stop** closes the managed terminal (`terminal.close`); **Restart** stops and starts cleanly.
- Pressing **Logs** focuses the managed terminal (`terminal.focus`) so the developer sees full live output.

### US-05: Auto-Attach to an Already-Running Local Instance
**As a** developer,
**I want** the plugin to detect if Dagster is already listening on the configured local port (e.g., started via Docker Compose or another shell),
**So that** I can inspect and control it without spawning a duplicate process.

**Acceptance Criteria:**
- Before starting a new process, the worker probes the configured URL's GraphQL endpoint.
- If reachable, the UI shows status `Attached` (instead of `Managed`) and enables Reload/Browse/Run actions without launching a duplicate terminal.
- If the external instance stops, the status transitions cleanly to `Stopped` and re-enables **Start**.

### US-06: Reload Code Locations
**As a** developer,
**I want** to reload Dagster code locations from the sidebar or command palette after editing Python files,
**So that** new or modified assets, jobs, schedules, and sensors appear immediately.

**Acceptance Criteria:**
- Clicking **Reload** (or running `Dagster: Reload Definitions`) invokes GraphQL `reloadRepositoryLocation` for each loaded code location.
- Load errors (e.g., Python syntax/import errors) are surfaced clearly in an inline error callout with the traceback.

---

## Epic 3: Jump-to-Source Navigation (Milestone 3 & 4)

### US-07: Click an Asset, Job, Schedule, or Sensor to Open Its Source File
**As a** developer,
**I want** to click an asset, job, schedule, or sensor in the sidebar and immediately see the Python file that defines it open in Orca's main editor area,
**So that** I can navigate straight from pipeline state to code.

**Acceptance Criteria:**
- Single-clicking an asset, job, schedule, or sensor selects it in the sidebar AND triggers source resolution.
- **Tier 1:** Uses `dagster/code_references` metadata (`LocalFileCodeReference.filePath`) when present, applying `pathPrefixMap` (e.g., `/app/src` → `docker/sample_project/src`) to resolve container/remote paths to the active workspace.
- **Tier 2:** Falls back to fast local workspace symbol scanning (`@asset`, `@multi_asset`, `@job`, `define_asset_job`, `@schedule`, `@sensor`) when metadata is absent or path does not exist locally.
- Opens the resolved file via `editor.open({ workspaceId, path })`.
- **Tier 3:** If the definition only exists remotely (not found in the local workspace), shows a non-blocking notice in the sidebar with metadata and an external link to the Git URL (if `UrlCodeReference` exists) or Dagster UI.

---

## Epic 4: Asset & Automation Operations (Milestone 2 & 4)

### US-08: Browse and Search Assets by Code Location & Group
**As a** developer,
**I want** to browse assets in a searchable tree grouped by code location and asset group with latest materialization status,
**So that** I can quickly find an asset and see whether it is healthy, stale, or failed.

**Acceptance Criteria:**
- Assets render in a `Tree` grouped by `Code Location → Group → Asset`.
- Search input filters by asset key, group, or description in real time.
- Each asset shows a status indicator (`success`, `danger`, `warning`, `neutral`) and last materialization relative time.

### US-09: Materialize Assets (1-Click, Partitioned, or With Config)
**As a** developer,
**I want** to materialize one or more selected assets directly from the sidebar, including choosing a partition key or passing optional YAML/JSON run config,
**So that** I can test changes without switching to a browser.

**Acceptance Criteria:**
- Standard assets materialize with one click via GraphQL `launchPipelineExecution` / `launchRun`.
- Single-dimension partitioned assets display a partition key picker (defaulting to latest partition).
- An expandable **Advanced Config** section accepts optional YAML/JSON run config and tags.
- Complex multi-partition backfills offer an **Open in Dagster Launchpad** external link (`shell.openExternal`).

### US-10: Browse, Toggle, and Trigger Jobs, Schedules, and Sensors
**As a** developer,
**I want** to view jobs, schedules, and sensors, launch jobs, and start/stop schedules and sensors,
**So that** I can manage automation state alongside my code.

**Acceptance Criteria:**
- **Jobs & Automation** tab groups items into collapsible sections: Jobs, Schedules, Sensors, with search filtering.
- Jobs support 1-click **Launch** (with optional config box) and single-click jump-to-source.
- Schedules and sensors display `RUNNING`/`STOPPED` state and a `Switch` toggle calling `startSchedule`/`stopRunningSchedule` and `startSensor`/`stopSensor`.
- Last tick status and error message (if any) are shown inline.

---

## Epic 5: Run Monitoring, Logs & Debugging (Milestone 2 & 4)

### US-11: Monitor Recent Runs with Adaptive Polling
**As a** developer,
**I want** to see a live-updating list of recent runs filterable by status,
**So that** I can track in-flight runs without hammering the Dagster server when idle.

**Acceptance Criteria:**
- Runs tab renders a `Table` with Run ID (short), Job/Asset target, Status `Badge`, and `RelativeTime`.
- Status filter supports `All`, `Active` (`STARTED`/`STARTING`/`QUEUED`), `Success`, `Failure`, and `Canceled`.
- Polling runs every `3s` when at least one run is active, `15s` when idle, and pauses when the sidebar panel is hidden (`ctx.visible`).

### US-12: Inspect Run Details, Step Events, and Error Logs
**As a** developer,
**I want** to open a run inline to inspect its step statuses, failure tracebacks, and logs, and re-execute or terminate it,
**So that** I can diagnose failures without leaving Orca.

**Acceptance Criteria:**
- Clicking a run opens an inline Run Detail view with Back navigation.
- Displays run metadata (`KeyValue`), per-step status list, and log viewer (`CodeView`) filterable by level (`All`, `Errors Only`).
- Provides **Re-execute** (full or from failure), **Terminate** (when active), and **Open in Dagster UI** actions.

---

## Epic 6: Agent Handoff & Orca Integration (Milestone 3 & 4)

### US-13: Send Failing Run Context to an Orca Coding Agent
**As a** developer,
**I want** to click **Send Error to Agent** on a failed run to launch an Orca coding agent pre-loaded with the run ID, target asset/job, resolved source file path, and error stack trace,
**So that** an agent can immediately diagnose and fix the pipeline bug.

**Acceptance Criteria:**
- Visible on failed runs in the Run Detail view.
- Default action calls `agent.launch({ workspaceId, agent, prompt })` using the plugin's configured default agent setting.
- Secondary action allows sending the same structured prompt to an already-running agent terminal (`agents.list` + `terminal.sendText`).
- Prompt stays concise and structured (truncating excessively long logs while preserving the Python traceback).

### US-14: Status Bar Indicator & Command Palette Actions
**As a** developer,
**I want** a compact status bar item and command palette shortcuts for frequent Dagster actions,
**So that** I can monitor connection/run health and trigger reloads from anywhere in Orca.

**Acceptance Criteria:**
- Status bar item shows `<profile> · <state>` (e.g., `Dagster: local · 1 running`, `Dagster: prod · 2 failed`, `Dagster: local · stopped`) with semantic tone (`neutral`, `accent`, `success`, `warning`, `danger`).
- Command palette exposes:
  - `dagster.openPanel` ("Dagster: Open Sidebar")
  - `dagster.switchProfile` ("Dagster: Switch Connection Profile")
  - `dagster.reloadDefinitions` ("Dagster: Reload Code Locations")
  - `dagster.local.start` / `dagster.local.stop` / `dagster.local.restart`

---

## Epic 7: Developer & Agent Testability (Milestone 1 & 5)

### US-15: Deterministic Local & Remote Dev Sandbox in Docker
**As an** agent or contributor developing the plugin,
**I want** a one-command Docker Compose environment providing both an unauthenticated local Dagster OSS instance and a token-authenticated remote proxy with realistic assets, jobs, schedules, sensors, and intentional failures,
**So that** every feature can be tested automatically without external cloud credentials.

**Acceptance Criteria:**
- `docker compose -f docker/compose.yaml up -d` starts `postgres`, `sample-code`, `dagster-webserver` (`:3000`), `dagster-daemon`, and `remote-auth-proxy` (`:3001`).
- Sample project includes passing assets, partitioned assets, a toggleable failing asset, jobs, schedules, sensors, and `with_source_code_references`.
- `remote-auth-proxy` rejects missing/wrong tokens with HTTP `401` and proxies valid Bearer or `Dagster-Cloud-Api-Token` requests to `dagster-webserver`.

---

## Milestone Traceability

| Milestone | Prompt File | User Stories Covered |
| --- | --- | --- |
| M1: Scaffold & Contracts | `prompts/01-scaffold-and-contracts.md` | Foundation for all US + US-15 verification |
| M2: GraphQL & Profiles | `prompts/02-graphql-and-profiles.md` | US-01, US-02, US-03, US-08, US-09, US-10, US-11, US-12 (worker/data layer) |
| M3: Local Controller & Source Jump | `prompts/03-local-controller-and-source-jump.md` | US-04, US-05, US-06, US-07, US-13 (worker logic) |
| M4: UI Panels & Status Bar | `prompts/04-ui-panels-and-status-bar.md` | US-01 through US-14 (UI + status bar + commands) |
| M5: Integration & E2E Verification | `prompts/05-integration-and-e2e-verification.md` | Full E2E verification across US-01 through US-15 |

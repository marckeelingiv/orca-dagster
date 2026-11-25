# Dagster for Orca

An Orca sidebar for Dagster OSS and Dagster+. Browse and materialize assets, launch jobs with YAML or JSON config, toggle schedules and sensors, and follow runs through to their step logs. Clicking a definition opens its Python file in the connection’s source worktree. A failed run can send its traceback, target and source path to a new or existing Orca agent.

## Open the plugin

Use Orca 1.4.259 or newer. From this checkout, inspect the manifest and request a development load.

```sh
orca plugin inspect . --json
orca plugin dev load . --json
```

Orca displays the permissions and approval command for this trusted worker. After approving it in your installation, open **Dagster** in the right sidebar or run **Dagster: Open Sidebar** from the palette. Orca loads the TypeScript worker and the packaged HTML sidebar. After editing panel code, run `npm run build` to refresh the sidebar bundle. The worker uses Node’s built-in fetch, filesystem and the installed Orca CLI.

## Try the sample workspace

```sh
docker compose -f docker/compose.yaml up --build -d
```

The included [workspace configuration](.orca-dagster.json) supplies two profiles.

| Profile | Endpoint | Credentials |
| --- | --- | --- |
| Local (Docker) | `http://localhost:3000/graphql` | None |
| Remote Dev (Auth Proxy) | `http://localhost:3001/graphql` | Set `DAGSTER_DEV_REMOTE_TOKEN=dev-dagster-token` in the Orca host environment |

1. Select **Local (Docker)**. The sidebar shows **Attached** when Compose owns the running instance. For a local dev server started from the sidebar, **Logs**, **Stop**, and **Restart** act on its managed Orca terminal.
2. Select `raw_orders` under **Assets** to open its definition. Press **Materialize**, then use the returned run ID to inspect its progress.
3. Materialize `flaky_enrichment` to exercise failure handling. Open its failed run to inspect the Python traceback, re-execute from failure, or send the error to an agent. To see it pass, launch `flaky_debug_job` with the following config.

```yaml
ops:
  flaky_enrichment:
    config:
      should_fail: false
```

Stop the sample services and keep their run history with `docker compose -f docker/compose.yaml down`.

## Explore lineage and partitions

Open **DAG** to pan and zoom the dependency graph. Select a node to inspect its neighbors, latest run, or asset details. **Run overlay** shows the selected run’s steps on their asset nodes. Run detail shows attempt durations on a shared timeline and links verification runs to their parent.

Use **Refresh graph and Git changes** after editing a checkout. The graph distinguishes changed Python and dbt SQL files from downstream dependents. Filter with `changed`, `changed*`, `status:stale`, or `checks:failed`. **Materialize Changed**, **Materialize Changed + Downstream**, and **Materialize Stale** show the exact groups for review. Select a partition where applicable, then launch each group. **Dagster: Show Asset for Source File** opens a searchable file picker. A file can define several assets.

In **Assets**, choose **Partitioned assets** and open **Load partition grid**. Select a failed partition to inspect its run. Enter `failed`, one key, or an inclusive `start..end` range and preview the backfill. The sidebar tracks the launched backfill by ID. Use **Inspect column schema** to compare materializations and select a column to trace recorded lineage on the DAG. The sample tracks order amounts from `raw_orders` through `cleaned_orders` to `daily_revenue`.

For a failed run, choose an asset and **Compare with last success**. The comparison shows changed configuration keys, available revision tags, failed checks, and the latest reported columns. **Send Error to Agent** includes this context, the failing step, traceback, and a source excerpt. Use **Re-execute From Failure** to verify an edit.

## Configure a connection

Use **Manage profiles** to create, duplicate, test or edit a connection. Pin a profile to restore it when returning to that workspace. A saved edit to a repository profile becomes a user override. Deleting the override restores the repository default.

Use the OS keyring or reference an environment variable for Bearer, Basic, Dagster+ token or custom-header authentication. The worker resolves credentials when making requests. URLs, Basic usernames and custom header names accept `${ENV_VAR}`. Use a base HTTP(S) URL or its `/graphql` endpoint. For a local profile, an explicitly configured port applies to the connection.

Repo defaults live in `.orca-dagster.json` and follow the [JSON Schema](schemas/orca-dagster.schema.json). A source path mapping such as `/app/src` → `docker/sample_project/src` lets Orca open container paths locally. Use **Source worktree** in Manage profiles to bind a local checkout for source navigation, Git inspection, and agents. An unbound connection uses the focused workspace. For local connections, enter the code location name in **Reload on save** to reload after Python, SQL, or YAML writes. A load error displays the traceback and **Fix Load Error with Agent**. Both preferences are stored with the user profile on this machine. The worker uses code references, Python definitions and dbt project manifests to locate the file. In a fresh dbt checkout it can also find a uniquely named model under the models directory. Clicking a match opens the file at the resolved line.

## Verify a checkout

```sh
orca plugin types . --json
npm ci
npm run typecheck
npm run build
orca plugin validate . --json
orca plugin test . --json
orca plugin check . --json
```

For live verification, start the included Compose stack, then run the same suite with the fixture enabled.

```sh
DAGSTER_E2E=1 DAGSTER_DEV_REMOTE_TOKEN=dev-dagster-token orca plugin test . --json
```

The live suite creates runs, reloads the sample location and restores automation to its starting state. The explorer suite also launches and tracks a live backfill and follows column lineage across three materialized assets. Editor, terminal, keyring and agent actions use the Orca test host. Set `DAGSTER_E2E_LOCAL_URL` and `DAGSTER_E2E_REMOTE_URL` to run the live suite on alternate Compose ports. For the reload-recovery test, run the suite separately with `DAGSTER_E2E_RELOAD_ROOT` pointing to a disposable `/tmp/astra-roadmap-reload-*` fixture whose `src` directory is mounted in the sample code server. This test temporarily breaks and restores that copy of the definitions. The component fixtures cover empty, stopped, connected assets, automation, runs, failed detail, profile editor and auth error states.

| User stories | Verification |
| --- | --- |
| US-01–03 · profiles, credentials, repository defaults | [Profile persistence and pins](test/profiles.test.ts), [auth modes and redaction](test/credentials.test.ts), [profile editor](test/panel.test.ts) |
| US-04–05 · managed terminals and external attach | [Controller lifecycle, restart, ownership and crash diagnostics](test/controller.test.ts), [live Docker attach](test/integration.test.ts) |
| US-06–07 · reload and source navigation | [Palette reload and per-location errors](test/worker.test.ts), [source tiers](test/resolver.test.ts), [live source mappings](test/integration.test.ts) |
| US-08–10 · assets, partitions, jobs and automation | [Sidebar interactions](test/panel.test.ts), [GraphQL operations](test/client.test.ts), [live launches and toggles](test/integration.test.ts) |
| US-11–12 · polling, run details, logs and controls | [Visible and hidden panel polling](test/panel.test.ts), [log cursors and filtering](test/client.test.ts), [live retry and cancellation](test/integration.test.ts) |
| US-13–14 · agent handoff, status bar and commands | [Agent targeting and prompt size](test/handoff.test.ts), [worker commands, status transitions and notifications](test/worker.test.ts), [live Python traceback](test/integration.test.ts) |
| US-15 · local and authenticated remote sandbox | [Compose stack](docker/compose.yaml), [both live endpoints and auth schemes](test/integration.test.ts) |

The original [user stories](docs/user-stories.md), [wireframes](docs/wireframes.md), [architecture](docs/architecture.md) and [milestone prompts](prompts/) are also included. Orca requires kebab-case contribution IDs for panels and status items, so the manifest uses `dagster-sidebar` and `dagster-status`. Its commands and request names use the dotted IDs in the architecture.

# Prompt 05 — Integration & End-to-End Verification (Milestone 5)

## Goal
Run full end-to-end verification of the `dagster` Orca plugin against the runnable Docker dev stack (both local unauthenticated OSS on `:3000` and token-authenticated remote proxy on `:3001`), audit capabilities and security hygiene, and visually verify rendered panels.

## Required Reading Before Running
1. Read `docs/user-stories.md` (verify every US-01 through US-15 acceptance criterion).
2. Read `docker/compose.yaml` and `.orca-dagster.json`.
3. Run `orca skills get orca-plugin-authoring --reference references/workflow.md`.

## Tasks
1. **Start & Verify Docker Dev Environment**:
   - Run `docker compose -f docker/compose.yaml up --build -d`.
   - Wait until `dagster-webserver` (`http://localhost:3000/graphql`) and `remote-auth-proxy` (`http://localhost:3001/graphql`) are healthy.
   - Verify `http://localhost:3001/graphql` returns HTTP `401` without auth and HTTP `200` with `Authorization: Bearer dev-dagster-token` and with `Dagster-Cloud-Api-Token: dev-dagster-token`.
2. **Run Automated Integration Suite (`test/integration.test.ts`)**:
   - Add an opt-in integration test suite (gated by `DAGSTER_E2E=1`) exercising against live Docker endpoints:
     - Local OSS (`:3000`): fetch snapshot, verify `raw_orders`, `cleaned_orders`, `daily_revenue` (partitioned), `flaky_enrichment`, `orders_pipeline_job`, `daily_orders_schedule`, `new_orders_sensor`.
     - Remote Proxy (`:3001`): verify `env-var` auth with `DAGSTER_DEV_REMOTE_TOKEN=dev-dagster-token` succeeds and missing/wrong token returns structured `AuthError`.
     - Materialize `raw_orders` → poll until `SUCCESS`.
     - Materialize `flaky_enrichment` (default config `should_fail: true`) → poll until `FAILURE` → verify `fetchRunDetail` captures step error & traceback → verify `agent.sendRunError` prompt includes run ID, resolved local file `docker/sample_project/src/sample_project/assets.py`, and Python traceback.
     - Reload code location `sample_project` via `reloadRepositoryLocation`.
     - Toggle `daily_orders_schedule` and `new_orders_sensor` on and back off.
     - Jump-to-source resolution for `raw_orders` (Tier 1 via `pathPrefixMap`) and `daily_orders_schedule` (Tier 2 symbol scan).
3. **Orca Plugin Verification Gate**:
   - Run `orca plugin validate . --json` — must exit `0`.
   - Run `orca plugin inspect . --json` — verify contributed panels, commands, status bar items, settings, and minimal capabilities match `docs/architecture.md`.
   - Run `orca plugin test . --json` — must exit `0`.
   - Run `orca plugin check . --json` — must exit `0` with zero dropped props or warnings.
4. **Security & Hygiene Audit**:
   - Verify zero raw tokens/secrets appear in logs (`orca.log`), error messages, or `.orca-dagster.json`.
   - Verify no `orca-` prefix in `orca-plugin.json` `id` and no `.orca/` files tracked in git.
   - If the user has enabled plugins and approves dev loading, capture panel screenshots with `orca plugin shot` to visually inspect light and dark themes.
5. **Teardown**:
   - Stop Docker stack with `docker compose -f docker/compose.yaml down` (keep volumes unless clean reset requested) and report test evidence for every US-01 through US-15.

## Exit Criteria
- All unit, panel, check, and live Docker E2E tests pass with concrete command output.
- Every user story in `docs/user-stories.md` is verified or explicitly called out with any remaining gap.

# Prompt 02 — Connection Profiles, Dynamic Credentials & GraphQL Client (Milestone 2)

## Goal
Implement the connection profile store (merging `.orca-dagster.json` and `orca.storage` with workspace pinning), dynamic credential resolver (`orca-secret` and `env-var` + `${ENV_VAR}` interpolation), and the pure-TypeScript Dagster GraphQL client supporting both Dagster OSS and Dagster+.

## Required Reading Before Editing
1. Read `docs/architecture.md` (Sections 3, 4, 5).
2. Read `docs/user-stories.md` (US-01, US-02, US-03, US-08, US-09, US-10, US-11, US-12).
3. Read `schemas/orca-dagster.schema.json` and `.orca-dagster.json`.
4. Read `src/types.ts` and `src/worker.ts`.

## Tasks
1. **Dynamic Credential Resolver (`src/profiles/credentials.ts`)**:
   - Implement `interpolateEnv(input: string, env: Record<string, string | undefined>): string` replacing `${VAR_NAME}` and throwing a typed `MissingEnvVarError(varName)` if missing.
   - Implement `resolveAuthHeaders(profile, { secrets, env, secretOverride? })`:
     - Supports `none`, `bearer`, `basic`, `dagster-plus-token` (`Dagster-Cloud-Api-Token`), `custom-header`.
     - Supports `source: 'env-var'` (reads `env[auth.envVar]`) and `source: 'orca-secret'` (reads `secrets.get(auth.secretKey)`).
     - Never includes raw secret values in error messages or logs.
   - Implement `normalizeGraphqlUrl(rawUrl: string): string` appending `/graphql` when absent.
2. **Profile Store (`src/profiles/store.ts`)**:
   - Read and validate `<workspacePath>/.orca-dagster.json` (silently skip if absent; surface validation warning if malformed; reject raw secret fields).
   - Merge repo profiles (`origin: 'repo'`) with user profiles from `orca.storage` (`origin: 'user'`), where user profiles override repo profiles with the same `id`.
   - Persist per-workspace pinned profile IDs under `profiles.pins` in `orca.storage`.
   - Handle `secrets.protection()` check and secret storage on save (`secrets.set`) / cleanup on delete (`secrets.delete`).
3. **Dagster GraphQL Client (`src/dagster/queries.ts`, `src/dagster/client.ts`, `src/dagster/types.ts`)**:
   - Use Node's built-in `fetch` with `AbortSignal` and `15s` timeout.
   - Implement every operation listed in `docs/architecture.md` Section 5:
     - `ping()` (`{ version }`)
     - `fetchSnapshot()` (code locations + load errors, assets with `CodeReferencesMetadataEntry` and latest materialization, jobs, schedules, sensors, recent runs)
     - `fetchAssetPartitions(assetKey, limit)`
     - `materializeAssets({ assetKeys, partitionKey, runConfigYaml, tags })`
     - `launchJob({ locationName, repositoryName, jobName, runConfigYaml, tags })`
     - `toggleSchedule(...)` / `toggleSensor(...)`
     - `fetchRunDetail(runId, { errorsOnly, limit })`
     - `reexecuteRun(runId, strategy)` / `terminateRun(runId)`
     - `reloadLocation(locationName)`
   - Classify HTTP `401`/`403` as structured `AuthError` and GraphQL `PythonError` unions into readable error payloads.
4. **Wire into `src/worker.ts`**:
   - Connect `profiles.*` and `dagster.*` panel request handlers to the store and client.
5. **Unit & Live Docker Tests**:
   - `test/credentials.test.ts`: test all 5 auth modes, `env-var` vs `orca-secret`, missing env var errors, `${ENV_VAR}` URL interpolation, and redaction.
   - `test/client.test.ts`: unit tests with mocked `fetch` covering OSS/Dagster+ responses, `401` auth errors, `PythonError` unions, and pagination.
   - Optional live verification against `docker/compose.yaml`:
     - `http://localhost:3000/graphql` (unauthenticated local OSS)
     - `http://localhost:3001/graphql` (auth-proxied remote: verify 401 without token and 200 with `DAGSTER_DEV_REMOTE_TOKEN=dev-dagster-token`).

## Exit Criteria (Run & Verify)
1. `orca plugin validate . --json` exits `0`.
2. `orca plugin test . --json` exits `0` with all unit tests passing.
3. If Docker stack is running (`docker compose -f docker/compose.yaml up -d`), `ping()` and `fetchSnapshot()` succeed against both `:3000` and `:3001` (with token).

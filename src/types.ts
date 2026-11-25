export type ProfileKind = 'local' | 'remote-oss' | 'dagster-plus'
export type AuthMode = 'none' | 'bearer' | 'basic' | 'dagster-plus-token' | 'custom-header'
export type CredentialSource = 'orca-secret' | 'env-var'
export type AuthConfig = {
  mode: AuthMode
  source?: CredentialSource
  secretKey?: string
  envVar?: string
  username?: string
  headerName?: string
}
export type LocalConfig = {
  command?: string
  port?: number
  cwd?: string
  pathPrefixMap?: Record<string, string>
}
export type ConnectionProfile = {
  id: string
  name: string
  kind: ProfileKind
  url: string
  auth: AuthConfig
  local?: LocalConfig
  sourceWorkspaceId?: string
  reloadLocationOnSave?: string
  origin?: 'repo' | 'user'
  /** Retained when an override or copy inherits a repository launch command. */
  repositoryCommand?: true
}
export type ProfileList = {
  profiles: ConnectionProfile[]
  activeProfileId: string | null
  workspaceId: string | null
  pinnedProfileId?: string | null
  keyringStatus: 'os-keyring' | 'unavailable'
  warning?: string
  sourceWorkspaces?: { id: string; name: string }[]
  showSourcePicker?: boolean
  sourcePickerPath?: string
  showProfiles?: boolean
  credentialEnvPresent?: Record<string, boolean>
}
export type LocalCommand = { command: string; cwd: string }
export type LocalState = {
  confirmation?: LocalCommand
  status: 'managed-running' | 'attached' | 'starting' | 'crashed' | 'stopped'
  terminalId?: string
  diagnostics?: string
}
export type ConnectionStatus =
  'connected' | 'connecting' | 'degraded' | 'stopped' | 'auth-error' | 'error' | 'unconfigured'
export type CodeReference =
  { filePath: string; lineNumber?: number; label?: string } | { url: string; label?: string }
export type CodeLocation = { name: string; loadStatus: string; error?: string }
export type Repository = { locationName: string; repositoryName: string }
export type AssetSummary = Repository & {
  assetKey: string[]
  running?: boolean
  staleStatus?: string
  opNames?: string[]
  groupName: string
  description?: string
  isPartitioned: boolean
  dbtUniqueId?: string
  codeReferences: CodeReference[]
  lastMaterializedAt?: number
  lastRunId?: string
  status: 'success' | 'danger' | 'warning' | 'neutral'
  jobNames: string[]
}
export type JobSummary = Repository & { name: string; description?: string }
export type InstigationSummary = JobSummary & {
  status: 'RUNNING' | 'STOPPED'
  tick?: { status: string; timestamp: number; error?: string }
}
export type ScheduleSummary = InstigationSummary & { cronSchedule: string }
export type SensorSummary = InstigationSummary & { minIntervalSeconds?: number }
export type RunSummary = {
  runId: string
  pipelineName: string
  status: string
  startTime?: number
  endTime?: number
  createdAt?: number
}
/** All times on the panel protocol are Unix milliseconds. */
export type RunDetail = RunSummary & {
  locationName?: string
  repositoryName?: string
  assetKeys: string[][]
  steps: {
    attempts?: { startTime?: number; endTime?: number }[]
    key: string
    status: string
    startTime?: number
    endTime?: number
  }[]
  logs: { timestamp: number; level: string; message: string; stepKey?: string; error?: string }[]
  error?: string
  failedChecks?: { assetKey: string[]; name: string }[]
  parentRunId?: string
  partitionKey?: string
  agentFix?: 'sent' | 'edited'
  comparison?: string
  source?: SourceResult
}
export type SnapshotData = {
  /** Include runs that started before the first page of recent history. */
  activeRunCount?: number
  locations: CodeLocation[]
  assets: AssetSummary[]
  jobs: JobSummary[]
  schedules: ScheduleSummary[]
  sensors: SensorSummary[]
  runs: RunSummary[]
}
export type Snapshot = SnapshotData & {
  status: ConnectionStatus
  syncedAt: number
  uiUrl?: string
  localState?: LocalState
  error?: string
}
export type Scope = { profileId?: string; workspaceId?: string | null }
export type RunConfig = { runConfigYaml?: string; tags?: Record<string, string> }
export type MaterializeInput = RunConfig & { assetKeys: string[][]; partitionKey?: string }
export type LaunchInput = RunConfig & Repository & { jobName: string }
export type ScheduleInput = Repository & { scheduleName: string; running: boolean }
export type SensorInput = Repository & { sensorName: string; running: boolean }
export type RunFilter = 'All' | 'Active' | 'Success' | 'Failure' | 'Canceled'
export type RunsInput = { filter?: RunFilter; cursor?: string; limit?: number }
export type RunPage = { runs: RunSummary[]; hasMore: boolean }
export type Partitions = {
  kind: 'none' | 'single' | 'multi'
  keys: string[]
  dimensionName?: string
}
export type SourceInput = {
  opNames?: string[]
  kind: 'asset' | 'job' | 'schedule' | 'sensor'
  name: string
  assetKey?: string[]
  dbtUniqueId?: string
  codeReferences?: CodeReference[]
  locationName?: string
  repositoryName?: string
}
export type SourceResult = {
  opened: boolean
  resolvedPath?: string
  lineNumber?: number
  externalUrl?: string
  reason?: string
}
export type AgentTargets = {
  defaultAgent: string
  terminals: { terminalId: string; title: string; agent: string; state: string }[]
}
export type SendErrorInput = {
  assetKey?: string[]
  runId: string
  mode: 'launch' | 'existing'
  terminalId?: string
}
export type RunStrategy = 'ALL_STEPS' | 'FROM_FAILURE'
export type StateChange = {
  activeProfileId: string | null
  status: ConnectionStatus
  activeRunCount: number
  syncedAt: number
}

export function isActiveRun(status: string): boolean {
  return ['QUEUED', 'STARTING', 'STARTED', 'CANCELING', 'MANAGED', 'NOT_STARTED'].includes(status)
}
export function countActiveRuns(data: SnapshotData | null): number {
  return data?.activeRunCount ?? data?.runs.filter((run) => isActiveRun(run.status)).length ?? 0
}

type Request<I, O> = { input: I; output: O }
type Scoped<I, O> = Request<I & Scope, O>
type ToggleResult = { status: 'RUNNING' | 'STOPPED' }
type RunId = { runId: string }
type State = { state: LocalState }
export type ReloadResult = { results: { locationName: string; ok: boolean; error?: string }[] }
declare module 'orca' {
  interface PluginRequests {
    'profiles.list': Request<Record<string, never> | undefined, ProfileList>
    'profiles.save': Scoped<
      {
        profile: ConnectionProfile
        pinToWorkspace?: boolean
        secretValue?: string
        duplicateFrom?: string
      },
      { profile: ConnectionProfile }
    >
    'profiles.delete': Scoped<{ profileId: string }, { ok: true }>
    'profiles.select': Scoped<
      { profileId: string; pinToWorkspace?: boolean },
      { activeProfileId: string }
    >
    'profiles.test': Request<
      { profile: ConnectionProfile; secretValue?: string },
      { ok: boolean; version?: string; error?: string; latencyMs: number }
    >
    'dagster.snapshot': Request<Scope, Snapshot>
    'dagster.reload': Scoped<{ locationNames?: string[] }, ReloadResult>
    'dagster.assetPartitions': Scoped<{ assetKey: string[]; limit?: number }, Partitions>
    'dagster.materialize': Scoped<MaterializeInput, RunId>
    'dagster.launchJob': Scoped<LaunchInput, RunId>
    'dagster.toggleSchedule': Scoped<ScheduleInput, ToggleResult>
    'dagster.toggleSensor': Scoped<SensorInput, ToggleResult>
    'dagster.runs': Scoped<RunsInput, RunPage>
    'dagster.runDetail': Scoped<
      RunId & { errorsOnly?: boolean; limit?: number },
      { run: RunDetail }
    >
    'dagster.reexecuteRun': Scoped<RunId & { strategy: RunStrategy }, RunId>
    'dagster.terminateRun': Scoped<RunId, { ok: boolean; message?: string }>
    'local.start': Scoped<{ confirmed?: LocalCommand }, State>
    'local.stop': Request<Scope, State>
    'local.restart': Scoped<{ confirmed?: LocalCommand }, State>
    'local.focusLogs': Request<Scope, { focused: boolean }>
    'dagster.graph': Scoped<
      { token?: string; offset?: number },
      { nodes: GraphNode[]; token: string; next?: number }
    >
    'dagster.health': Request<Scope, HealthRow[]>
    'source.index': Scoped<{ token: string }, SourceIndex>
    'dagster.columnFlow': Scoped<
      { assetKey: string[]; assetId?: string; token?: string; column: string; partition?: string },
      ColumnFlow[]
    >
    'dagster.columns': Scoped<{ assetKey: string[]; partition?: string }, ColumnHistory>
    'dagster.partitionPage': Scoped<
      { token?: string; assetKey: string[]; cursor?: string; fixed?: Record<string, string> },
      PartitionPage
    >
    'dagster.prepareBackfill': Scoped<
      { token?: string; assetKey: string[]; selection: string; fixed?: Record<string, string> },
      { keys: string[]; targets: BackfillPreview[] }
    >
    'dagster.partitionRun': Scoped<{ assetKey: string[]; partition: string }, { runId?: string }>
    'dagster.previewBackfill': Scoped<BackfillSelection, BackfillPreview[]>
    'dagster.launchBackfill': Scoped<BackfillSelection, { backfillId: string }>
    'dagster.backfillStatus': Scoped<{ backfillId: string }, { status: string; progress: string }>
    'dagster.compareFailure': Scoped<{ runId: string; assetKey: string[] }, { comparison: string }>
    'agent.loadError': Scoped<{ locationName: string }, { launched: boolean }>
    'source.open': Scoped<SourceInput, SourceResult>
    'agent.listTargets': Request<Scope, AgentTargets>
    'agent.sendRunError': Scoped<SendErrorInput, { launched: boolean; terminalId?: string }>
  }
  interface PluginPanelEvents {
    'dagster.sourcePicker': {}
    'dagster.definitionsChanged': Scope
    'dagster.stateChanged': StateChange
    'dagster.workspaceChanged': { workspaceId: string | null; activeProfileId: string | null }
    'dagster.showProfiles': {}
  }
}

export type GraphNode = AssetSummary & {
  health?: Omit<HealthRow, 'id' | 'assetKey'>
  kinds?: string[]
  tags?: Record<string, string>
  owners?: string[]
  isMaterializable?: boolean
  keyString: string
  dependencyKeys: string[]
  dependedByKeys?: string[]
}
export type SourceIndex = {
  files: { path: string; assetIds: string[] }[]
  changed: string[]
  ambiguous?: string[]
  gitError?: string
}
export type HealthRow = {
  id: string
  assetKey: string[]
  checkCount?: number
  freshness?: string
  failed?: string[]
  causes?: string[]
}
export type Column = { name: string; type: string; description?: string }
export type ColumnFlow = { id: string; column: string; upstreamIds: string[] }
export type ColumnEntry = {
  columnName: string
  columnDeps: { assetKey: { path: string[] }; columnName: string }[]
}
export type ColumnHistory = { columns?: Column[]; previous?: Column[]; lineage: ColumnEntry[] }
export type PartitionPage = {
  token?: string
  rows: { key: string; label?: string; status: string }[]
  cursor: string
  hasMore: boolean
  calendar?: boolean
  dimensions?: { name: string; partitionKeys: string[] }[]
  fixed?: Record<string, string>
}
export type BackfillSelection = { assetKey: string[]; keys: string[] }
export type BackfillPreview = {
  assetKey: { path: string[] }
  partitions: { partitionKeys?: string[]; ranges?: { start: string; end: string }[] } | null
}

// Dagster timestamps on event records are millisecond strings, while run and tick times are seconds.
const trace = 'message stack errorChain { error { message stack } }'
const errors = `__typename ... on Error { message } ... on PythonError { ${trace} }`
const run = 'runId pipelineName status startTime endTime creationTime'
const repo = 'repository { name location { name } }'
const state = `status ticks(limit: 1) { status timestamp error { ${trace} } }`
const launchResult = `${errors} ... on RunConfigValidationInvalid { errors { message } }
  ... on LaunchRunSuccess { run { runId } }`

export const ping = 'query Ping { version }'
export const locationNames = `query LocationNames {
  workspaceOrError { ${errors} ... on Workspace { locationEntries { name } } }
}`
export const assetOptions = {
  stale: 'staleStatus',
  refs: '... on CodeReferencesMetadataEntry { codeReferences { ... on LocalFileCodeReference { filePath lineNumber label } ... on UrlCodeReference { url label } } }'
}
export function assetFields(fields: typeof assetOptions) {
  return `assetKey { path } groupName description isPartitioned ${fields.stale} jobNames ${repo}
    assetMaterializations(limit: 1) { runId timestamp }
    metadataEntries { ... on TextMetadataEntry { label text } ${fields.refs} }`
}
export const snapshot = (fields: typeof assetOptions) => `query Snapshot {
  workspaceOrError { ${errors} ... on Workspace { locationEntries {
    name loadStatus locationOrLoadError { __typename ... on PythonError { ${trace} }
      ... on RepositoryLocation { repositories { name pipelines { name isJob description }
        schedules { name description cronSchedule scheduleState { ${state} } }
        sensors { name description minIntervalSeconds sensorState { ${state} } }
      } }
    }
  } } }
  assetNodes { ${assetFields(fields)} }
  runsOrError(limit: 25) { ${errors} ... on Runs { results { ${run} } } }
  activeRuns: runsOrError(filter: { statuses: [QUEUED, STARTING, STARTED, CANCELING, MANAGED, NOT_STARTED] }) {
    ... on Runs { count }
  }
}`
export const latest = `query Latest($keys: [AssetKeyInput!]!) {
  assetsLatestInfo(assetKeys: $keys) { assetKey { path } latestRun { runId status }
    inProgressRunIds unstartedRunIds }
}`
export const runs = `query RunsPage($filter: RunsFilter, $cursor: String, $limit: Int!) {
  runsOrError(filter: $filter, cursor: $cursor, limit: $limit) {
    ${errors} ... on Runs { results { ${run} } }
  }
}`
export const partitions = `query Partitions($key: AssetKeyInput!, $limit: Int!) {
  assetNodeOrError(assetKey: $key) { __typename ... on Error { message } ... on AssetNode { partitionDefinition { dimensionTypes { name } } partitionKeyConnection(limit: $limit, ascending: false) { results } } }
}`
export const selection = `query MaterializationTarget($keys: [AssetKeyInput!]!) {
  assetNodes(assetKeys: $keys) { assetKey { path } jobs { name } isMaterializable ${repo} }
}`
export const launch = `mutation Launch($params: ExecutionParams!) { launchRun(executionParams: $params) { ${launchResult} } }`
export const reexecute = `mutation Reexecute($params: ReexecutionParams!) {
  launchRunReexecution(reexecutionParams: $params) { ${launchResult} }
}`
export const terminate = `mutation Terminate($id: String!) {
  terminateRun(runId: $id) { ${errors} ... on TerminateRunFailure { message } }
}`
export const reload = `mutation Reload($name: String!) {
  reloadRepositoryLocation(repositoryLocationName: $name) { ${errors} ... on WorkspaceLocationEntry {
    locationOrLoadError { __typename ... on PythonError { ${trace} } }
  } }
}`
export const detail = `query RunDetail($id: ID!) { runOrError(runId: $id) {
  ${errors} ... on Run { ${run} parentRunId tags {key value} repositoryOrigin { repositoryLocationName repositoryName }
    assetSelection { path } assets { key { path } } stepStats { stepKey status startTime endTime attempts {startTime endTime} }
  }
} }`
export const logs = `query RunLogs($id: ID!, $cursor: String, $limit: Int!) {
  logsForRun(runId: $id, afterCursor: $cursor, limit: $limit) { ${errors} ... on EventConnection {
    hasMore cursor events { __typename ... on MessageEvent { timestamp level message stepKey }
      ... on ErrorEvent { error { ${trace} } } ... on AssetCheckEvaluationEvent {evaluation {success checkName assetKey {path}}} }
  } }
}`
export const scheduleLookup = `query ScheduleState($selector: ScheduleSelector!) {
  scheduleOrError(scheduleSelector: $selector) { ${errors} ... on Schedule { scheduleState { id } } }
}`
export const sensorLookup = `query SensorState($selector: SensorSelector!) {
  sensorOrError(sensorSelector: $selector) { ${errors} ... on Sensor { sensorState { id } } }
}`
const scheduleResult = `${errors} ... on ScheduleStateResult { scheduleState { status } }`
export const startSchedule = `mutation StartSchedule($selector: ScheduleSelector!) {
  startSchedule(scheduleSelector: $selector) { ${scheduleResult} }
}`
export const stopSchedule = `mutation StopSchedule($id: String!) { stopRunningSchedule(id: $id) { ${scheduleResult} } }`
export const startSensor = `mutation StartSensor($selector: SensorSelector!) {
  startSensor(sensorSelector: $selector) { ${errors} ... on Sensor { sensorState { status } } }
}`
export const stopSensor = `mutation StopSensor($id: String!) {
  stopSensor(id: $id) { ${errors} ... on StopSensorMutationResult { instigationState { status } } }
}`

import type {
  AssetSummary,
  CodeReference,
  InstigationSummary,
  Repository,
  RunSummary
} from '../types'

export type Trace = { message: string; stack?: string[]; errorChain?: { error: Trace }[] }
export type Result = {
  __typename: string
  message?: string
  stack?: string[]
  errorChain?: Trace['errorChain']
  errors?: { message: string }[]
}
export function formatError(value: Result | Trace): string {
  const parts = [value.message, ...(value.stack ?? [])]
  for (const link of value.errorChain ?? []) parts.push('Caused by', formatError(link.error))
  if ('errors' in value) parts.push(...(value.errors ?? []).map((error) => error.message))
  return parts.filter(Boolean).join('\n').trim()
}

export type Key = { path: string[] }
export type Target = { assetKey: Key; repository: { name: string; location: { name: string } } }
export function repository(node: Target): Repository {
  return { repositoryName: node.repository.name, locationName: node.repository.location.name }
}
export type RawAsset = Target &
  Pick<AssetSummary, 'groupName' | 'isPartitioned' | 'jobNames'> & {
    description: string | null
    staleStatus: string | null
    assetMaterializations: { timestamp: string; runId: string }[]
    metadataEntries: { label?: string; text?: string; codeReferences?: CodeReference[] }[]
  }
export type Latest = {
  assetKey: Key
  latestRun: { runId: string; status: string } | null
  inProgressRunIds: string[]
  unstartedRunIds: string[]
}
export function assetSummary(asset: RawAsset, latest?: Latest): AssetSummary {
  const materialization = asset.assetMaterializations[0]
  let status: AssetSummary['status'] = materialization ? 'success' : 'neutral'
  if (asset.staleStatus === 'STALE') status = 'warning'
  if (latest?.latestRun?.status === 'FAILURE' && latest.latestRun.runId !== materialization?.runId)
    status = 'danger'
  if (latest && (latest.inProgressRunIds.length || latest.unstartedRunIds.length))
    status = 'warning'
  return {
    ...repository(asset),
    assetKey: asset.assetKey.path,
    running: Boolean(latest?.inProgressRunIds.length || latest?.unstartedRunIds.length),
    staleStatus: asset.staleStatus ?? undefined,
    groupName: asset.groupName,
    description: asset.description ?? undefined,
    isPartitioned: asset.isPartitioned,
    jobNames: asset.jobNames,
    dbtUniqueId: asset.metadataEntries.find(
      (entry) => entry.label === 'dagster_dbt/unique_id' || entry.label === 'dbt_unique_id'
    )?.text,
    codeReferences: asset.metadataEntries.flatMap(
      (entry) =>
        entry.codeReferences ??
        (entry.label === 'repository_path' && entry.text ? [{ filePath: entry.text }] : [])
    ),
    lastMaterializedAt: materialization ? Number(materialization.timestamp) : undefined,
    lastRunId: latest?.latestRun?.runId ?? materialization?.runId,
    status
  }
}
export function milliseconds(seconds?: number | null) {
  return seconds == null ? undefined : seconds * 1000
}
export type RawRun = Pick<RunSummary, 'runId' | 'pipelineName' | 'status'> & {
  startTime: number | null
  endTime: number | null
  creationTime: number
}
export function runSummary(run: RawRun): RunSummary {
  return {
    runId: run.runId,
    pipelineName: run.pipelineName,
    status: run.status,
    startTime: milliseconds(run.startTime),
    endTime: milliseconds(run.endTime),
    createdAt: milliseconds(run.creationTime)
  }
}

export type State = Pick<InstigationSummary, 'status'>
type Automation = { name: string; description: string | null }
type RawState = State & { ticks: { status: string; timestamp: number; error?: Trace }[] }
export type RawRepository = {
  name: string
  pipelines: (Automation & { isJob: boolean })[]
  schedules: (Automation & { cronSchedule: string; scheduleState: RawState })[]
  sensors: (Automation & { minIntervalSeconds: number; sensorState: RawState })[]
}
export function instigation(
  repo: Repository,
  item: Automation,
  state: RawState,
  redact: (s: string) => string
): InstigationSummary {
  const tick = state.ticks[0]
  return {
    ...repo,
    name: item.name,
    description: item.description ?? undefined,
    status: state.status,
    tick: tick && {
      status: tick.status,
      timestamp: tick.timestamp * 1000,
      error: tick.error ? redact(formatError(tick.error)) : undefined
    }
  }
}
export type LocationEntry = {
  name: string
  loadStatus: string
  locationOrLoadError: (Result & { repositories: RawRepository[] }) | null
}
export type RawDetail = RawRun & {
  parentRunId?: string
  tags?: { key: string; value: string }[]
  repositoryOrigin?: { repositoryLocationName: string; repositoryName: string }
  assets: { key: Key }[]
  assetSelection?: Key[]
  stepStats: {
    attempts?: { startTime?: number; endTime?: number }[]
    stepKey: string
    status: string | null
    startTime?: number
    endTime?: number
  }[]
}
export type LogEvent = {
  evaluation?: { success: boolean; checkName: string; assetKey: Key }
  __typename: string
  timestamp: string
  level: string
  message: string
  stepKey?: string | null
  error?: Trace
}

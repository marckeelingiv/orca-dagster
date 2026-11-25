import { createHash } from 'node:crypto'
import type {
  LaunchInput,
  MaterializeInput,
  Partitions,
  Repository,
  RunConfig,
  RunDetail,
  RunPage,
  RunsInput,
  RunStrategy,
  ScheduleInput,
  SensorInput,
  SnapshotData
} from '../types'
import * as q from './queries'
import {
  assetSummary,
  formatError,
  instigation,
  milliseconds,
  repository,
  runSummary
} from './types'
import type {
  Key,
  Latest,
  LocationEntry,
  LogEvent,
  RawAsset,
  RawDetail,
  RawRun,
  Result,
  State,
  Target
} from './types'

export class DagsterApiError extends Error {
  readonly name: string = 'DagsterApiError'
  constructor(
    message: string,
    readonly code: string,
    readonly status?: number
  ) {
    super(message)
  }
}
export class AuthError extends DagsterApiError {
  override readonly name = 'AuthError'
  constructor(message: string, status?: number) {
    super(message, 'UNAUTHORIZED', status)
  }
}
type Options = {
  beforeMutation?: () => Promise<void>
  signal?: AbortSignal
  timeoutMs?: number
  fetch?: typeof fetch
}
type Variables = Record<string, unknown>
const filters = {
  Active: ['QUEUED', 'STARTING', 'STARTED', 'CANCELING', 'MANAGED', 'NOT_STARTED'],
  Success: ['SUCCESS'],
  Failure: ['FAILURE'],
  Canceled: ['CANCELED']
}
function limit(value: number | undefined, fallback: number) {
  return Number.isFinite(value) ? Math.max(1, Math.min(2147483646, Math.floor(value!))) : fallback
}
function selector(repo: Repository) {
  return { repositoryLocationName: repo.locationName, repositoryName: repo.repositoryName }
}

type LogRow = RunDetail['logs'][number]
type LogHistory = {
  cursor?: string
  all: LogRow[]
  errors: LogRow[]
  failedChecks?: RunDetail['failedChecks']
  stepFailure?: string
  failure?: string
}
type LogPage = { events: LogEvent[]; cursor: string; hasMore: boolean }

// The worker may construct a new client on each poll. Keep cursors scoped to the
// transport, endpoint, credentials, and run; retain no raw credentials in keys.
const histories = new WeakMap<typeof fetch, Map<string, LogHistory>>()
const maxLogRows = 1000
const maxPollPages = 4
const maxFailurePages = 12
const unsupportedFields = new WeakMap<typeof fetch, Map<string, Set<string>>>()
const legacyEndpoints = new WeakMap<typeof fetch, Set<string>>()
function excerpt(text: string): string {
  return text.length <= 8192 ? text : text.slice(0, 4096) + '\n…\n' + text.slice(-4096)
}
function boundedTail(rows: LogRow[]): LogRow[] {
  let chars = 0
  let start = rows.length
  while (start > 0 && rows.length - start < maxLogRows) {
    const row = rows[start - 1]
    chars += row.message.length + (row.error?.length ?? 0)
    if (chars > 256 * 1024) break
    start--
  }
  return rows.slice(start)
}
const legacyPartitions = `query LegacyPartitions($key: AssetKeyInput!) {
  assetNodeOrError(assetKey: $key) { __typename ... on Error { message } ... on AssetNode {
    partitionKeysByDimension { name partitionKeys }
  } }
}`

export class DagsterClient {
  private readonly secrets: string[]
  constructor(
    private readonly endpoint: string,
    private readonly headers: Record<string, string>,
    private readonly options: Options = {}
  ) {
    const secrets = Object.values(headers)
    for (const [name, value] of Object.entries(headers)) {
      if (name.toLowerCase() !== 'authorization') continue
      const auth = /^(basic|bearer)\s+(.+)$/i.exec(value)
      if (!auth) continue
      secrets.push(auth[2])
      if (auth[1].toLowerCase() === 'basic') {
        const decoded = Buffer.from(auth[2], 'base64').toString()
        secrets.push(decoded, decoded.slice(decoded.indexOf(':') + 1))
      }
    }
    this.secrets = [...new Set(secrets.filter((secret) => secret.length >= 4))].sort(
      (a, b) => b.length - a.length
    )
  }
  redact(text: string) {
    for (const secret of this.secrets) text = text.split(secret).join('[redacted]')
    return text
  }
  /** Retry read queries only for schema validation errors naming an optional field or type. */
  protected async compatible<K extends string, T>(
    operation: string,
    optional: Record<K, string>,
    load: (fields: Record<K, string>) => Promise<T>
  ): Promise<T> {
    const transport = this.options.fetch ?? fetch
    const byEndpoint = unsupportedFields.get(transport) ?? new Map<string, Set<string>>()
    unsupportedFields.set(transport, byEndpoint)
    const key = JSON.stringify([this.endpoint, operation])
    const omitted = new Set(byEndpoint.get(key))
    for (;;) {
      const fields = { ...optional }
      for (const name of omitted) fields[name as K] = ''
      try {
        return await load(fields)
      } catch (e) {
        if (!(e instanceof DagsterApiError) || e.code !== 'GRAPHQL_ERROR') throw e
        const names = [
          ...e.message.matchAll(
            /(?:Cannot query field|Unknown (?:field|type|argument)) ['"]([\w]+)['"]/gi
          )
        ].map((m) => m[1])
        const missing = (Object.keys(fields) as K[]).filter((k) =>
          names.some((name) => fields[k].match(/\w+/g)?.includes(name))
        )
        if (!missing.length) throw e
        for (const field of missing) omitted.add(field)
        if (byEndpoint.size >= 128) byEndpoint.delete(byEndpoint.keys().next().value!)
        byEndpoint.set(key, omitted)
      }
    }
  }
  protected async request<T>(query: string, variables: Variables = {}): Promise<T> {
    if (query.trimStart().startsWith('mutation')) await this.options.beforeMutation?.()
    const timeout = AbortSignal.timeout(this.options.timeoutMs ?? 15000)
    const signal = this.options.signal ? AbortSignal.any([timeout, this.options.signal]) : timeout
    try {
      const response = await (this.options.fetch ?? fetch)(this.endpoint, {
        method: 'POST',
        headers: { ...this.headers, 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, variables }),
        redirect: 'error',
        signal
      })
      if ([401, 403].includes(response.status))
        throw new AuthError(
          'Dagster rejected the credentials (HTTP ' + response.status + ').',
          response.status
        )
      const payload = (await response.json().catch(() => null)) as {
        data?: T
        errors?: { message: string; extensions?: { code?: string } }[]
      }
      if (payload?.errors?.length) {
        const message = this.redact(payload.errors.map((error) => error.message).join('\n'))
        if (
          payload.errors.some((error) =>
            ['UNAUTHENTICATED', 'FORBIDDEN'].includes(error.extensions?.code ?? '')
          )
        )
          throw new AuthError(message)
        throw new DagsterApiError(message, 'GRAPHQL_ERROR', response.status)
      }
      if (!response.ok)
        throw new DagsterApiError(
          'Dagster returned HTTP ' + response.status,
          'HTTP_ERROR',
          response.status
        )
      if (!payload?.data) throw new DagsterApiError('Missing GraphQL data', 'INVALID_RESPONSE')
      return payload.data
    } catch (error) {
      if (error instanceof DagsterApiError) throw error
      if (signal.aborted)
        throw new DagsterApiError(
          timeout.aborted ? 'Dagster request timed out' : 'Dagster request canceled',
          timeout.aborted ? 'TIMEOUT' : 'ABORTED'
        )
      throw new DagsterApiError(
        this.redact(error instanceof Error ? error.message : 'Dagster request failed'),
        'REQUEST_ERROR'
      )
    }
  }
  private expect<T extends Result>(result: T, expected: string): T {
    if (result?.__typename === expected) return result
    const code = result?.__typename ?? 'INVALID_RESPONSE'
    const message = this.redact((result && formatError(result)) || code)
    if (code === 'UnauthorizedError') throw new AuthError(message)
    throw new DagsterApiError(message, code)
  }
  protected async operation<T>(
    query: string,
    variables: Variables,
    field: string,
    expected: string
  ): Promise<T> {
    const data = await this.request<Record<string, T & Result>>(query, variables)
    return this.expect(data[field], expected)
  }
  async ping(): Promise<string> {
    const { version } = await this.request<{ version?: unknown }>(q.ping)
    if (typeof version !== 'string' || !/^\d+(?:\.\d+)+(?:[-+.a-zA-Z][\w.+-]*)?$/.test(version)) {
      throw new DagsterApiError('Dagster did not return a valid version.', 'INVALID_RESPONSE')
    }
    return version
  }
  async fetchRuns(input: RunsInput = {}): Promise<RunPage> {
    const size = Math.min(200, limit(input.limit, 25))
    const filter =
      input.filter && input.filter !== 'All' ? { statuses: filters[input.filter] } : undefined
    const page = await this.operation<{ results: RawRun[] }>(
      q.runs,
      { filter, cursor: input.cursor, limit: size + 1 },
      'runsOrError',
      'Runs'
    )
    return {
      runs: page.results.slice(0, size).map(runSummary),
      hasMore: page.results.length > size
    }
  }
  async fetchLocationNames(): Promise<string[]> {
    const workspace = await this.operation<{ locationEntries: { name: string }[] }>(
      q.locationNames,
      {},
      'workspaceOrError',
      'Workspace'
    )
    return workspace.locationEntries.map((entry) => entry.name)
  }
  async fetchSnapshot(): Promise<SnapshotData> {
    const data = await this.compatible('Snapshot', q.assetOptions, (fields) =>
      this.request<{
        workspaceOrError: Result & { locationEntries: LocationEntry[] }
        assetNodes: RawAsset[]
        runsOrError: Result & { results: RawRun[] }
        activeRuns?: { count?: number }
      }>(q.snapshot(fields))
    )
    const entries = this.expect(data.workspaceOrError, 'Workspace').locationEntries
    const runs = this.expect(data.runsOrError, 'Runs').results.map(runSummary)
    const latest = data.assetNodes.length
      ? (
          await this.request<{ assetsLatestInfo: Latest[] }>(q.latest, {
            keys: data.assetNodes.map((a) => a.assetKey)
          })
        ).assetsLatestInfo
      : []
    const byKey = new Map(latest.map((a) => [JSON.stringify(a.assetKey.path), a]))
    const snapshot: SnapshotData = {
      locations: [],
      jobs: [],
      schedules: [],
      sensors: [],
      runs,
      assets: data.assetNodes.map((a) =>
        assetSummary(a, byKey.get(JSON.stringify(a.assetKey.path)))
      )
    }
    for (const entry of entries) {
      const location = entry.locationOrLoadError
      snapshot.locations.push({
        name: entry.name,
        loadStatus: entry.loadStatus,
        error:
          location?.__typename === 'PythonError' ? this.redact(formatError(location)) : undefined
      })
      for (const r of location?.repositories ?? []) {
        const repo = { locationName: entry.name, repositoryName: r.name }
        for (const j of r.pipelines)
          if (j.isJob && !j.name.startsWith('__ASSET_JOB'))
            snapshot.jobs.push({ ...repo, name: j.name, description: j.description ?? undefined })
        for (const s of r.schedules)
          snapshot.schedules.push({
            ...instigation(repo, s, s.scheduleState, (t) => this.redact(t)),
            cronSchedule: s.cronSchedule
          })
        for (const s of r.sensors)
          snapshot.sensors.push({
            ...instigation(repo, s, s.sensorState, (t) => this.redact(t)),
            minIntervalSeconds: s.minIntervalSeconds
          })
      }
    }
    if (typeof data.activeRuns?.count === 'number') snapshot.activeRunCount = data.activeRuns.count
    return snapshot
  }
  async fetchAssetPartitions(assetKey: string[], max?: number): Promise<Partitions> {
    const size = Math.min(200, limit(max, 200))
    const key = { path: assetKey }
    const transport = this.options.fetch ?? fetch
    if (!legacyEndpoints.get(transport)?.has(this.endpoint)) {
      try {
        const node = await this.operation<{
          partitionDefinition: { dimensionTypes: { name: string }[] } | null
          partitionKeyConnection: { results: string[] } | null
        }>(q.partitions, { key, limit: size }, 'assetNodeOrError', 'AssetNode')
        const dimensions = node.partitionDefinition?.dimensionTypes ?? []
        if (dimensions.length !== 1) return { kind: dimensions.length ? 'multi' : 'none', keys: [] }
        return {
          kind: 'single',
          dimensionName: dimensions[0].name,
          keys: node.partitionKeyConnection?.results ?? []
        }
      } catch (error) {
        if (!(
          error instanceof DagsterApiError &&
          error.code === 'GRAPHQL_ERROR' &&
          /Cannot query field ['"]partitionKeyConnection['"] on type ['"]AssetNode['"]/.test(
            error.message
          )
        ))
          throw error
        const endpoints = legacyEndpoints.get(transport) ?? new Set<string>()
        endpoints.add(this.endpoint)
        legacyEndpoints.set(transport, endpoints)
      }
    }
    const node = await this.operation<{
      partitionKeysByDimension: { name: string; partitionKeys: string[] }[]
    }>(legacyPartitions, { key }, 'assetNodeOrError', 'AssetNode')
    const dimensions = node.partitionKeysByDimension
    if (dimensions.length !== 1) return { kind: dimensions.length ? 'multi' : 'none', keys: [] }
    return {
      kind: 'single',
      dimensionName: dimensions[0].name,
      keys: dimensions[0].partitionKeys.slice(-size).reverse()
    }
  }
  private async launch(target: Variables, config: RunConfig) {
    const params = {
      selector: target,
      runConfigData: config.runConfigYaml,
      executionMetadata: {
        tags: Object.entries(config.tags ?? {}).map(([key, value]) => ({ key, value }))
      }
    }
    return (
      await this.operation<{ run: { runId: string } }>(
        q.launch,
        { params },
        'launchRun',
        'LaunchRunSuccess'
      )
    ).run
  }
  launchJob(input: LaunchInput) {
    return this.launch({ ...selector(input), pipelineName: input.jobName }, input)
  }
  async materializeAssets(input: MaterializeInput) {
    if (
      !input.assetKeys.length ||
      input.assetKeys.some((path) => !path.length || path.some((part) => !part))
    ) {
      throw new DagsterApiError(
        'Select at least one asset with a complete key.',
        'INVALID_SELECTION'
      )
    }
    const keys = input.assetKeys.map((path) => ({ path }))
    const { assetNodes } = await this.request<{
      assetNodes: (Target & { jobs: { name: string }[]; isMaterializable: boolean })[]
    }>(q.selection, { keys })
    const first = assetNodes[0]
    if (
      !first ||
      !keys.every(
        (key) =>
          assetNodes.filter((n) => JSON.stringify(n.assetKey.path) === JSON.stringify(key.path))
            .length === 1
      )
    ) {
      throw new DagsterApiError(
        'Select assets with an unambiguous definition.',
        'INVALID_SELECTION'
      )
    }
    const repo = repository(first)
    const common = first.jobs.find(
      (j) =>
        j.name.startsWith('__ASSET_JOB') &&
        assetNodes.every(
          (n) =>
            n.isMaterializable &&
            n.repository.name === repo.repositoryName &&
            n.repository.location.name === repo.locationName &&
            n.jobs.some((job) => job.name === j.name)
        )
    )
    if (!common)
      throw new DagsterApiError(
        'Selected assets must share a runnable implicit asset job.',
        'INVALID_SELECTION'
      )
    return this.launch(
      { ...selector(repo), pipelineName: common.name, assetSelection: keys },
      {
        ...input,
        tags:
          input.partitionKey === undefined
            ? input.tags
            : { ...input.tags, 'dagster/partition': input.partitionKey }
      }
    )
  }
  async toggleSchedule(input: ScheduleInput): Promise<State> {
    const vars = { selector: { ...selector(input), scheduleName: input.scheduleName } }
    if (input.running)
      return (
        await this.operation<{ scheduleState: State }>(
          q.startSchedule,
          vars,
          'startSchedule',
          'ScheduleStateResult'
        )
      ).scheduleState
    const { scheduleState } = await this.operation<{ scheduleState: { id: string } }>(
      q.scheduleLookup,
      vars,
      'scheduleOrError',
      'Schedule'
    )
    return (
      await this.operation<{ scheduleState: State }>(
        q.stopSchedule,
        { id: scheduleState.id },
        'stopRunningSchedule',
        'ScheduleStateResult'
      )
    ).scheduleState
  }
  async toggleSensor(input: SensorInput): Promise<State> {
    const vars = { selector: { ...selector(input), sensorName: input.sensorName } }
    if (input.running)
      return (
        await this.operation<{ sensorState: State }>(q.startSensor, vars, 'startSensor', 'Sensor')
      ).sensorState
    const { sensorState } = await this.operation<{ sensorState: { id: string } }>(
      q.sensorLookup,
      vars,
      'sensorOrError',
      'Sensor'
    )
    const stopped = await this.operation<{ instigationState: State | null }>(
      q.stopSensor,
      { id: sensorState.id },
      'stopSensor',
      'StopSensorMutationResult'
    )
    return stopped.instigationState ?? { status: 'STOPPED' }
  }
  async reexecuteRun(runId: string, strategy: RunStrategy) {
    return (
      await this.operation<{ run: { runId: string } }>(
        q.reexecute,
        { params: { parentRunId: runId, strategy } },
        'launchRunReexecution',
        'LaunchRunSuccess'
      )
    ).run
  }
  async terminateRun(runId: string) {
    const { terminateRun } = await this.request<{ terminateRun: Result }>(q.terminate, {
      id: runId
    })
    if (terminateRun.__typename === 'TerminateRunFailure')
      return { ok: false, message: this.redact(formatError(terminateRun)) }
    this.expect(terminateRun, 'TerminateRunSuccess')
    return { ok: true }
  }
  async reloadLocation(name: string): Promise<void> {
    const entry = await this.operation<LocationEntry>(
      q.reload,
      { name },
      'reloadRepositoryLocation',
      'WorkspaceLocationEntry'
    )
    if (entry.locationOrLoadError) this.expect(entry.locationOrLoadError, 'RepositoryLocation')
  }
  async fetchRunDetail(
    runId: string,
    options: { errorsOnly?: boolean; limit?: number } = {}
  ): Promise<RunDetail> {
    const detail = await this.operation<RawDetail>(q.detail, { id: runId }, 'runOrError', 'Run')
    const size = Math.min(maxLogRows, limit(options.limit, 200))
    const transport = this.options.fetch ?? fetch
    let cache = histories.get(transport)
    if (!cache) {
      cache = new Map()
      histories.set(transport, cache)
    }
    const key = createHash('sha256')
      .update(JSON.stringify([this.endpoint, Object.entries(this.headers).sort(), runId]))
      .digest('hex')
    let previous = cache.get(key)
    let history: LogHistory = previous ?? { all: [], errors: [] }
    for (let n = 1; n <= maxFailurePages; n++) {
      const page = await this.operation<LogPage>(
        q.logs,
        { id: runId, cursor: history.cursor, limit: 1000 },
        'logsForRun',
        'EventConnection'
      )
      if (page.hasMore && (!page.cursor || page.cursor === history.cursor))
        throw new DagsterApiError('Run log cursor did not advance', 'INVALID_RESPONSE')
      const rows: LogRow[] = []
      const errors: LogRow[] = []
      let { stepFailure, failure } = history
      const failedChecks = new Map(history.failedChecks?.map((c) => [JSON.stringify(c), c]))
      for (const event of page.events) {
        if (event.evaluation && !event.evaluation.success) {
          const check = {
            assetKey: event.evaluation.assetKey.path,
            name: event.evaluation.checkName
          }
          failedChecks.set(JSON.stringify(check), check)
        }
        const error = event.error ? excerpt(this.redact(formatError(event.error))) : undefined
        const message = excerpt(this.redact(event.message))
        const row = {
          message,
          error,
          timestamp: Number(event.timestamp),
          level: event.level,
          stepKey: event.stepKey ?? undefined
        }
        rows.push(row)
        if (event.__typename === 'ExecutionStepFailureEvent') stepFailure ??= error || message
        if (error || event.level === 'ERROR' || event.level === 'CRITICAL') {
          errors.push(row)
          failure ??= error || message
        }
      }
      history = {
        failedChecks: [...failedChecks.values()],
        all: boundedTail(history.all.concat(rows)),
        errors: boundedTail(history.errors.concat(errors)),
        cursor: page.hasMore || page.events.length ? page.cursor : history.cursor,
        stepFailure,
        failure
      }
      // A concurrent reader may already have advanced this run's cursor.
      if (cache.get(key) === previous) {
        cache.delete(key)
        cache.set(key, history)
        if (cache.size > 8) cache.delete(cache.keys().next().value!)
        previous = history
      }
      // On failure, continue through any backlog until the step traceback is
      // found. Subsequent polls and filter switches resume at the same cursor.
      if (!page.hasMore || (n >= maxPollPages && (detail.status !== 'FAILURE' || stepFailure)))
        break
    }
    const logs = (options.errorsOnly ? history.errors : history.all).slice(-size)
    return {
      ...runSummary(detail),
      failedChecks: history.failedChecks,
      partitionKey: detail.tags?.find((t) => t.key === 'dagster/partition')?.value,
      parentRunId: detail.parentRunId ?? undefined,
      locationName: detail.repositoryOrigin?.repositoryLocationName,
      repositoryName: detail.repositoryOrigin?.repositoryName,
      assetKeys: (detail.assetSelection ?? detail.assets.map((a) => a.key)).map((a: Key) => a.path),
      steps: detail.stepStats.map((s) => ({
        key: s.stepKey,
        attempts: s.attempts?.map((a) => ({
          startTime: milliseconds(a.startTime),
          endTime: milliseconds(a.endTime)
        })),
        status: s.status ?? 'IN_PROGRESS',
        startTime: milliseconds(s.startTime),
        endTime: milliseconds(s.endTime)
      })),
      logs,
      error: history.stepFailure ?? history.failure
    }
  }
}

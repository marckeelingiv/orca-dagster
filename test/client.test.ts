import { test, assert } from 'orca/testing'
import { AuthError, DagsterApiError, DagsterClient } from '../src/dagster/client'

const endpoint = 'https://example.invalid/graphql'
function server(replies: unknown[], headers: Record<string, string> = {}) {
  const calls: RequestInit[] = []
  const client = new DagsterClient(endpoint, headers, {
    fetch: async (_url, init) => {
      if (!init || !replies.length) throw new Error('Unexpected request')
      calls.push(init)
      return Response.json(replies.shift())
    }
  })
  return { client, calls, variables: (n: number) => JSON.parse(String(calls[n].body)).variables }
}
const run = (runId: string) => ({
  runId,
  pipelineName: 'job',
  status: 'SUCCESS',
  startTime: 42,
  endTime: 43,
  creationTime: 41
})
const result = (field: string, type: string, fields: object) => ({
  data: { [field]: { __typename: type, ...fields } }
})
test('counts active runs outside the recent history page and rejects an empty asset selection before launch', async () => {
  const { client, calls } = server([
    {
      data: {
        workspaceOrError: { __typename: 'Workspace', locationEntries: [] },
        assetNodes: [],
        runsOrError: { __typename: 'Runs', results: [run('finished')] },
        activeRuns: { count: 7 }
      }
    }
  ])
  assert.equal((await client.fetchSnapshot()).activeRunCount, 7)
  assert.match(JSON.parse(String(calls[0].body)).query, /activeRuns: runsOrError/)
  await assert.rejects(client.materializeAssets({ assetKeys: [] }), /Select at least one/)
  assert.equal(calls.length, 1)
})
test('reads OSS and Dagster+ GraphQL payloads and disables redirects', async () => {
  for (const headers of [{}, { 'Dagster-Cloud-Api-Token': 'cloud-token' }] as Record<
    string,
    string
  >[]) {
    const { client, calls } = server([{ data: { version: '1.13' } }], headers)
    assert.equal(await client.ping(), '1.13')
    assert.deepEqual(calls[0].headers, { ...headers, 'Content-Type': 'application/json' })
    assert.equal(calls[0].redirect, 'error')
    assert.ok(calls[0].signal)
  }
})
test('paginates and filters runs on the server with an exclusive cursor', async () => {
  const { client, variables } = server([
    result('runsOrError', 'Runs', { results: [run('new'), run('old')] }),
    result('runsOrError', 'Runs', { results: [run('old')] })
  ])
  const page = await client.fetchRuns({ limit: 1, filter: 'Success' })
  assert.equal(page.hasMore, true)
  assert.deepEqual(
    page.runs.map((r) => r.runId),
    ['new']
  )
  assert.equal(page.runs[0].startTime, 42000)
  const second = await client.fetchRuns({ limit: 1, cursor: page.runs[0].runId, filter: 'Success' })
  assert.deepEqual(variables(1), { filter: { statuses: ['SUCCESS'] }, cursor: 'new', limit: 2 })
  assert.equal(second.hasMore, false)
})
test('classifies HTTP 401 and 403 without exposing the response body', async () => {
  for (const status of [401, 403]) {
    const client = new DagsterClient(
      endpoint,
      {},
      { fetch: async () => new Response('sensitive response', { status }) }
    )
    await assert.rejects(
      client.ping(),
      (error) =>
        error instanceof AuthError &&
        error.status === status &&
        !error.message.includes('sensitive')
    )
  }
})
test('preserves Python chains and removes echoed credentials from diagnostics', async () => {
  const token = 'credential-value'
  const encoded = Buffer.from('user:' + token).toString('base64')
  const { client } = server(
    [
      result('launchRun', 'PythonError', {
        message: 'Outer: ' + encoded,
        stack: ['outer.py'],
        errorChain: [{ error: { message: 'inner: ' + token, stack: ['inner.py'] } }]
      }),
      { errors: [{ message: 'GraphQL: ' + token }] }
    ],
    { Authorization: 'Basic ' + encoded }
  )
  await assert.rejects(
    client.launchJob({ locationName: 'loc', repositoryName: 'repo', jobName: 'job' }),
    (error) =>
      error instanceof DagsterApiError &&
      error.code === 'PythonError' &&
      /outer.py[\s\S]*inner.py/.test(error.message) &&
      !error.message.includes(token) &&
      !error.message.includes(encoded)
  )
  await assert.rejects(client.ping(), /GraphQL: \[redacted\]/)
})
test('launches a shared implicit job with asset selection, partition tag and YAML', async () => {
  const assetNodes = ['one', 'two'].map((name) => ({
    assetKey: { path: [name] },
    isMaterializable: true,
    repository: { name: 'repo', location: { name: 'loc' } },
    jobs: [{ name: name + '_job' }, { name: '__ASSET_JOB_1' }]
  }))
  const { client, variables } = server([
    { data: { assetNodes } },
    result('launchRun', 'LaunchRunSuccess', { run: { runId: 'id' } })
  ])
  const yaml = 'ops:\n  x:\n    config: false'
  assert.deepEqual(
    await client.materializeAssets({
      assetKeys: [['one'], ['two']],
      partitionKey: 'today',
      runConfigYaml: yaml,
      tags: { via: 'orca' }
    }),
    { runId: 'id' }
  )
  assert.deepEqual(variables(1).params, {
    selector: {
      repositoryLocationName: 'loc',
      repositoryName: 'repo',
      pipelineName: '__ASSET_JOB_1',
      assetSelection: assetNodes.map((n) => n.assetKey)
    },
    runConfigData: yaml,
    executionMetadata: {
      tags: [
        { key: 'via', value: 'orca' },
        { key: 'dagster/partition', value: 'today' }
      ]
    }
  })
})
test('preserves the failing step cause beyond the visible error log tail', async () => {
  const step = {
    __typename: 'ExecutionStepFailureEvent',
    level: 'ERROR',
    stepKey: 'step',
    timestamp: '45000',
    message: 'Failed',
    error: {
      message: 'wrapper',
      stack: [],
      errorChain: [{ error: { message: 'Real cause', stack: ['asset.py:42'] } }]
    }
  }
  const { client, calls, variables } = server([
    result('runOrError', 'Run', { ...run('id'), assets: [], stepStats: [] }),
    result('logsForRun', 'EventConnection', { events: [step], cursor: 'next', hasMore: true }),
    result('logsForRun', 'EventConnection', {
      events: [{ ...step, __typename: 'RunFailureEvent', message: 'Run failed', error: null }],
      hasMore: false
    })
  ])
  const detail = await client.fetchRunDetail('id', { errorsOnly: true, limit: 1 })
  assert.match(
    JSON.parse(String(calls[1].body)).query,
    /errorChain\s*\{\s*error\s*\{\s*message stack\s*\}\s*\}/
  )
  assert.equal(variables(2).cursor, 'next')
  assert.equal(detail.logs.length, 1)
  assert.equal(detail.logs[0].message, 'Run failed')
  assert.match(detail.error ?? '', /Real cause[\s\S]*asset.py:42/)
})
test('resolves stop mutations using the server-issued state IDs', async () => {
  const { client, variables } = server([
    result('scheduleOrError', 'Schedule', { scheduleState: { id: 's1' } }),
    result('stopRunningSchedule', 'ScheduleStateResult', { scheduleState: { status: 'STOPPED' } }),
    result('sensorOrError', 'Sensor', { sensorState: { id: 's2' } }),
    result('stopSensor', 'StopSensorMutationResult', { instigationState: { status: 'STOPPED' } })
  ])
  const repo = { locationName: 'loc', repositoryName: 'repo', running: false }
  assert.deepEqual(await client.toggleSchedule({ ...repo, scheduleName: 'a' }), {
    status: 'STOPPED'
  })
  assert.deepEqual(await client.toggleSensor({ ...repo, sensorName: 'b' }), { status: 'STOPPED' })
  assert.deepEqual(variables(1), { id: 's1' })
  assert.deepEqual(variables(3), { id: 's2' })
})
test('requests up to 200 partition keys in descending order from the server', async () => {
  const { client, calls, variables } = server([
    result('assetNodeOrError', 'AssetNode', {
      partitionDefinition: { dimensionTypes: [{ name: 'date' }] },
      partitionKeyConnection: { results: ['2026-11-23', '2026-11-22'] }
    })
  ])
  assert.deepEqual(await client.fetchAssetPartitions(['daily'], 2000), {
    kind: 'single',
    dimensionName: 'date',
    keys: ['2026-11-23', '2026-11-22']
  })
  assert.equal(variables(0).limit, 200)
  assert.match(JSON.parse(String(calls[0].body)).query, /ascending: false/)
})
test('snapshot distinguishes stale, failed, and active assets and reports latest tick errors', async () => {
  const repo = { name: 'repo', location: { name: 'loc' } }
  const assets = ['stale', 'failed', 'active'].map((name) => ({
    assetKey: { path: [name] },
    repository: repo,
    description: null,
    groupName: 'main',
    isPartitioned: false,
    jobNames: [],
    staleStatus: 'STALE',
    assetMaterializations: [{ timestamp: '123456789', runId: 'success' }],
    metadataEntries: []
  }))
  const state = {
    status: 'RUNNING',
    ticks: [
      { status: 'FAILURE', timestamp: 42, error: { message: 'secret-tick', stack: ['sensor.py'] } }
    ]
  }
  const { client } = server(
    [
      {
        data: {
          workspaceOrError: {
            __typename: 'Workspace',
            locationEntries: [
              {
                name: 'loc',
                loadStatus: 'LOADED',
                locationOrLoadError: {
                  __typename: 'RepositoryLocation',
                  repositories: [
                    {
                      name: 'repo',
                      pipelines: [],
                      schedules: [],
                      sensors: [{ name: 'sensor', minIntervalSeconds: 5, sensorState: state }]
                    }
                  ]
                }
              }
            ]
          },
          assetNodes: assets,
          runsOrError: { __typename: 'Runs', results: [] }
        }
      },
      {
        data: {
          assetsLatestInfo: assets.map((asset, i) => ({
            assetKey: asset.assetKey,
            latestRun: i ? { runId: 'failed', status: 'FAILURE' } : null,
            inProgressRunIds: i === 2 ? ['running'] : [],
            unstartedRunIds: []
          }))
        }
      }
    ],
    { 'X-Token': 'secret-tick' }
  )
  const snapshot = await client.fetchSnapshot()
  assert.deepEqual(
    snapshot.assets.map((a) => a.status),
    ['warning', 'danger', 'warning']
  )
  assert.equal(snapshot.assets[0].lastMaterializedAt, 123456789)
  assert.deepEqual(snapshot.sensors[0].tick, {
    status: 'FAILURE',
    timestamp: 42000,
    error: '[redacted]\nsensor.py'
  })
})
test('rejects missing, non-string, and non-version probe results', async () => {
  for (const version of [undefined, null, 42, '', 'ok', 'not a dagster version']) {
    await assert.rejects(
      server([{ data: { version } }]).client.ping(),
      (error) => error instanceof DagsterApiError && error.code === 'INVALID_RESPONSE'
    )
  }
})
test('falls back only for an unsupported partitionKeyConnection and remembers the legacy schema', async () => {
  const keys = Array.from({ length: 240 }, (_, i) => String(i))
  const old = result('assetNodeOrError', 'AssetNode', {
    partitionKeysByDimension: [{ name: 'date', partitionKeys: keys }]
  })
  const { client, calls } = server([
    { errors: [{ message: "Cannot query field 'partitionKeyConnection' on type 'AssetNode'." }] },
    old,
    old
  ])
  const page = await client.fetchAssetPartitions(['daily'], 1000)
  assert.equal(page.keys.length, 200)
  assert.equal(page.keys[0], '239')
  assert.equal(page.keys[199], '40')
  assert.deepEqual((await client.fetchAssetPartitions(['daily'], 2)).keys, ['239', '238'])
  for (const init of calls.slice(1))
    assert.match(JSON.parse(String(init.body)).query, /partitionKeysByDimension/)
  const other = server([{ errors: [{ message: 'partitionKeyConnection resolver failed' }] }])
  await assert.rejects(other.client.fetchAssetPartitions(['daily']), /resolver failed/)
  assert.equal(other.calls.length, 1)
})
test('bounds active polls and resumes across client instances and filters to retrieve a late step traceback', async () => {
  let status = 'STARTED'
  const offsets: number[] = []
  const transport: typeof fetch = async (_url, init) => {
    const { query, variables } = JSON.parse(String(init?.body))
    if (query.includes('query RunDetail'))
      return Response.json(
        result('runOrError', 'Run', { ...run('r'), status, assets: [], stepStats: [] })
      )
    const start = Number(variables.cursor ?? 0)
    offsets.push(start)
    const end = Math.min(start + variables.limit, 9100)
    const events = Array.from({ length: end - start }, (_, i) =>
      start + i === 9010
        ? {
            __typename: 'ExecutionStepFailureEvent',
            level: 'ERROR',
            stepKey: 'late',
            timestamp: '500',
            message: 'Step failed',
            error: {
              message: 'wrapper',
              errorChain: [{ error: { message: 'Late cause', stack: ['late.py:15'] } }]
            }
          }
        : {
            __typename: 'LogMessageEvent',
            timestamp: '500',
            level: 'INFO',
            message: 'entry ' + (start + i)
          }
    )
    return Response.json(
      result('logsForRun', 'EventConnection', { events, cursor: String(end), hasMore: end < 9100 })
    )
  }
  const from = (token: string) =>
    new DagsterClient(endpoint, { 'X-Api-Key': token }, { fetch: transport })
  const a = await from('token-A').fetchRunDetail('r', { limit: 1 })
  assert.deepEqual(offsets, [0, 1000, 2000, 3000])
  assert.equal(a.logs[0].message, 'entry 3999')
  status = 'FAILURE'
  const b = await from('token-A').fetchRunDetail('r', { errorsOnly: true, limit: 1 })
  assert.deepEqual(offsets.slice(4), [4000, 5000, 6000, 7000, 8000, 9000])
  assert.equal(b.logs[0].stepKey, 'late')
  assert.match(b.error ?? '', /Late cause[\s\S]*late.py:15/)
  const c = await from('token-A').fetchRunDetail('r', { limit: 2 })
  assert.equal(offsets.at(-1), 9100)
  assert.deepEqual(
    c.logs.map((row) => row.message),
    ['entry 9098', 'entry 9099']
  )
  assert.equal(c.error, b.error)
  status = 'STARTED'
  await from('token-B').fetchRunDetail('r', { limit: 1 })
  assert.deepEqual(offsets.slice(-4), [0, 1000, 2000, 3000])
})

test('partition fallback survives IPC client recreation, isolated per endpoint and transport', async () => {
  const calls: string[] = []
  const transport: typeof fetch = async (url, init) => {
    const current = JSON.parse(String(init?.body)).query.includes('partitionKeyConnection')
    calls.push(String(url) + (current ? '/modern' : '/legacy'))
    return Response.json(
      current
        ? {
            errors: [
              { message: "Cannot query field 'partitionKeyConnection' on type 'AssetNode'." }
            ]
          }
        : result('assetNodeOrError', 'AssetNode', { partitionKeysByDimension: [] })
    )
  }
  for (const url of ['a', 'a', 'b'])
    await new DagsterClient(url, {}, { fetch: transport }).fetchAssetPartitions(['key'])
  assert.deepEqual(calls, ['a/modern', 'a/legacy', 'a/legacy', 'b/modern', 'b/legacy'])
})
test('short passwords do not replace ordinary log characters, and the full Basic header is redacted', () => {
  const header = 'Basic ' + Buffer.from('user:a').toString('base64')
  assert.equal(
    new DagsterClient(endpoint, { Authorization: header }).redact('a data frame ' + header),
    'a data frame [redacted]'
  )
})
test('a failed run without a step failure has a hard page limit and resumes at its saved cursor', async () => {
  const offsets: number[] = []
  const client = new DagsterClient(
    endpoint,
    {},
    {
      fetch: async (_, init) => {
        const { query, variables } = JSON.parse(String(init?.body))
        if (query.includes('query RunDetail'))
          return Response.json(
            result('runOrError', 'Run', {
              ...run('failed'),
              status: 'FAILURE',
              assets: [],
              stepStats: []
            })
          )
        const cursor = Number(variables.cursor ?? 0)
        assert.ok(cursor < 100, 'unbounded log pagination')
        offsets.push(cursor)
        return Response.json(
          result('logsForRun', 'EventConnection', {
            cursor: String(cursor + 1),
            hasMore: true,
            events: []
          })
        )
      }
    }
  )
  await client.fetchRunDetail('failed')
  const initial = offsets.length
  assert.ok(initial > 0 && initial < 100)
  await client.fetchRunDetail('failed')
  assert.equal(offsets[initial], initial)
  assert.equal(offsets.length, initial * 2)
})

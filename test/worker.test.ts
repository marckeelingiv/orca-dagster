import assert from 'node:assert/strict'
import { test } from 'node:test'
import { mockHost, mountWorker } from 'orca/testing'
import { openDagsterPanel } from '../src/open-panel'
import activate, { deactivate } from '../src/worker'
import type { ConnectionProfile } from '../src/types'

const alpha: ConnectionProfile = {
  id: 'alpha',
  name: 'A'.repeat(80),
  kind: 'remote-oss',
  url: 'https://alpha.example',
  auth: { mode: 'none' }
}
const beta = { ...alpha, id: 'beta', name: 'Beta', url: 'https://beta.example' }
const context = (workspaceId: string) => ({
  workspaceId,
  branch: 'main',
  displayName: workspaceId,
  terminals: []
})
function hostForWorker() {
  return mockHost({
    storage: {
      'profiles.user': [alpha, beta],
      'profiles.pins': { first: 'alpha', second: 'beta' }
    },
    results: {
      'workspace.readContext': context('first'),
      'workspace.list': { workspaces: [], truncated: false },
      'statusBar.update': { ok: true },
      'notifications.show': { delivered: true }
    }
  })
}

test('status bar follows run transitions, notifies once and restores the new workspace pin', async (t) => {
  let status = 'STARTED'
  const requests: string[] = []
  t.mock.method(globalThis, 'fetch', async (url: string, init: RequestInit) => {
    requests.push(url)
    const { query } = JSON.parse(String(init.body))
    assert.match(query, /query Snapshot/)
    return Response.json({
      data: {
        workspaceOrError: { __typename: 'Workspace', locationEntries: [] },
        assetNodes: [],
        runsOrError: {
          __typename: 'Runs',
          results: [
            {
              runId: 'r1',
              status,
              pipelineName: 'orders',
              creationTime: 1,
              startTime: 2,
              endTime: null
            }
          ]
        }
      }
    })
  })
  const host = hostForWorker()
  const worker = await mountWorker({ default: activate, deactivate }, { host })
  try {
    const scope = { workspaceId: 'first', profileId: 'alpha' }
    const snapshot = await worker.request('dagster.snapshot', scope)
    assert.equal(snapshot.status, 'connected')
    assert.deepEqual(snapshot, JSON.parse(JSON.stringify(snapshot)))
    const item = host.callsOf('statusBar.update').at(-1) as {
      id: string
      text: string
      tone: string
    }
    assert.equal(item.id, 'dagster-status')
    assert.equal(item.tone, 'accent')
    assert.ok(item.text.length <= 64)
    assert.match(item.text, /1 running$/)
    const posts = worker.posts.length
    await worker.request('dagster.snapshot', scope)
    assert.equal(worker.posts.length, posts)
    status = 'FAILURE'
    await worker.request('dagster.snapshot', scope)
    await worker.request('dagster.snapshot', scope)
    assert.equal(host.callsOf('notifications.show').length, 1)
    assert.match((host.callsOf('statusBar.update').at(-1) as { text: string }).text, /1 failed$/)
    await worker.emit('workspace.changed', { context: context('second') })
    assert.deepEqual(
      worker.posts.filter((e) => e.event === 'dagster.workspaceChanged').at(-1)?.payload,
      { workspaceId: 'second', activeProfileId: 'beta' }
    )
    assert.equal(requests.at(-1), 'https://beta.example/graphql')
    const count = requests.length
    await assert.rejects(
      worker.request('dagster.materialize', { ...scope, assetKeys: [['orders']] }),
      /changed/
    )
    assert.equal(requests.length, count)
    assert.equal(
      host.callsOf('notifications.show').length,
      1,
      'historical failures are not new notifications'
    )
    assert.deepEqual(worker.refusals, [])
  } finally {
    await worker.deactivate()
  }
})

test('palette commands reveal the sidebar, select profiles and reload current locations including failures', async (t) => {
  const reloaded: string[] = []
  t.mock.method(globalThis, 'fetch', async (_url: string, init: RequestInit) => {
    const { query, variables } = JSON.parse(String(init.body))
    const workspaceOrError = {
      __typename: 'Workspace',
      locationEntries: [
        { name: 'a', loadStatus: 'LOADED' },
        { name: 'b', loadStatus: 'LOADED' }
      ]
    }
    if (query.includes('query LocationNames')) return Response.json({ data: { workspaceOrError } })
    if (query.includes('mutation Reload')) {
      reloaded.push(variables.name)
      const locationOrLoadError =
        variables.name === 'b'
          ? { __typename: 'PythonError', message: 'ImportError: missing package', stack: [] }
          : { __typename: 'RepositoryLocation' }
      return Response.json({
        data: {
          reloadRepositoryLocation: { __typename: 'WorkspaceLocationEntry', locationOrLoadError }
        }
      })
    }
    return Response.json({
      data: { workspaceOrError, assetNodes: [], runsOrError: { __typename: 'Runs', results: [] } }
    })
  })
  let revealed = 0
  const worker = await mountWorker(
    {
      default: (orca) =>
        activate(orca, async () => {
          revealed++
        }),
      deactivate
    },
    { host: hostForWorker() }
  )
  try {
    await worker.command('dagster.openPanel')
    await worker.command('dagster.switchProfile')
    assert.equal(revealed, 2)
    assert.ok(worker.posts.some((p) => p.event === 'dagster.showProfiles'))
    assert.ok((await worker.request('profiles.list')).showProfiles)
    assert.ok(!(await worker.request('profiles.list')).showProfiles)
    const summary = await worker.command('dagster.reloadDefinitions')
    assert.deepEqual(reloaded, ['a', 'b'])
    assert.match(String(summary), /a reloaded/)
    assert.match(String(summary), /ImportError/)
    assert.deepEqual(worker.refusals, [])
  } finally {
    await worker.deactivate()
  }
})

test('reveals the enabled Dagster panel by its installed publisher key', async () => {
  const calls: string[][] = []
  await openDagsterPanel(async (args) => {
    calls.push(args)
    return JSON.stringify({
      result: {
        plugins: [
          { pluginKey: 'local.dagster', status: 'disabled' },
          { pluginKey: 'team.dagster', status: 'running' }
        ]
      }
    })
  })
  assert.deepEqual(calls, [['list'], ['open', 'team.dagster', '--panel', 'dagster-sidebar']])
})

test('grid pages and backfill preview reuse the same scoped catalog across IPC requests', async (t) => {
  let grids = 0,
    previews = 0
  const keys = Array.from({ length: 150 }, (_, i) => 'p' + i)
  t.mock.method(globalThis, 'fetch', async (_url: string, init: RequestInit) => {
    const { query, variables } = JSON.parse(String(init.body))
    if (query.includes('PartitionGrid')) {
      grids++
      return Response.json({
        data: {
          assetNodeOrError: {
            __typename: 'AssetNode',
            partitionKeysByDimension: [{ name: 'day', type: 'STATIC', partitionKeys: keys }],
            assetPartitionStatuses: {
              __typename: 'DefaultPartitionStatuses',
              failedPartitions: ['p110'],
              materializedPartitions: [],
              materializingPartitions: []
            }
          }
        }
      })
    }
    if (query.includes('BackfillPreview')) {
      previews++
      assert.deepEqual(variables.params.partitionNames, ['p110'])
      return Response.json({ data: { assetBackfillPreview: [] } })
    }
    return Response.json({
      data: {
        workspaceOrError: { __typename: 'Workspace', locationEntries: [] },
        runsOrError: { __typename: 'Runs', results: [] },
        assetNodes: []
      }
    })
  })
  const worker = await mountWorker({ default: activate, deactivate }, { host: hostForWorker() })
  try {
    const input = { workspaceId: 'first', profileId: 'alpha', assetKey: ['a'] }
    const { token, cursor } = await worker.request('dagster.partitionPage', input)
    assert.ok(token)
    const next = await worker.request('dagster.partitionPage', { ...input, token, cursor })
    assert.equal(next.rows[0].key, 'p48')
    const review = await worker.request('dagster.prepareBackfill', {
      ...input,
      token,
      selection: 'failed'
    })
    assert.deepEqual(review.keys, ['p110'])
    assert.equal(grids, 1)
    assert.equal(previews, 1)
    await assert.rejects(
      worker.request('dagster.partitionPage', { ...input, token, assetKey: ['b'] }),
      /expired/
    )
    await worker.emit('workspace.changed', { context: context('second') })
    await assert.rejects(
      worker.request('dagster.partitionPage', {
        workspaceId: 'second',
        profileId: 'beta',
        assetKey: ['a'],
        token
      }),
      /expired/
    )
  } finally {
    await worker.deactivate()
  }
})

test('a late successful snapshot after a workspace switch cannot reattach its watcher or fail the panel request', async (t) => {
  let release!: () => void, started!: () => void
  const start = new Promise<void>((r) => {
    started = r
  })
  const unblock = new Promise<void>((r) => {
    release = r
  })
  t.mock.method(globalThis, 'fetch', async (url: string) => {
    if (url.startsWith(alpha.url)) {
      started()
      await unblock
    }
    return Response.json({
      data: {
        workspaceOrError: { __typename: 'Workspace', locationEntries: [] },
        assetNodes: [],
        runsOrError: { __typename: 'Runs', results: [] }
      }
    })
  })
  const worker = await mountWorker({ default: activate, deactivate }, { host: hostForWorker() })
  try {
    const old = worker.request('dagster.snapshot', { workspaceId: 'first', profileId: 'alpha' })
    await start
    await worker.emit('workspace.changed', { context: context('second') })
    release()
    assert.equal((await old).status, 'connected')
    assert.equal((await worker.request('profiles.list', {})).activeProfileId, 'beta')
  } finally {
    release()
    await worker.deactivate()
  }
})

import * as main from '../src/worker'
import { mountWorker, mockHost } from 'orca/testing'
import assert from 'node:assert/strict'
import { mkdtemp, mkdir, writeFile, rm, readFile, realpath } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, dirname, basename } from 'node:path'
import { execFileSync } from 'node:child_process'
import { setTimeout as delay } from 'node:timers/promises'
import { test } from 'node:test'
import { changedPaths, sourceIndex } from '../src/source/index'
import { partitionPage } from '../src/dagster/partition-status'
import { ReloadWatcher } from '../src/local/reload-watcher'
import { Explorer } from '../src/dagster/explorer'
import { data, profile } from '../src/panel/fixtures'
import { Session } from '../src/session'
import type { OrcaWorkerApi } from 'orca/worker'
import type { GraphNode } from '../src/types'

test('source index maps shared Python definitions and dbt SQL alongside repository code references', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'asset-index-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  execFileSync('git', ['init', '-q', root])
  await mkdir(join(root, 'models'))
  await writeFile(join(root, 'dbt_project.yml'), 'name: fixture')
  await writeFile(join(root, 'assets.py'), '@asset\ndef one(): pass\n@asset\ndef two(): pass')
  await writeFile(join(root, 'models/orders.sql'), 'select 1')
  await writeFile(join(root, 'unmapped.py'), '')
  const nodes: GraphNode[] = ['one', 'two', 'orders'].map((name) => ({
    ...data.assets[0],
    assetKey: [name],
    keyString: name,
    dependencyKeys: [],
    codeReferences: [{ filePath: 'assets.py' }],
    dbtUniqueId: name === 'orders' ? 'model.shop.orders' : undefined
  }))
  const index = await sourceIndex(root, profile, nodes, new AbortController().signal)
  assert.deepEqual(index.changed, ['assets.py', 'models/orders.sql', 'unmapped.py'])
  assert.deepEqual(index.files, [
    { path: 'assets.py', assetIds: ['one', 'two', 'orders'] },
    { path: 'models/orders.sql', assetIds: ['orders'] }
  ])
  assert.deepEqual(changedPaths('R  new name.py\0old name.py\0 D deleted.sql\0?? unrelated.md\0'), [
    'deleted.sql',
    'new name.py',
    'old name.py'
  ])
})

test('multidimensional status pages preserve dimension ordering and restrict the requested slice', () => {
  const catalog = {
    partitionKeysByDimension: [
      { name: 'region', type: 'STATIC', partitionKeys: ['west', 'east'] },
      { name: 'day', type: 'TIME_WINDOW', partitionKeys: ['a', 'b', 'c'] }
    ],
    assetPartitionStatuses: {
      __typename: 'MultiPartitionStatuses' as const,
      primaryDimensionName: 'day',
      ranges: [
        {
          primaryDimStartKey: 'b',
          primaryDimEndKey: 'c',
          secondaryDim: {
            __typename: 'DefaultPartitionStatuses' as const,
            materializedPartitions: ['east'],
            failedPartitions: ['west'],
            materializingPartitions: []
          }
        }
      ]
    }
  }
  const page = partitionPage(catalog, undefined, { region: 'west' })
  assert.deepEqual(
    page.rows.map((r) => [r.key, r.status]),
    [
      ['a|west', 'missing'],
      ['b|west', 'failed'],
      ['c|west', 'failed']
    ]
  )
  assert.equal(partitionPage(catalog, 'b', { region: 'east' }).rows[0].status, 'success')
  assert.throws(() => partitionPage(catalog, 'removed'))
})

test('bound source worktree takes precedence and a missing or paired checkout requires rebinding', async (t) => {
  const path = await mkdtemp(join(tmpdir(), 'source-binding-'))
  t.after(() => rm(path, { recursive: true, force: true }))
  const workspaces = [
    { id: 'a', path, hostId: 'local' },
    { id: 'b', path, hostId: 'local' }
  ]
  const session = new Session({
    host: {
      call: async (name: string) =>
        name === 'workspace.list' ? { workspaces } : { workspaceId: 'a' }
    }
  } as unknown as OrcaWorkerApi)
  const cn = {
    profile: { ...profile, sourceWorkspaceId: 'b' },
    workspace: { id: 'a', path },
    revision: 0
  }
  assert.deepEqual(await session.sourceWorkspace(cn), { id: 'b', path })
  workspaces[1].hostId = 'ssh:other'
  await assert.rejects(session.sourceWorkspace(cn), /Rebind/)
  workspaces.pop()
  await assert.rejects(session.sourceWorkspace(cn), /Rebind/)
})

test('reload watcher coalesces writes and detaches on stop', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'reload-watch-')),
    watcher = new ReloadWatcher()
  t.after(async () => {
    watcher.stop()
    await rm(root, { recursive: true, force: true })
  })
  let calls = 0
  let finish!: () => void, fail!: (e: unknown) => void
  const ready = new Promise<void>((resolve, reject) => {
    finish = resolve
    fail = reject
  })
  watcher.configure(
    'profile',
    root,
    async () => {
      calls++
      finish()
    },
    fail
  )
  await writeFile(join(root, 'a.py'), '1')
  await writeFile(join(root, 'b.sql'), '2')
  await Promise.race([
    ready,
    delay(3000).then(() => {
      if (!calls) throw Error('No reload')
    })
  ])
  watcher.stop()
  await writeFile(join(root, 'a.py'), '3')
  await delay(500)
  assert.equal(calls, 1)
})

test(
  'live explorer previews, launches and tracks a partition selection',
  { skip: process.env.DAGSTER_E2E !== '1', timeout: 240000 },
  async () => {
    const c = new Explorer(
      (process.env.DAGSTER_E2E_LOCAL_URL ?? 'http://localhost:3000') + '/graphql',
      {}
    )
    const graph = await c.graph(),
      cleaned = graph.find((n) => n.assetKey[0] === 'cleaned_orders')!
    assert.ok(
      cleaned.dependencyKeys.includes(graph.find((n) => n.assetKey[0] === 'raw_orders')!.keyString)
    )
    assert.ok((await c.health()).some((h) => h.assetKey[0] === 'raw_orders'))
    const selection = { assetKey: ['daily_revenue'], keys: ['2026-11-05'] }
    const grid = await c.partitionPage(selection.assetKey)
    assert.ok(grid.rows.some((r) => r.key === selection.keys[0]))
    // Materialize the fixture's unpartitioned inputs before launching a partition in isolation.
    const { runId } = await c.materializeAssets({ assetKeys: [['raw_orders'], ['cleaned_orders']] })
    let upstream = await c.fetchRunDetail(runId)
    while (!['FAILURE', 'SUCCESS', 'CANCELED'].includes(upstream.status)) {
      await delay(1000)
      upstream = await c.fetchRunDetail(runId)
    }
    assert.equal(upstream.status, 'SUCCESS')
    const targets = await c.preview(selection)
    assert.deepEqual(targets[0].assetKey.path, selection.assetKey)
    const { backfillId } = await c.launchBackfill(selection)
    let status = await c.backfillStatus(backfillId)
    while (['REQUESTED', 'CANCELING'].includes(status.status)) {
      await delay(1000)
      status = await c.backfillStatus(backfillId)
    }
    assert.equal(status.status, 'COMPLETED_SUCCESS', status.progress)
    assert.match(status.progress, /1\/1 materialized/)
    assert.ok(
      (await c.columns(selection.assetKey, selection.keys[0])).columns?.some(
        (col) => col.name === 'total_revenue'
      )
    )
    const flow = await c.columnFlow(selection.assetKey, 'total_revenue', selection.keys[0])
    assert.ok(flow.some((r) => r.id === cleaned.keyString && r.column === 'amount'))
    assert.ok(
      flow.some((r) => r.id === graph.find((n) => n.assetKey[0] === 'raw_orders')!.keyString)
    )
  }
)

test(
  'local source writes expose a reload traceback and recover the code location',
  {
    skip: !process.env.DAGSTER_E2E_RELOAD_ROOT || process.env.DAGSTER_E2E === '1',
    timeout: 120000
  },
  async () => {
    // The reloadable Compose code-server must mount this disposable copy at /app/src.
    const root = await realpath(process.env.DAGSTER_E2E_RELOAD_ROOT!)
    assert.equal(dirname(root), await realpath(tmpdir()))
    assert.ok(basename(root).startsWith('astra-roadmap-reload-'))
    const workspaceId = 'reload-fixture'
    const url = process.env.DAGSTER_E2E_LOCAL_URL ?? 'http://localhost:3000'
    const host = mockHost({
      settings: { defaultAgent: 'codex' },
      results: {
        'workspace.readContext': {
          workspaceId,
          branch: 'fixture',
          displayName: 'Fixture',
          terminals: []
        },
        'workspace.list': {
          workspaces: [
            {
              id: workspaceId,
              kind: 'folder',
              name: 'Fixture',
              path: root,
              hostId: 'local',
              branch: null
            }
          ],
          truncated: false
        },
        'terminal.list': { terminals: [], truncated: false },
        'secrets.protection': { status: 'os-keyring' },
        'notifications.show': { delivered: true },
        'statusBar.update': { ok: true },
        'agent.launch': { terminalId: 'fixture-agent' }
      }
    })
    const worker = await mountWorker(main, { host }),
      scope = { workspaceId, profileId: 'reload' }
    const file = join(root, 'src/sample_project/definitions.py'),
      original = await readFile(file, 'utf8')
    const client = new Explorer(url + '/graphql', {})
    async function waitForLocation(broken: boolean) {
      const end = Date.now() + 45000
      while (Date.now() < end) {
        const location = (await client.fetchSnapshot()).locations.find(
          (l) => l.name === 'sample_project'
        )
        if (
          location &&
          (broken ? location.error?.includes('Orca reload fixture') : !location.error)
        )
          return
        await delay(400)
      }
      throw Error('Timed out waiting for ' + (broken ? 'load failure' : 'recovery'))
    }
    try {
      await worker.request('profiles.save', {
        workspaceId,
        profile: {
          id: 'reload',
          kind: 'local',
          name: 'Reload',
          url,
          auth: { mode: 'none' },
          sourceWorkspaceId: workspaceId,
          reloadLocationOnSave: 'sample_project'
        }
      })
      await worker.request('dagster.snapshot', scope)
      try {
        await writeFile(file, original + '\nraise RuntimeError("Orca reload fixture")\n')
        await waitForLocation(true)
        const result = await worker.request('agent.loadError', {
          ...scope,
          locationName: 'sample_project'
        })
        assert.ok(result.launched)
        assert.match(
          (host.callsOf('agent.launch')[0] as { prompt: string }).prompt,
          /Orca reload fixture/
        )
      } finally {
        await writeFile(file, original)
      }
      await waitForLocation(false)
      async function waitRun(id: string) {
        const deadline = Date.now() + 60000
        while (Date.now() < deadline) {
          const run = await client.fetchRunDetail(id)
          if (['SUCCESS', 'FAILURE', 'CANCELED'].includes(run.status)) return run
          await delay(500)
        }
        throw Error('Timed out waiting for verification run ' + id)
      }
      const assetFile = join(root, 'src/sample_project/assets.py'),
        source = await readFile(assetFile, 'utf8')
      const failed = await worker.request('dagster.materialize', {
        ...scope,
        assetKeys: [['flaky_enrichment']]
      })
      assert.equal((await waitRun(failed.runId)).status, 'FAILURE')
      const sent = await worker.request('agent.sendRunError', {
        ...scope,
        runId: failed.runId,
        mode: 'launch'
      })
      assert.equal(sent.terminalId, 'fixture-agent')
      const marker = 'Reload verification fixture'
      try {
        await writeFile(
          assetFile,
          source
            .replace('if config.should_fail:', 'if config.should_fail and False:')
            .replace('Intentionally fails by default', marker)
        )
        const deadline = Date.now() + 30000
        while (
          !(await client.fetchSnapshot()).assets.some((a) => a.description?.startsWith(marker))
        ) {
          if (Date.now() > deadline) throw Error('Updated definition did not load')
          await delay(500)
        }
        const { run } = await worker.request('dagster.runDetail', { ...scope, runId: failed.runId })
        assert.equal(run.agentFix, 'edited')
        const retry = await worker.request('dagster.reexecuteRun', {
          ...scope,
          runId: failed.runId,
          strategy: 'FROM_FAILURE'
        })
        const verified = await waitRun(retry.runId)
        assert.equal(verified.status, 'SUCCESS', verified.error)
        assert.equal(verified.parentRunId, failed.runId)
      } finally {
        await writeFile(assetFile, source)
        await client.reloadLocation('sample_project')
      }
    } finally {
      await worker.deactivate()
    }
  }
)

test('reload watcher skips dependency trees and discovers new source subdirectories', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'selective-watch-'))
  const watcher = new ReloadWatcher()
  t.after(async () => {
    watcher.stop()
    await rm(root, { recursive: true, force: true })
  })
  const ignored = ['.git', '.venv', 'node_modules', 'target', 'dbt_packages', '__pycache__']
  for (const dir of ignored) await mkdir(join(root, dir, 'nested'), { recursive: true })
  let calls = 0
  let resolveReload!: () => void
  const reload = new Promise<void>((resolve) => {
    resolveReload = resolve
  })
  await watcher.configure(
    'workspace',
    root,
    async () => {
      calls++
      resolveReload()
    },
    (e) => assert.fail(String(e))
  )
  for (const dir of ignored) await writeFile(join(root, dir, 'nested', 'a.py'), '1')
  await delay(550)
  assert.equal(calls, 0)
  await mkdir(join(root, 'new', 'models'), { recursive: true })
  await writeFile(join(root, 'new', 'models', 'orders.sql'), 'select 1')
  await Promise.race([
    reload,
    delay(2500).then(() => {
      if (!calls) throw Error('New directory was not watched')
    })
  ])
  assert.equal(calls, 1)
})

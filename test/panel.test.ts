import {
  afterEach,
  assert,
  mockHost,
  mount,
  test,
  type MockRequests,
  type TestPanel
} from 'orca/testing'
import * as panel from '../src/panel'
import { keyOf } from '../src/panel/selection'
import { data, list, requests, run } from '../src/panel/fixtures'
import type { ConnectionProfile, LaunchInput } from '../src/types'

let view: TestPanel | undefined
afterEach(async () => {
  await view?.unmount()
  view = undefined
})
async function start(overrides: MockRequests = {}) {
  const host = mockHost({ requests: { ...requests, ...overrides } })
  view = await mount(panel, { host })
  return view
}

test('duplicates an encrypted profile using its key reference and saves under a new ID', async () => {
  const original: ConnectionProfile = {
    ...list.profiles[0],
    auth: { mode: 'bearer', source: 'orca-secret' }
  }
  const saved: ConnectionProfile[] = []
  const ui = await start({
    'profiles.list': { ...list, profiles: [original] },
    'profiles.save': (input) => {
      saved.push(input.profile)
      assert.equal(input.duplicateFrom, original.id)
      assert.ok(!input.secretValue)
      return { profile: input.profile }
    }
  })
  await ui.press({ text: 'Profiles', type: 'Button' })
  await ui.press({ text: 'Duplicate' })
  await ui.press({ text: 'Save Profile' })
  assert.equal(saved[0].id, 'copy-of-local-docker')
  assert.equal(saved[0].auth.secretKey, 'profile:local:token')
  assert.equal(saved[0].url, original.url)
})

test('starts a new default local profile directly from the empty sidebar', async () => {
  let saved: ConnectionProfile | undefined
  const starts: unknown[] = []
  const ui = await start({
    'profiles.list': () => ({
      ...list,
      profiles: saved ? [saved] : [],
      activeProfileId: saved?.id ?? null
    }),
    'profiles.save': (input) => {
      saved = input.profile
      return { profile: input.profile }
    },
    'local.start': (scope) => {
      starts.push(scope)
      return { state: { status: 'starting' } }
    }
  })
  await ui.press({ text: 'Start Local Dagster' })
  assert.deepEqual(starts, [{ workspaceId: list.workspaceId, profileId: 'local' }])
})

test('passes the selected job’s optional YAML to the launch request', async () => {
  const launches: LaunchInput[] = []
  const ui = await start({
    'dagster.launchJob': (input) => {
      launches.push(input)
      return { runId: 'run' }
    }
  })
  await ui.select({ label: 'Dagster views' }, 'automation')
  await ui.press({ text: run.pipelineName, type: 'Button' })
  await ui.toggle({ text: 'Job config', type: 'Collapsible' })
  await ui.type({ label: 'Job run config YAML or JSON' }, 'ops: {}')
  await ui.press({ text: 'Launch', type: 'Button' })
  assert.equal(launches[0].runConfigYaml, 'ops: {}')
})

test('asset selection opens scoped source and materializes the latest partition', async () => {
  const calls: unknown[] = []
  const ui = await start({
    'source.open': (input) => {
      calls.push(input)
      return { opened: true, resolvedPath: 'src/assets.py' }
    },
    'dagster.materialize': (input) => {
      calls.push(input)
      return { runId: 'new' }
    }
  })
  await ui.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  assert.ok(ui.find({ text: 'src/assets.py' }))
  assert.equal(ui.valueOf({ label: 'Partition' }), '2026-11-24')
  await ui.press({ text: 'Materialize' })
  assert.deepEqual(calls, [
    {
      profileId: 'local',
      workspaceId: 'sample',
      kind: 'asset',
      name: 'daily_revenue',
      assetKey: ['daily_revenue'],
      locationName: 'sample_project',
      repositoryName: '__repository__',
      codeReferences: []
    },
    {
      profileId: 'local',
      workspaceId: 'sample',
      assetKeys: [['daily_revenue']],
      partitionKey: '2026-11-24'
    }
  ])
  assert.deepEqual(ui.errors, [])
})
test('automation, run pagination, failed detail and agent handoff', async () => {
  const sent: unknown[] = []
  const ui = await start({
    'dagster.runs': (input) => ({
      runs: input.cursor ? [{ ...run, runId: 'older' }] : [run],
      hasMore: !input.cursor
    }),
    'agent.sendRunError': (input) => {
      sent.push(input)
      return { launched: true }
    }
  })
  await ui.select({ label: 'Dagster views' }, 'automation')
  for (const title of ['Jobs', 'Schedules', 'Sensors'])
    assert.ok(ui.find({ type: 'List', label: title }))
  await ui.select({ label: 'Dagster views' }, 'runs')
  await ui.loadMore({ label: 'Runs' })
  assert.deepEqual(ui.rowsOf({ label: 'Runs' }), [run.runId, 'older'])
  await ui.activateRow({ label: 'Runs' }, run.runId)
  await ui.press({ text: 'Send Error to Agent' })
  assert.deepEqual(sent, [
    {
      profileId: 'local',
      workspaceId: 'sample',
      runId: run.runId,
      mode: 'launch'
    }
  ])
  await ui.press({ text: 'Back to Runs' })
  assert.ok(ui.find({ label: 'Runs', type: 'Table' }))
})
test('command opens profiles and credential source controls exchange cleanly', async () => {
  const ui = await start()
  await ui.post('dagster.showProfiles', {})
  await ui.press({ text: 'New' })
  await ui.select({ label: 'Auth mode' }, 'bearer')
  assert.ok(ui.find({ label: 'Environment variable' }))
  await ui.select({ label: 'Credential source' }, 'orca-secret')
  assert.ok(ui.find({ label: 'Secret' }))
  assert.equal(ui.find({ label: 'Environment variable' }), null)
  await ui.select({ label: 'Credential source' }, 'env-var')
  assert.equal(ui.find({ label: 'Secret' }), null)
})
test('bounds large asset collections and filters before limiting rows', async () => {
  const ui = await start({
    'dagster.snapshot': {
      ...data,
      assets: Array.from({ length: 2000 }, (_, i) => ({
        ...data.assets[0],
        assetKey: ['asset-' + i]
      }))
    }
  })
  await ui.type({ label: 'Search assets' }, 'asset-1999')
  assert.match(JSON.stringify(ui.tree()), /asset-1999/)
  assert.deepEqual(ui.warnings, [])
  assert.deepEqual(ui.errors, [])
})
test('hidden panels skip snapshots, worker events refresh after showing', async () => {
  let count = 0
  const host = mockHost({
    requests: {
      ...requests,
      'dagster.snapshot': () => {
        count++
        return data
      }
    }
  })
  view = await mount(panel, { host, visible: false })
  assert.equal(count, 0)
  await view.post('dagster.stateChanged', {
    status: 'connected',
    activeProfileId: 'local',
    activeRunCount: 0,
    syncedAt: 1
  })
  assert.equal(count, 0)
  await view.setVisible(true)
  await view.post('dagster.stateChanged', {
    status: 'connected',
    activeProfileId: 'local',
    activeRunCount: 0,
    syncedAt: 2
  })
  assert.equal(count, 1)
})

test('late source results from the previous workspace never update the new selection', async () => {
  let workspaceId = 'sample'
  let resolveOld!: (result: { opened: boolean; resolvedPath: string }) => void
  let started!: () => void
  const opening = new Promise<void>((resolve) => {
    started = resolve
  })
  const ui = await start({
    'profiles.list': () => ({ ...list, workspaceId }),
    'source.open': (input) => {
      if (input.workspaceId === 'next') return { opened: true, resolvedPath: 'new.py' }
      started()
      return new Promise((resolve) => {
        resolveOld = resolve
      })
    }
  })
  const old = ui.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  await opening
  workspaceId = 'next'
  const switching = ui.post('dagster.workspaceChanged', {
    workspaceId,
    activeProfileId: 'local'
  })
  resolveOld({ opened: true, resolvedPath: 'old.py' })
  await Promise.all([old, switching])
  assert.equal(ui.find({ text: 'old.py' }), null)
  await ui.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  assert.ok(ui.find({ text: 'new.py' }))
})
test('poll interval follows active runs, skips hidden work and releases the timer on unmount', async () => {
  let count = 0
  const host = mockHost({
    settings: {
      pollIntervalActiveSec: 1,
      pollIntervalIdleSec: 5,
      maxRunLogLines: 75
    },
    requests: {
      ...requests,
      'dagster.snapshot': () => {
        count++
        return { ...data, runs: [{ ...run, status: 'STARTED' }] }
      }
    }
  })
  view = await mount(panel, { host })
  const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))
  await wait(1200)
  assert.equal(count, 2)
  await view.setVisible(false)
  await wait(1200)
  assert.equal(count, 2)
  await view.setVisible(true)
  await wait(500)
  assert.equal(count, 3)
  await view.unmount()
  view = undefined
  await wait(1100)
  assert.equal(count, 3)
})

test('profile selection reloads the snapshot and scopes subsequent controls', async () => {
  let activeProfileId = 'local'
  const selected: unknown[] = []
  const snapshots: unknown[] = []
  const ui = await start({
    'profiles.list': () => ({
      ...list,
      activeProfileId,
      profiles: [...list.profiles, { ...list.profiles[0], id: 'remote', kind: 'remote-oss' }]
    }),
    'profiles.select': (input) => {
      selected.push(input)
      activeProfileId = input.profileId
      return { activeProfileId }
    },
    'dagster.snapshot': (input) => {
      snapshots.push(input)
      return data
    }
  })
  await ui.select({ label: 'Connection profile' }, 'remote')
  assert.deepEqual(selected, [{ profileId: 'remote', workspaceId: 'sample' }])
  assert.deepEqual(snapshots.at(-1), {
    profileId: 'remote',
    workspaceId: 'sample'
  })
  assert.equal(ui.find({ label: 'Local controls' }), null)
})

test('edited credentials are submitted only for the chosen source and never persisted in panel preferences', async () => {
  const tested: unknown[] = []
  const ui = await start({
    'profiles.test': (input) => {
      tested.push(input)
      return { ok: true, version: '1.10', latencyMs: 4 }
    }
  })
  await ui.press({ text: 'Profiles', type: 'Button' })
  await ui.select({ label: 'Auth mode' }, 'bearer')
  await ui.select({ label: 'Credential source' }, 'orca-secret')
  await ui.type({ label: 'Secret' }, 'fixture-token')
  await ui.press({ text: 'Test Connection' })
  assert.match(JSON.stringify(tested[0]), /fixture-token/)
  assert.ok(
    ui.find({ label: /Connected · Dagster/ }) ||
      JSON.stringify(ui.tree()).includes('Connected · Dagster')
  )
  await ui.select({ label: 'Credential source' }, 'env-var')
  await ui.type({ label: 'Environment variable' }, 'DAGSTER_TOKEN')
  await ui.press({ text: 'Test Connection' })
  assert.ok(!JSON.stringify([tested[1], ui.tree(), ui.persisted]).includes('fixture-token'))
})

test('external links run only on a press and use the expanded deployment path', async () => {
  const host = mockHost({
    requests: {
      ...requests,
      'source.open': { opened: false },
      'dagster.snapshot': {
        ...data,
        uiUrl: 'https://example.test/deployment/graphql'
      }
    }
  })
  view = await mount(panel, { host })
  await view.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  assert.deepEqual(host.callsOf('shell.openExternal'), [])
  await view.press({ text: 'Open in Dagster UI' })
  assert.deepEqual(host.callsOf('shell.openExternal'), [
    { url: 'https://example.test/deployment/assets/daily_revenue' }
  ])
  assert.deepEqual(view.refusals, [])
})

test('searches asset groups and descriptions and shows status and materialization age for each leaf', async () => {
  const asset = {
    ...data.assets[0],
    description: 'Accounting ledger',
    status: 'danger' as const,
    lastMaterializedAt: data.syncedAt - 120000
  }
  const ui = await start({
    'dagster.snapshot': {
      ...data,
      assets: [
        asset,
        {
          ...asset,
          assetKey: ['other'],
          description: 'Other',
          groupName: 'other'
        }
      ]
    }
  })
  for (const q of ['COMMERCE', 'ledger', 'daily_revenue']) {
    await ui.type({ label: 'Search assets' }, q, { replace: true })
    const items = JSON.stringify(ui.find({ type: 'Tree' })?.props.items)
    assert.match(items, /daily_revenue/)
    assert.match(items, /Failed · Materialized 2m ago/)
    assert.match(items, /"icon":"error"/)
    assert.ok(!items.includes('other'))
  }
})

test('materializes compatible assets with a shared partition and reports the returned run', async () => {
  const base = { ...data.assets[0], jobNames: ['__ASSET_JOB'] }
  const assets = [
    base,
    { ...base, assetKey: ['second'] },
    { ...base, assetKey: ['elsewhere'], repositoryName: 'other' },
    { ...base, assetKey: ['hourly'] },
    { ...base, assetKey: ['multi'] }
  ]
  const launches: unknown[] = []
  const ui = await start({
    'dagster.snapshot': { ...data, assets },
    'source.open': { opened: false },
    'dagster.assetPartitions': ({ assetKey }) => ({
      kind: assetKey[0] === 'multi' ? 'multi' : 'single',
      dimensionName: assetKey[0] === 'hourly' ? 'hour' : 'day',
      keys: assetKey[0] === 'second' ? ['today', 'shared'] : ['yesterday', 'shared']
    }),
    'dagster.materialize': (input) => {
      launches.push(input)
      return { runId: 'batch-run' }
    }
  })
  await ui.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  await ui.press({ text: 'Keep in selection' })
  for (const key of assets.slice(2).map(keyOf)) {
    await ui.selectItem({ label: 'Assets' }, key)
    assert.ok(ui.find({ text: 'Materialize', type: 'Button' })?.disabled)
  }
  await ui.selectItem({ label: 'Assets' }, keyOf(assets[1]))
  assert.equal(ui.valueOf({ label: 'Partition' }), 'shared')
  await ui.press({ text: 'Materialize' })
  assert.deepEqual(launches, [
    {
      workspaceId: 'sample',
      profileId: 'local',
      assetKeys: [base.assetKey, ['second']],
      partitionKey: 'shared'
    }
  ])
  assert.ok(ui.find({ text: 'View run batch-run' }))
})

test('remote asset and automation sources display the resolver notice and open its Git link on press', async () => {
  const url = 'https://example.org/org/repo/blob/main/assets.py#L43'
  const host = mockHost({
    requests: {
      ...requests,
      'source.open': {
        opened: false,
        reason: 'Source lives in another repository.',
        externalUrl: url
      },
      'dagster.snapshot': {
        ...data,
        schedules: [
          {
            ...data.schedules[0],
            tick: { status: 'SKIPPED', timestamp: data.syncedAt - 30000 }
          }
        ]
      }
    }
  })
  view = await mount(panel, { host })
  await view.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  assert.ok(view.find({ text: 'Source lives in another repository.' }))
  assert.equal(host.callsOf('shell.openExternal').length, 0)
  await view.press({ text: 'Open remote definition' })
  await view.select({ label: 'Dagster views' }, 'automation')
  await view.press({ text: 'daily_orders_schedule', type: 'Button' })
  assert.ok(view.find({ text: /Last tick SKIPPED/ }))
  assert.ok(
    view.findAll({ type: 'RelativeTime' }).some((t) => t.props.at === data.syncedAt - 30000)
  )
  await view.press({ text: 'Open remote definition' })
  assert.deepEqual(host.callsOf('shell.openExternal'), [{ url }, { url }])
})

test('local source includes line numbers on assets and jobs and Launch reports the run ID', async () => {
  const ui = await start({
    'source.open': {
      opened: true,
      resolvedPath: 'src/definitions.py',
      lineNumber: 43
    },
    'dagster.launchJob': { runId: 'job-run' }
  })
  await ui.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  assert.ok(ui.find({ text: 'src/definitions.py:43' }))
  await ui.select({ label: 'Dagster views' }, 'automation')
  await ui.press({ text: run.pipelineName, type: 'Button' })
  assert.ok(ui.find({ text: 'src/definitions.py:43' }))
  await ui.press({ text: 'Launch' })
  assert.ok(ui.find({ text: 'View run job-run' }))
})

test('remote profile edits preserve and update source path mappings', async () => {
  const profile = {
    ...list.profiles[0],
    kind: 'remote-oss' as const,
    local: { pathPrefixMap: { '/app/src': 'repo/src' } }
  }
  const saved: unknown[] = []
  const ui = await start({
    'profiles.list': { ...list, profiles: [profile] },
    'profiles.test': (input) => {
      saved.push(input.profile.local)
      return { ok: true, latencyMs: 1 }
    },
    'profiles.save': (input) => {
      saved.push(input.profile.local)
      return { profile: input.profile }
    }
  })
  await ui.press({ text: 'Profiles', type: 'Button' })
  assert.equal(ui.valueOf({ label: 'Path prefix mappings' }), '/app/src=repo/src')
  await ui.press({ text: 'Test Connection' })
  await ui.type({ label: 'Path prefix mappings' }, '/app/src=changed/src', {
    replace: true
  })
  await ui.press({ text: 'Save Profile' })
  assert.deepEqual(saved, [profile.local, { pathPrefixMap: { '/app/src': 'changed/src' } }])
})

test('poll refresh keeps the loaded run window live and resumes failed detail promptly', async () => {
  let refreshed = false,
    detailCalls = 0
  const rows = Array.from({ length: 80 }, (_, i) => ({
    ...run,
    runId: String(i)
  }))
  const ui = await start({
    'dagster.runs': ({ limit = 30, cursor }) => {
      const start = cursor === undefined ? 0 : Number(cursor) + 1
      return {
        runs: rows
          .slice(start, start + limit)
          .map((r) => ({ ...r, status: refreshed ? 'SUCCESS' : 'FAILURE' })),
        hasMore: start + limit < rows.length
      }
    },
    'dagster.runDetail': ({ runId }) => {
      detailCalls++
      return { run: { ...run, runId } }
    }
  })
  await ui.select({ label: 'Dagster views' }, 'runs')
  await ui.loadMore({ label: 'Runs' })
  assert.equal(ui.rowsOf({ label: 'Runs' }).length, 60)
  refreshed = true
  await ui.post('dagster.stateChanged', {
    activeProfileId: 'local',
    status: 'connected',
    activeRunCount: 0,
    syncedAt: 1
  })
  const table = ui.find({ label: 'Runs', type: 'Table' })!
  const cells = table.props.rows as Array<{ cells: { Status: string } }>
  assert.equal(cells.length, 60)
  assert.equal(cells[59].cells.Status, '🟢 SUCCESS')
  await ui.activateRow({ label: 'Runs' }, '59')
  assert.equal(detailCalls, 1)
  await ui.setVisible(false)
  await new Promise((r) => setTimeout(r, 350))
  await ui.setVisible(true)
  await new Promise((r) => setTimeout(r, 350))
  assert.equal(detailCalls, 2)
})

test('previews a Dagster expression in lineage without launching and browses partitions without opening source', async () => {
  const host = mockHost({
    requests: {
      ...requests,
      'dagster.snapshot': {
        ...data,
        uiUrl: 'https://example.test/deploy/graphql',
        assets: [...data.assets, { ...data.assets[0], assetKey: ['raw'], isPartitioned: false }]
      }
    }
  })
  view = await mount(panel, { host })
  const expression = '+key:"sales/daily" or group:"raw"'
  await view.type({ label: 'Dagster asset selection' }, expression)
  await view.press({ text: 'Review selection in Dagster DAG' })
  assert.deepEqual(host.callsOf('shell.openExternal'), [
    { url: 'https://example.test/deploy/asset-groups~' + encodeURIComponent(expression) + '/' }
  ])
  await view.press({ text: 'Partitioned assets' })
  const tree = JSON.stringify(view.find({ label: 'Partitioned assets' })?.props.items)
  assert.match(tree, /daily_revenue/)
  assert.ok(!tree.includes('"raw"'))
  await view.selectItem({ label: 'Partitioned assets' }, keyOf(data.assets[0]))
  assert.equal(view.valueOf({ label: 'Partition' }), '2026-11-24')
  await view.press({ text: 'View all partitions in Dagster' })
  assert.deepEqual(host.callsOf('shell.openExternal').at(-1), {
    url: 'https://example.test/deploy/assets/daily_revenue?view=partitions'
  })
  assert.deepEqual(view.errors, [])
})

test('changed and downstream use the full graph, review exact targets, and launch once per group', async () => {
  const node = (name: string) => ({
    ...data.assets[0],
    assetKey: [name],
    isPartitioned: false,
    jobNames: ['__ASSET_JOB'],
    keyString: JSON.stringify(['sample_project', '__repository__', [name]]),
    dependencyKeys: [] as string[]
  })
  const root = node('raw'),
    child = node('cleaned')
  child.dependencyKeys = [root.keyString]
  const launches: string[][][] = []
  const ui = await start({
    'dagster.graph': (i) =>
      i.offset ? { nodes: [child], token: 'graph' } : { nodes: [root], token: 'graph', next: 1 },
    'dagster.health': [],
    'source.index': {
      changed: ['model.sql'],
      files: [{ path: 'model.sql', assetIds: [root.keyString] }]
    },
    'dagster.materialize': (i) => {
      launches.push(i.assetKeys)
      return { runId: 'reviewed' }
    }
  })
  await ui.select({ label: 'Dagster views' }, 'dag')
  await ui.toggle({ type: 'Collapsible', text: /^Git diff/ })
  await ui.press({ text: 'Materialize Changed + Downstream' })
  assert.deepEqual(launches, [])
  await ui.press({ text: 'Launch reviewed group' })
  assert.deepEqual(launches, [[['raw'], ['cleaned']]])
  assert.ok(!ui.find({ text: 'Launch reviewed group' }))
})

test('column history labels additions, removals and type changes in the selected asset drawer', async () => {
  const ui = await start({
    'source.open': { opened: false },
    'dagster.columns': {
      columns: [
        { name: 'id', type: 'int' },
        { name: 'amount', type: 'decimal' }
      ],
      previous: [
        { name: 'amount', type: 'float' },
        { name: 'legacy', type: 'text' }
      ],
      lineage: []
    }
  })
  await ui.selectItem({ label: 'Assets' }, keyOf(data.assets[0]))
  await ui.press({ text: 'Inspect column schema' })
  assert.ok(ui.find({ text: 'id · int · added' }))
  assert.ok(ui.find({ text: 'amount · decimal · was float' }))
  assert.ok(ui.find({ text: 'legacy · removed' }))
})

test('a slow source index leaves the sidebar interactive and the graph-only asset has a selectable key', async () => {
  const asset = { ...data.assets[0], assetKey: ['full-graph-only'], isPartitioned: false }
  let release!: () => void, began!: () => void
  const indexing = new Promise<void>((resolve) => {
    began = resolve
  })
  const delayed = new Promise<void>((resolve) => {
    release = resolve
  })
  const opened: string[][] = []
  const ui = await start({
    'dagster.graph': {
      token: 'full',
      nodes: [{ ...asset, keyString: keyOf(asset), dependencyKeys: [], health: {} }]
    },
    'source.index': async () => {
      began()
      await delayed
      return { files: [], changed: [] }
    },
    'source.open': (i) => {
      opened.push(i.assetKey!)
      return { opened: false }
    }
  })
  const switching = ui.select({ label: 'Dagster views' }, 'dag')
  try {
    await Promise.race([
      indexing,
      new Promise((_, reject) =>
        setTimeout(() => reject(Error('Index request did not start')), 1500).unref()
      )
    ])
    assert.equal(ui.find({ label: 'Refresh now' })?.disabled, false)
    assert.equal(ui.find({ text: 'Reload', type: 'Button' })?.disabled, false)
    // The component test host settles all outstanding IPC before returning an action.
    // Inspect the controls while the index is blocked, then let that host settle.
    release()
    await switching
    await ui.select({ label: 'Dagster views' }, 'assets')
    await ui.selectItem({ label: 'Assets' }, keyOf(asset))
    assert.deepEqual(opened, [asset.assetKey])
    assert.equal(ui.find({ label: 'Assets', type: 'Tree' })?.props.selected, keyOf(asset))
  } finally {
    release()
    await switching
  }
})

test('the sidebar shows the exact repository command and cwd and sends approval only after Run command', async () => {
  const plan = { cwd: '/work/a b', command: 'uv run dagster dev -p 4399' }
  const attempts: unknown[] = []
  const ui = await start({
    'dagster.snapshot': { ...data, status: 'stopped', localState: { status: 'stopped' } },
    'local.start': (i) => {
      attempts.push(i.confirmed)
      return {
        state: i.confirmed ? { status: 'starting' } : { status: 'stopped', confirmation: plan }
      }
    }
  })
  await ui.press({ text: 'Start', type: 'Button' })
  assert.deepEqual(attempts, [undefined])
  assert.ok(ui.findAll({ type: 'CodeView' }).some((v) => v.props.code === plan.cwd))
  assert.ok(ui.findAll({ type: 'CodeView' }).some((v) => v.props.code === plan.command))
  await ui.press({ text: 'Cancel', type: 'Button' })
  assert.deepEqual(attempts, [undefined])
  await ui.press({ text: 'Start', type: 'Button' })
  await ui.press({ text: 'Run command', type: 'Button' })
  assert.deepEqual(attempts, [undefined, undefined, plan])
})

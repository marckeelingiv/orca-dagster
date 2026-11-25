import { mkdir, mkdtemp, rm, symlink, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { assert, mockHost, mountWorker, test } from 'orca/testing'
import type { OrcaWorkerApi } from 'orca/worker'
import { openSource, resolveSource } from '../src/source/resolver'
import type { ConnectionProfile, SourceInput } from '../src/types'

const root = resolve('.')
const sample = 'docker/sample_project/src/sample_project/'
const profile: ConnectionProfile = {
  id: 'dev',
  name: 'dev',
  kind: 'local',
  auth: { mode: 'none' },
  url: 'http://localhost:3000',
  local: { pathPrefixMap: { '/app': '.', '/app/src': 'docker/sample_project/src' } }
}
const asset: SourceInput = { kind: 'asset', name: 'raw_orders' }

test('metadata maps direct and container paths and returns line numbers independently of opening', async () => {
  const host = mockHost({ results: { 'editor.open': { opened: true } } })
  let api!: OrcaWorkerApi
  const worker = await mountWorker(
    {
      default(orca) {
        api = orca
      }
    },
    { host }
  )
  try {
    const path = sample + 'assets.py'
    const direct = await resolveSource(
      root,
      profile,
      { ...asset, codeReferences: [{ filePath: join(root, path), lineNumber: 21 }] },
      profile.url
    )
    assert.deepEqual(direct, { opened: false, resolvedPath: path, lineNumber: 21 })
    assert.equal(host.callsOf('editor.open').length, 0)
    const commands: string[][] = []
    const opened = await openSource(
      api,
      'ws',
      root,
      profile,
      {
        ...asset,
        codeReferences: [{ filePath: '/app/src/sample_project/assets.py', lineNumber: 20 }]
      },
      profile.url,
      async (args) => {
        commands.push(args)
        return ''
      }
    )
    assert.deepEqual(opened, { opened: true, resolvedPath: path, lineNumber: 20 })
    assert.deepEqual(commands, [
      ['file', 'open', '--path', path, '--worktree', 'ws', '--line', '20', '--focus']
    ])
  } finally {
    await worker.deactivate()
  }
})

test('finds decorated functions and factory calls in the sample workspace', async () => {
  for (const [kind, name, file] of [
    ['asset', 'raw_orders', 'assets'],
    ['job', 'orders_pipeline_job', 'jobs'],
    ['schedule', 'daily_orders_schedule', 'schedules'],
    ['sensor', 'new_orders_sensor', 'sensors']
  ] as const) {
    const found = await resolveSource(root, profile, { kind, name }, profile.url)
    assert.equal(found.resolvedPath, sample + file + '.py')
    assert.ok(found.lineNumber)
  }
})

test('skips escaping symlinks and returns the metadata URL or Dagster UI', async () => {
  const workspace = await mkdtemp(join(tmpdir(), 'dagster-source-'))
  const outside = await mkdtemp(join(tmpdir(), 'dagster-other-'))
  try {
    await writeFile(join(outside, 'only.py'), '@asset\ndef only(): pass')
    await symlink(outside, join(workspace, 'linked'), 'dir')
    const input: SourceInput = {
      kind: 'asset',
      name: 'only',
      codeReferences: [{ filePath: 'linked/only.py' }, { url: 'https://git.example/only' }]
    }
    const result = await resolveSource(workspace, profile, input, profile.url)
    assert.equal(result.resolvedPath, undefined)
    assert.equal(result.externalUrl, 'https://git.example/only')
    assert.equal(
      (await resolveSource(null, profile, asset, profile.url)).externalUrl,
      profile.url + '/assets/raw_orders'
    )
  } finally {
    await Promise.all([rm(workspace, { recursive: true }), rm(outside, { recursive: true })])
  }
})

test('resolves dbt SQL, translated model keys, and the source declaration in YAML', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dagster-dbt-'))
  try {
    await mkdir(join(root, 'models'))
    await writeFile(join(root, 'dbt_project.yml'), 'name: demo')
    await writeFile(join(root, 'models/ledger.sql'), 'select 1')
    const model: SourceInput = {
      kind: 'asset',
      name: 'translated',
      dbtUniqueId: 'model.demo.ledger'
    }
    assert.equal(
      (await resolveSource(root, profile, model, profile.url)).resolvedPath,
      'models/ledger.sql'
    )
    await mkdir(join(root, 'target'))
    await writeFile(
      join(root, 'models/sources.yml'),
      'sources:\n  - name: raw\n    tables:\n      - name: receipts\n'
    )
    await writeFile(
      join(root, 'target/manifest.json'),
      JSON.stringify({
        nodes: {},
        sources: {
          'source.demo.raw.receipts': {
            source_name: 'raw',
            name: 'receipts',
            original_file_path: 'models/sources.yml'
          }
        }
      })
    )
    const source = await resolveSource(
      root,
      profile,
      { kind: 'asset', name: 'receipts', assetKey: ['raw', 'receipts'] },
      profile.url
    )
    assert.equal(source.resolvedPath, 'models/sources.yml')
    assert.equal(source.lineNumber, 4)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('static inventory resolves stacked decorators, literal keys and prefixes, AssetSpecs and versioned dbt models', async () => {
  const root = await mkdtemp(join(tmpdir(), 'scanner-'))
  try {
    await writeFile(
      join(root, 'defs.py'),
      [
        '@dg.asset(key_prefix=["shop","stage"], name="orders")',
        '@other.wrapper(value=(2, 3))',
        'async def internal(): pass',
        '@asset(key=dg.AssetKey(["finance", "orders"]))',
        'def other(): pass',
        'specs = [dg.AssetSpec(key=["raw", "events"]), AssetSpec("accounts")]',
        '@multi_asset(specs=[AssetSpec(["raw","customers"])])',
        'def definitions(): pass',
        '@dbt_assets(manifest=manifest)',
        '@wrap',
        'def transform(): pass'
      ].join('\n')
    )
    await writeFile(join(root, 'dbt_project.yml'), 'name: shop')
    await mkdir(join(root, 'models'))
    await writeFile(join(root, 'models/orders.sql'), 'select 1')
    await writeFile(join(root, 'models/orders_v1.sql'), 'select 2')
    for (const [assetKey, line] of [
      [['shop', 'stage', 'orders'], 1],
      [['finance', 'orders'], 4],
      [['raw', 'events'], 6],
      [['accounts'], 6],
      [['raw', 'customers'], 7],
      [['transform'], 9]
    ] as const) {
      const found = await resolveSource(
        root,
        profile,
        { kind: 'asset', name: assetKey.join('/'), assetKey: [...assetKey] },
        profile.url
      )
      assert.equal(found.resolvedPath, 'defs.py')
      assert.equal(found.lineNumber, line)
    }
    const wrap = await resolveSource(
      root,
      profile,
      { kind: 'asset', name: 'one', dbtUniqueId: 'model.p.one', opNames: ['transform'] },
      profile.url
    )
    assert.equal(wrap.lineNumber, 9)
    const model = await resolveSource(
      root,
      profile,
      { kind: 'asset', name: 'translated', dbtUniqueId: 'model.p.orders.v1' },
      profile.url
    )
    assert.equal(model.resolvedPath, 'models/orders_v1.sql')
    await rm(join(root, 'models/orders_v1.sql'))
    const fallback = await resolveSource(
      root,
      profile,
      { kind: 'asset', name: 'translated', dbtUniqueId: 'model.p.orders.v1' },
      profile.url
    )
    assert.equal(fallback.resolvedPath, 'models/orders.sql')
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

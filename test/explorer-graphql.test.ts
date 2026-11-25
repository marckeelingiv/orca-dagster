import assert from 'node:assert/strict'
import { test } from 'node:test'
import { Explorer, type FlowScope } from '../src/dagster/explorer'
import { backfillKeys } from '../src/dagster/partition-status'
import { data } from '../src/panel/fixtures'

const repo = { name: 'repo', location: { name: 'loc' } }
const node = (name: string) => ({
  assetKey: { path: [name] },
  repository: repo,
  groupName: 'shop',
  isMaterializable: true,
  isPartitioned: false,
  jobNames: ['__ASSET_JOB'],
  assetMaterializations: [],
  metadataEntries: [],
  dependencyKeys: [] as { path: string[] }[],
  tags: [{ key: 'dagster/kind/dbt', value: '' }]
})
const id = (name: string) => JSON.stringify(['loc', 'repo', [name]])
type Call = { query: string; variables: Record<string, unknown> }
function server(handler: (call: Call) => object) {
  const calls: Call[] = []
  const transport: typeof fetch = async (_, init) => {
    const call = JSON.parse(String(init?.body))
    calls.push(call)
    return Response.json(handler(call))
  }
  const connect = () => new Explorer('https://local.invalid/graphql', {}, { fetch: transport })
  return { calls, connect, client: connect() }
}
const unsupported = (name: string) => ({
  errors: [{ message: "Cannot query field '" + name + "' on type 'AssetNode'." }]
})

test('graph carries health and check counts alongside topology, including failed checks, in one query', async () => {
  const { client, calls } = server(() => ({
    data: {
      assetNodes: [
        node('raw'),
        {
          ...node('orders'),
          dependencyKeys: [{ path: ['raw'] }],
          kinds: ['dbt', 'duckdb'],
          assetChecksOrError: {
            checks: [
              { name: 'not_null', executionForLatestMaterialization: { status: 'FAILED' } },
              { name: 'unique', executionForLatestMaterialization: null }
            ]
          }
        }
      ]
    }
  }))
  const [, orders] = await client.graph()
  assert.equal(calls.length, 1)
  assert.match(calls[0].query, /assetChecksOrError/)
  assert.deepEqual(orders.dependencyKeys, [id('raw')])
  assert.deepEqual(orders.kinds, ['dbt', 'duckdb'])
  assert.equal(orders.health?.checkCount, 2)
  assert.deepEqual(orders.health?.failed, ['not_null'])
})

test('older graphs retry without unavailable fields, preserve independent features and remember the schema', async () => {
  const { client, connect, calls } = server(({ query }) => {
    if (query.includes(' kinds ')) return unsupported('kinds')
    if (query.includes('CodeReferencesMetadataEntry'))
      return { errors: [{ message: "Unknown type 'CodeReferencesMetadataEntry'." }] }
    if (query.includes('freshnessStatusInfo')) return unsupported('freshnessStatusInfo')
    return { data: { assetNodes: [node('raw')] } }
  })
  const rows = await client.graph()
  assert.deepEqual(rows[0].kinds, ['dbt'])
  assert.ok(calls.at(-1)!.query.includes('assetChecksOrError'))
  const before = calls.length
  await connect().graph()
  assert.equal(calls.length, before + 1)
  const denied = server(() => ({
    errors: [{ message: 'restricted', extensions: { code: 'FORBIDDEN' } }]
  }))
  await assert.rejects(denied.client.graph(), /restricted/)
  assert.equal(denied.calls.length, 1)
})

test('legacy column history keeps the table schema and cannot pick a different partition', async () => {
  const { client } = server(({ query }) => {
    if (query.includes('TableColumnLineageMetadataEntry'))
      return { errors: [{ message: "Unknown type 'TableColumnLineageMetadataEntry'." }] }
    if (query.includes('partitions:$parts'))
      return {
        errors: [
          { message: "Unknown argument 'partitions' on field 'AssetNode.assetMaterializations'." }
        ]
      }
    return {
      data: {
        assetNodeOrError: {
          __typename: 'AssetNode',
          assetMaterializations: ['other', 'day'].map((partition) => ({
            partition,
            metadataEntries: [
              {
                label: 'dagster/column_schema',
                schema: { columns: [{ name: partition, type: 'int' }] }
              }
            ]
          }))
        }
      }
    }
  })
  const result = await client.columns(['orders'], 'day')
  assert.deepEqual(result.columns, [{ name: 'day', type: 'int' }])
  assert.equal(result.previous, undefined)
  assert.deepEqual(result.lineage, [])
})

test('column clicks fetch the asset dependency cone once per partition and topology, even across clients', async () => {
  const scope: FlowScope = {
    assetId: id('orders'),
    cache: new Map(),
    topology: ['raw', 'orders', 'mart', 'unrelated'].map((name, i) => ({
      ...data.assets[0],
      assetKey: [name],
      keyString: id(name),
      dependencyKeys: i === 1 ? [id('raw')] : i === 2 ? [id('orders')] : []
    }))
  }
  const { client, connect, calls } = server(({ query, variables }) => {
    assert.match(query, /assetNodes\(assetKeys:\$keys\)/)
    assert.deepEqual(
      variables.keys,
      ['raw', 'orders', 'mart'].map((name) => ({ path: [name] }))
    )
    return {
      data: {
        assetNodes: ['raw', 'orders', 'mart'].map((name, i) => ({
          ...node(name),
          latest: [
            {
              metadataEntries: [
                {
                  label: 'dagster/column_lineage',
                  lineage: i
                    ? [
                        {
                          columnName: 'amount',
                          columnDeps: [
                            {
                              assetKey: { path: [i === 1 ? 'raw' : 'orders'] },
                              columnName: 'amount'
                            }
                          ]
                        }
                      ]
                    : []
                }
              ]
            }
          ]
        }))
      }
    }
  })
  const flow = await client.columnFlow(['orders'], 'amount', undefined, scope)
  assert.deepEqual(flow.map((f) => f.id).sort(), ['raw', 'orders', 'mart'].map(id).sort())
  assert.deepEqual(flow.find((f) => f.id === id('mart'))?.upstreamIds, [id('orders')])
  const second = await connect().columnFlow(['orders'], 'currency', undefined, scope)
  assert.deepEqual(second, [{ id: id('orders'), column: 'currency', upstreamIds: [] }])
  assert.equal(calls.length, 1)
  await connect().columnFlow(['orders'], 'amount', 'day', scope)
  assert.equal(calls.length, 2)
})

test('column flow can show the selected column on an old server without lineage metadata', async () => {
  const { client, calls } = server(({ query }) =>
    query.includes('TableColumnLineageMetadataEntry')
      ? { errors: [{ message: "Unknown type 'TableColumnLineageMetadataEntry'." }] }
      : { data: { assetNodes: [{ ...node('a'), latest: [{ metadataEntries: [] }] }] } }
  )
  assert.deepEqual(await client.columnFlow(['a'], 'x'), [
    { id: id('a'), column: 'x', upstreamIds: [] }
  ])
  assert.equal(calls.length, 2)
})

test('paging and non-contiguous failed partitions use one catalog, with faithful inclusive ranges', async () => {
  const keys = Array.from({ length: 146 }, (_, i) => 'p' + i)
  const catalog = {
    partitionKeysByDimension: [{ name: 'day', type: 'STATIC', partitionKeys: keys }],
    assetPartitionStatuses: {
      __typename: 'DefaultPartitionStatuses' as const,
      materializedPartitions: [],
      failedPartitions: ['p2', 'p52', 'p145'],
      materializingPartitions: []
    }
  }
  const { client, calls } = server(() => ({
    data: { assetNodeOrError: { __typename: 'AssetNode', ...catalog } }
  }))
  let page = await client.partitionPage(['a'])
  const seen = [...page.rows]
  while (page.hasMore) {
    page = await client.partitionPage(['a'], page.cursor)
    seen.push(...page.rows)
  }
  assert.deepEqual(
    seen.map((r) => r.key),
    keys
  )
  assert.equal(calls.length, 1)
  assert.deepEqual(backfillKeys(catalog, 'failed'), ['p2', 'p52', 'p145'])
  assert.deepEqual(backfillKeys(catalog, 'p46..p51'), keys.slice(46, 52))
  assert.throws(() => backfillKeys(catalog, 'p51..p2'), /No matching partitions/)
})

test('legacy column lookup stays scoped when the assetNodes list cannot filter by key', async () => {
  const scope: FlowScope = {
    assetId: id('raw'),
    cache: new Map(),
    topology: [{ ...data.assets[0], assetKey: ['raw'], keyString: id('raw'), dependencyKeys: [] }]
  }
  const { client, calls } = server(({ query, variables }) => {
    if (query.includes('assetNodes(assetKeys:'))
      return { errors: [{ message: "Unknown argument 'assetKeys' on field 'Query.assetNodes'." }] }
    assert.match(query, /n0:assetNodeOrError/)
    assert.deepEqual(variables.k0, { path: ['raw'] })
    return { data: { n0: { ...node('raw'), __typename: 'AssetNode', latest: [] } } }
  })
  assert.deepEqual(await client.columnFlow(['raw'], 'x', undefined, scope), [
    { id: id('raw'), column: 'x', upstreamIds: [] }
  ])
  assert.equal(calls.length, 2)
})

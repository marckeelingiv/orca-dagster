import assert from 'node:assert/strict'
import { test } from 'node:test'
import { evaluateAssetSelection } from '../src/dagster/selection'
import { data } from '../src/panel/fixtures'
import type { GraphNode } from '../src/types'

const nodes: GraphNode[] = ['raw', 'orders', 'sales'].map((name, i) => ({
  ...data.assets[0],
  assetKey: ['shop', name],
  keyString: JSON.stringify(['private-repo', [name]]),
  kinds: i ? ['dbt'] : ['duckdb'],
  dependencyKeys: [],
  status: i ? 'warning' : 'success',
  staleStatus: i ? 'STALE' : 'FRESH',
  running: i === 2
}))
nodes[1].dependencyKeys = [nodes[0].keyString]
nodes[2].dependencyKeys = [nodes[1].keyString]
function select(query: string) {
  const result = evaluateAssetSelection(query, nodes, { changed: new Set([nodes[1].keyString]) })
  assert.equal(result.error, undefined)
  return result.matchedAssets.map((a) => a.assetKey[1])
}
test('selection traverses the graph and combines filters on asset status and metadata', () => {
  assert.deepEqual(select('roots(*)'), ['raw'])
  assert.deepEqual(select('sinks(*)'), ['sales'])
  assert.deepEqual(select('changed*'), ['orders', 'sales'])
  assert.deepEqual(select('+sales and kind:dbt'), ['orders', 'sales'])
  assert.deepEqual(select('status:running'), ['sales'])
  assert.deepEqual(select('status:stale and not status:running'), ['orders'])
  assert.ok(evaluateAssetSelection('bad:value', nodes).error)
})
test('glob matching considers the leaf and full user-facing key and treats other punctuation literally', () => {
  assert.deepEqual(select('key:"shop/o***s"'), ['orders'])
  assert.deepEqual(select('key:"or*rs"'), ['orders'])
  assert.deepEqual(select('key:"*private-repo*"'), [])
  assert.deepEqual(select('key:"[shop/orders]"'), [])
  const long = [{ ...nodes[0], assetKey: ['a'.repeat(2000) + 'x'] }]
  const result = evaluateAssetSelection('key:"' + '*a'.repeat(100) + 'b*"', long)
  assert.equal(result.matchedKeys.size, 0)
})

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { computeVerticalDagLayout } from '../src/dagster/layout'
import { data } from '../src/panel/fixtures'
const node = (name: string, parents: string[] = []) => ({
  ...data.assets[0],
  assetKey: [name],
  keyString: name,
  dependencyKeys: parents
})
test('layered layout centers rows, places every dependency above its child and draws unique cubic edges', () => {
  const catalog = [node('z', ['a', 'b']), node('b', ['external']), node('a', ['external', 'a'])]
  const result = computeVerticalDagLayout(catalog)
  assert.deepEqual(result, computeVerticalDagLayout([...catalog].reverse()))
  const byKey = new Map(result.nodes.map((n) => [n.key, n]))
  assert.equal(byKey.get('external')!.external, true)
  assert.equal(result.layerCount, 3)
  assert.equal(result.edges.length, 4)
  for (const e of result.edges) {
    assert.ok(byKey.get(e.from)!.y < byKey.get(e.to)!.y)
    assert.match(e.path, /^M .* C /)
  }
  const hidden = computeVerticalDagLayout(catalog, { includeExternalSources: false })
  assert.equal(hidden.nodes.length, 3)
  assert.equal(hidden.edges.length, 2)
})
test('layout tolerates cycles, an empty graph, and deep chains', () => {
  assert.deepEqual(computeVerticalDagLayout([]).nodes, [])
  const cycle = computeVerticalDagLayout([node('a', ['b']), node('b', ['a'])])
  assert.equal(cycle.nodes.length, 2)
  assert.ok(cycle.edges.every((e) => !e.path.includes('NaN')))
  const chain = Array.from({ length: 2000 }, (_, i) => node(String(i), i ? [String(i - 1)] : []))
  assert.equal(computeVerticalDagLayout(chain).layerCount, chain.length)
})

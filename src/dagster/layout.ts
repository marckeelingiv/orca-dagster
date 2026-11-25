import type { GraphNode as AssetSummary } from '../types'

export interface DagLayoutNode {
  key: string
  label: string
  external: boolean
  asset?: AssetSummary
  layer: number
  order: number
  x: number
  y: number
  width: number
  height: number
}

export interface DagLayoutEdge {
  id: string
  from: string
  to: string
  external: boolean
  path: string
}

export interface DagLayoutResult {
  nodes: DagLayoutNode[]
  edges: DagLayoutEdge[]
  width: number
  height: number
  layerCount: number
}

export interface DagLayoutOptions {
  nodeWidth?: number
  nodeHeight?: number
  horizontalGap?: number
  verticalGap?: number
  padding?: number
  includeExternalSources?: boolean
}

/**
 * Computes a clean top-to-bottom (vertical) layered DAG layout from Dagster assets.
 * Upstream dependencies sit above downstream assets; rows are centered and ordered via
 * a 2-pass barycenter heuristic to reduce edge crossings.
 */
export function computeVerticalDagLayout(
  assets: readonly AssetSummary[],
  options?: DagLayoutOptions
): DagLayoutResult {
  const nodeWidth = options?.nodeWidth ?? 216
  const nodeHeight = options?.nodeHeight ?? 68
  const horizontalGap = options?.horizontalGap ?? 26
  const verticalGap = options?.verticalGap ?? 54
  const padding = options?.padding ?? 36
  const includeExternal = options?.includeExternalSources ?? true

  if (assets.length === 0) {
    return { nodes: [], edges: [], width: 0, height: 0, layerCount: 0 }
  }

  const assetByKey = new Map<string, AssetSummary>()
  for (const a of assets) {
    assetByKey.set(a.keyString, a)
  }

  const allKeys = new Set<string>(assetByKey.keys())
  const externalKeys = new Set<string>()
  const parents = new Map<string, Set<string>>()
  const children = new Map<string, Set<string>>()

  const ensureKey = (k: string) => {
    if (!parents.has(k)) parents.set(k, new Set())
    if (!children.has(k)) children.set(k, new Set())
  }

  for (const key of allKeys) {
    ensureKey(key)
  }

  for (const a of assets) {
    for (const dep of a.dependencyKeys ?? []) {
      if (dep === a.keyString) continue
      if (!assetByKey.has(dep)) {
        if (!includeExternal) continue
        allKeys.add(dep)
        externalKeys.add(dep)
        ensureKey(dep)
      }
      parents.get(a.keyString)!.add(dep)
      children.get(dep)!.add(a.keyString)
    }
    for (const child of a.dependedByKeys ?? []) {
      if (child === a.keyString) continue
      if (!assetByKey.has(child)) continue
      parents.get(child)!.add(a.keyString)
      children.get(a.keyString)!.add(child)
    }
  }

  // Kahn's topological sort with longest-path layer assignment (cycle-safe).
  const inDegree = new Map<string, number>()
  for (const k of allKeys) {
    inDegree.set(k, parents.get(k)!.size)
  }

  const queue: string[] = []
  for (const [k, deg] of inDegree.entries()) {
    if (deg === 0) queue.push(k)
  }
  queue.sort((a, b) => a.localeCompare(b))

  const layerByKey = new Map<string, number>()
  const visited = new Set<string>()

  for (let head = 0; head < queue.length; head++) {
    const k = queue[head]
    visited.add(k)
    let layer = 0
    for (const p of parents.get(k)!) {
      const pLayer = layerByKey.get(p) ?? 0
      if (pLayer + 1 > layer) layer = pLayer + 1
    }
    layerByKey.set(k, layer)

    const nextChildren = [...children.get(k)!].sort((a, b) => a.localeCompare(b))
    for (const c of nextChildren) {
      const nextDeg = (inDegree.get(c) ?? 1) - 1
      inDegree.set(c, nextDeg)
      if (nextDeg === 0) {
        queue.push(c)
      }
    }
  }

  // Fallback for any nodes in a cycle (rare in Dagster, but keeps layout deterministic).
  for (const k of allKeys) {
    if (!visited.has(k)) {
      let maxParent = -1
      for (const p of parents.get(k)!) {
        const l = layerByKey.get(p)
        if (l !== undefined && l > maxParent) maxParent = l
      }
      layerByKey.set(k, maxParent + 1)
    }
  }

  // Group by layer.
  const layersMap = new Map<number, string[]>()
  let maxLayer = 0
  for (const [k, l] of layerByKey.entries()) {
    if (l > maxLayer) maxLayer = l
    const list = layersMap.get(l) ?? []
    list.push(k)
    layersMap.set(l, list)
  }

  const layers: string[][] = []
  for (let l = 0; l <= maxLayer; l += 1) {
    const row = layersMap.get(l) ?? []
    // Initial deterministic sort: groupName, then external vs internal, then key
    row.sort((a, b) => {
      const extA = externalKeys.has(a) ? 0 : 1
      const extB = externalKeys.has(b) ? 0 : 1
      if (extA !== extB) return extA - extB
      const grpA = assetByKey.get(a)?.groupName ?? ''
      const grpB = assetByKey.get(b)?.groupName ?? ''
      if (grpA !== grpB) return grpA.localeCompare(grpB)
      return a.localeCompare(b)
    })
    layers.push(row)
  }

  // Barycenter crossing reduction (2 downward + 2 upward sweeps).
  const posIndex = new Map<string, number>()
  const indexRow = (row: string[]) => row.forEach((k, idx) => posIndex.set(k, idx))
  layers.forEach(indexRow)
  function reorder(row: string[], neighbors: Map<string, Set<string>>) {
    if (row.length < 2) return
    const weights = new Map(
      row.map((k) => [k, averageNeighborPos(neighbors.get(k)!, posIndex, posIndex.get(k)!)])
    )
    row.sort((a, b) => weights.get(a)! - weights.get(b)! || a.localeCompare(b))
    indexRow(row)
  }
  for (let iter = 0; iter < 2; iter++) {
    for (let l = 1; l < layers.length; l++) reorder(layers[l], parents)
    for (let l = layers.length - 2; l >= 0; l--) reorder(layers[l], children)
  }

  // Compute coordinates, centering narrower layers horizontally.
  const maxRowCols = layers.reduce((acc, row) => Math.max(acc, row.length), 1)
  const contentWidth = maxRowCols * nodeWidth + Math.max(0, maxRowCols - 1) * horizontalGap
  const totalWidth = contentWidth + padding * 2
  const totalHeight =
    layers.length * nodeHeight + Math.max(0, layers.length - 1) * verticalGap + padding * 2

  const nodes: DagLayoutNode[] = []
  const nodeByKey = new Map<string, DagLayoutNode>()

  layers.forEach((row, layerIdx) => {
    const rowWidth = row.length * nodeWidth + Math.max(0, row.length - 1) * horizontalGap
    const startX = padding + (contentWidth - rowWidth) / 2
    const y = padding + layerIdx * (nodeHeight + verticalGap)

    row.forEach((key, order) => {
      const x = startX + order * (nodeWidth + horizontalGap)
      const asset = assetByKey.get(key)
      const node: DagLayoutNode = {
        key,
        label: asset?.assetKey.join('/') ?? externalLabel(key),
        external: externalKeys.has(key),
        asset,
        layer: layerIdx,
        order,
        x,
        y,
        width: nodeWidth,
        height: nodeHeight
      }
      nodes.push(node)
      nodeByKey.set(key, node)
    })
  })

  // Build smooth vertical cubic Bézier edges.
  const edges: DagLayoutEdge[] = []
  for (const node of nodes) {
    const kids = [...(children.get(node.key) ?? [])].sort((a, b) => a.localeCompare(b))
    for (const targetKey of kids) {
      const target = nodeByKey.get(targetKey)
      if (!target) continue
      const x1 = node.x + node.width / 2
      const y1 = node.y + node.height
      const x2 = target.x + target.width / 2
      const y2 = target.y
      const dy = Math.max(24, Math.abs(y2 - y1) * 0.48)
      const path = `M ${x1} ${y1} C ${x1} ${y1 + dy}, ${x2} ${y2 - dy}, ${x2} ${y2}`
      edges.push({
        id: `${node.key}->${targetKey}`,
        from: node.key,
        to: targetKey,
        external: node.external,
        path
      })
    }
  }

  return {
    nodes,
    edges,
    width: totalWidth,
    height: totalHeight,
    layerCount: layers.length
  }
}

function externalLabel(key: string) {
  try {
    const path: unknown = JSON.parse(key)
    if (Array.isArray(path) && path.every((part) => typeof part === 'string')) return path.join('/')
  } catch {
    /* The layout also accepts plain external asset names. */
  }
  return key
}

function averageNeighborPos(
  neighbors: Set<string>,
  posIndex: Map<string, number>,
  fallback: number
): number {
  if (neighbors.size === 0) return fallback
  let sum = 0
  let count = 0
  for (const nb of neighbors) {
    const pos = posIndex.get(nb)
    if (pos !== undefined) {
      sum += pos
      count += 1
    }
  }
  return count > 0 ? sum / count : fallback
}

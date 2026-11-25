import type { PartitionPage } from '../types'

type Dimension = { name: string; type: string; partitionKeys: string[] }
type TimeRange = { startKey: string; endKey: string; status: string }
type OneDimensional =
  | { __typename: 'TimePartitionStatuses'; ranges: TimeRange[] }
  | {
      __typename: 'DefaultPartitionStatuses'
      failedPartitions: string[]
      materializedPartitions: string[]
      materializingPartitions: string[]
    }
type Multi = {
  __typename: 'MultiPartitionStatuses'
  primaryDimensionName: string
  ranges: { primaryDimStartKey: string; primaryDimEndKey: string; secondaryDim: OneDimensional }[]
}
export type PartitionCatalog = {
  partitionKeysByDimension: Dimension[]
  assetPartitionStatuses: OneDimensional | Multi
}

export function partitionPage(
  catalog: PartitionCatalog,
  cursor?: string,
  fixed?: Record<string, string>,
  pageSize = 48
): PartitionPage {
  const statuses = catalog.assetPartitionStatuses
  const dimensions = catalog.partitionKeysByDimension
  const primary = dimensions.find(
    (d) =>
      d.name ===
      (statuses.__typename === 'MultiPartitionStatuses'
        ? statuses.primaryDimensionName
        : dimensions[0]?.name)
  )
  if (!primary) return { rows: [], cursor: '', hasMore: false }
  const other = dimensions.filter((d) => d !== primary)
  const selected = Object.fromEntries(
    other.map((d) => [d.name, fixed?.[d.name] ?? d.partitionKeys[0]])
  )
  const start = cursor ? primary.partitionKeys.indexOf(cursor) + 1 : 0
  if (cursor && !start) throw Error('Partition definitions changed. Reload the grid.')
  const slice = primary.partitionKeys.slice(start, start + pageSize)
  const orders = new Map(
    dimensions.map((d) => [d.name, new Map(d.partitionKeys.map((k, i) => [k, i]))])
  )
  function inRange(d: Dimension, key: string, a: string, b: string) {
    const order = orders.get(d.name)!,
      pos = order.get(key)
    return pos !== undefined && pos >= (order.get(a) ?? Infinity) && pos <= (order.get(b) ?? -1)
  }
  const staticStatuses = new Map<OneDimensional, Map<string, string>>()
  function status(s: OneDimensional | undefined, key: string, d: Dimension) {
    if (!s) return 'missing'
    if (s.__typename === 'TimePartitionStatuses') {
      const value = s.ranges.find((r) => inRange(d, key, r.startKey, r.endKey))?.status
      return value === 'MATERIALIZED' ? 'success' : (value?.toLowerCase() ?? 'missing')
    }
    let lookup = staticStatuses.get(s)
    if (!lookup) {
      lookup = new Map()
      for (const k of s.materializedPartitions) lookup.set(k, 'success')
      for (const k of s.materializingPartitions) lookup.set(k, 'materializing')
      for (const k of s.failedPartitions) lookup.set(k, 'failed')
      staticStatuses.set(s, lookup)
    }
    return lookup.get(key) ?? 'missing'
  }
  for (const d of other)
    if (!d.partitionKeys.includes(selected[d.name]))
      throw Error('Choose a valid ' + d.name + ' partition.')
  const rows = slice.map((primaryKey) => {
    if (statuses.__typename !== 'MultiPartitionStatuses')
      return { key: primaryKey, label: primaryKey, status: status(statuses, primaryKey, primary) }
    const range = statuses.ranges.find((r) =>
      inRange(primary, primaryKey, r.primaryDimStartKey, r.primaryDimEndKey)
    )
    const parts = { ...selected, [primary.name]: primaryKey }
    const key = Object.keys(parts)
      .sort()
      .map((d) => parts[d])
      .join('|')
    return {
      key,
      label: primaryKey,
      status: status(range?.secondaryDim, selected[other[0].name], other[0])
    }
  })
  return {
    rows,
    cursor: slice.at(-1) ?? '',
    hasMore: start + slice.length < primary.partitionKeys.length,
    calendar: primary.type === 'TIME_WINDOW',
    dimensions: other,
    fixed: selected
  }
}

export function backfillKeys(
  catalog: PartitionCatalog,
  selection: string,
  fixed?: Record<string, string>
) {
  const { rows } = partitionPage(catalog, undefined, fixed, Infinity)
  let chosen: string[] = []
  if (selection === 'failed') chosen = rows.filter((r) => r.status === 'failed').map((r) => r.key)
  else if (rows.some((r) => r.key === selection)) chosen = [selection]
  else {
    const [from, to] = selection.split('..')
    const start = rows.findIndex((r) => r.key === from || r.label === from)
    const end = rows.findIndex((r) => r.key === to || r.label === to)
    if (start >= 0 && end >= start) chosen = rows.slice(start, end + 1).map((r) => r.key)
  }
  if (!chosen.length)
    throw Error(
      'No matching partitions. Enter failed, a partition key, or an inclusive start..end range.'
    )
  return chosen
}

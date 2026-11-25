import { partitionPage, type PartitionCatalog } from './partition-status'
import type {
  RunDetail,
  BackfillPreview,
  BackfillSelection,
  Column,
  ColumnEntry,
  ColumnHistory,
  GraphNode,
  HealthRow,
  PartitionPage
} from '../types'
import { assetSummary, type Key, type RawAsset } from './types'
import { assetFields, assetOptions } from './queries'
import { DagsterClient } from './client'

const nodeErrors = '__typename ... on Error {message}'
const metadata = {
  schema: '... on TableSchemaMetadataEntry {schema {columns {name type description}}}',
  lineage:
    '... on TableColumnLineageMetadataEntry {lineage {columnName columnDeps {assetKey {path} columnName}}}'
}
const healthFields = {
  causes: 'staleCauses {reason}',
  freshness: 'freshnessStatusInfo {freshnessStatus}',
  checks:
    'assetChecksOrError {... on AssetChecks {checks {name executionForLatestMaterialization {status}}}}'
}
type HealthInfo = {
  staleCauses?: { reason: string }[]
  freshnessStatusInfo?: { freshnessStatus: string }
  assetChecksOrError?: {
    checks?: { name: string; executionForLatestMaterialization?: { status: string } }[]
  }
}
function healthInfo(row: HealthInfo): GraphNode['health'] {
  const checks = row.assetChecksOrError?.checks
  return {
    causes: row.staleCauses?.map((c) => c.reason),
    freshness: row.freshnessStatusInfo?.freshnessStatus,
    checkCount: checks?.length,
    failed: checks
      ?.filter((c) =>
        ['FAILED', 'EXECUTION_FAILED'].includes(c.executionForLatestMaterialization?.status ?? '')
      )
      .map((c) => c.name)
  }
}
type Entry = { label: string; schema?: { columns: Column[] }; lineage?: ColumnEntry[] }
type FlowRow = Pick<RawAsset, 'assetKey' | 'repository' | 'isPartitioned'> & {
  latest: { metadataEntries: Entry[] }[]
  scoped?: { partition?: string; metadataEntries: Entry[] }[]
}
export type FlowCache = Map<string, { at: number; rows: FlowRow[] }>
export type FlowScope = { topology: GraphNode[]; assetId: string; cache: FlowCache }
function dependencyCone(nodes: GraphNode[], assetId: string) {
  const children = new Map<string, string[]>()
  const parents = new Map(nodes.map((n) => [n.keyString, n.dependencyKeys]))
  for (const [child, deps] of parents)
    for (const dep of deps) {
      const group = children.get(dep) ?? []
      group.push(child)
      children.set(dep, group)
    }
  const ids = new Set([assetId])
  for (const edges of [children, parents]) {
    const queue = new Set([assetId])
    for (const id of queue)
      for (const next of edges.get(id) ?? []) {
        queue.add(next)
        ids.add(next)
      }
  }
  return nodes.filter((n) => ids.has(n.keyString))
}

export class Explorer extends DagsterClient {
  async compareFailure(
    run: RunDetail,
    assetKey: string[],
    describeRevision?: (before: string, after: string) => Promise<string>
  ): Promise<string> {
    type Info = {
      runId: string
      status: string
      creationTime: number
      runConfig: unknown
      tags: { key: string; value: string }[]
      repositoryOrigin?: { repositoryName: string; repositoryLocationName: string }
    }
    const fields =
      'runId status creationTime tags {key value} repositoryOrigin {repositoryName repositoryLocationName}'
    const config = (id: string) =>
      this.operation<Info>(
        `query ComparisonRun($id:ID!){runOrError(runId:$id){${nodeErrors} ... on Run {${fields} runConfig}}}`,
        { id },
        'runOrError',
        'Run'
      )
    const failed = await config(run.runId)
    const partition = failed.tags.find((t) => t.key === 'dagster/partition')?.value
    let before = String(Math.floor(failed.creationTime * 1000))
    let baseline: Info | undefined
    while (!baseline) {
      const { assetMaterializations: rows } = await this.operation<{
        assetMaterializations: { timestamp: string; partition: string | null; runOrError: Info }[]
      }>(
        `query SuccessfulMaterializations($key:AssetKeyInput!,$parts:[String!],$before:String!){assetNodeOrError(assetKey:$key){${nodeErrors} ... on AssetNode {assetMaterializations(partitions:$parts,beforeTimestampMillis:$before,limit:50){partition timestamp runOrError {... on Run {${fields}}}}}}}`,
        { key: { path: assetKey }, parts: partition ? [partition] : undefined, before },
        'assetNodeOrError',
        'AssetNode'
      )
      baseline = rows.find(
        (r) =>
          r.partition === (partition ?? null) &&
          r.runOrError.status === 'SUCCESS' &&
          r.runOrError.repositoryOrigin?.repositoryName === run.repositoryName &&
          r.runOrError.repositoryOrigin?.repositoryLocationName === run.locationName
      )?.runOrError
      if (rows.length < 50 || rows.at(-1)!.timestamp === before) break
      before = rows.at(-1)!.timestamp
    }
    if (!baseline) return 'No earlier successful materialization for this asset and partition.'
    const previous = await config(baseline.runId)
    const diffs: string[] = []
    function compare(a: unknown, b: unknown, path: string) {
      if (diffs.length >= 25 || a === b) return
      if (a && b && typeof a === 'object' && typeof b === 'object') {
        const left = a as Record<string, unknown>,
          right = b as Record<string, unknown>
        for (const key of new Set([...Object.keys(left), ...Object.keys(right)]))
          compare(left[key], right[key], path ? path + '.' + key : key)
      } else diffs.push(path || 'root')
    }
    compare(previous.runConfig, failed.runConfig, '')
    const revision = (r: Info) =>
      r.tags.find((t) =>
        ['dagster/git_sha', 'dagster-cloud/git_sha', 'dagster/code_version'].includes(t.key)
      )?.value ?? 'unavailable'
    return this.redact(
      [
        `Last success ${previous.runId} · ${assetKey.join('/')} · ${partition ?? 'unpartitioned'}`,
        'Changed configuration keys: ' + (diffs.join(', ') || 'none'),
        'Recorded revision: ' + revision(previous) + ' → ' + revision(failed),
        await describeRevision?.(revision(previous), revision(failed))
      ]
        .filter(Boolean)
        .join('\n')
    )
  }
  async graph(): Promise<GraphNode[]> {
    const options = {
      ...assetOptions,
      ...healthFields,
      kinds: 'kinds',
      tags: 'tags {key value}',
      owners: 'owners {... on UserAssetOwner {email} ... on TeamAssetOwner {team}}',
      materializable: 'isMaterializable',
      ops: 'opNames',
      deps: 'dependencyKeys {path}'
    }
    type Node = RawAsset &
      HealthInfo & {
        kinds?: string[]
        tags?: { key: string; value: string }[]
        owners?: { team?: string; email?: string }[]
        isMaterializable?: boolean
        isSource?: boolean
        dependencyKeys?: Key[]
        dependencies?: { asset: { assetKey: Key } }[]
        opNames?: string[]
        opName?: string
      }
    const { assetNodes } = await this.compatible('Graph', options, (f) =>
      this.request<{ assetNodes: Node[] }>(
        `query Graph { assetNodes { ${assetFields(f)}
        ${f.kinds} ${f.tags} ${f.owners} ${f.materializable || 'isSource'} ${f.ops || 'opName'}
        ${f.deps || 'dependencies {asset {assetKey {path}}}'} ${f.causes} ${f.freshness} ${f.checks}
      }}`
      )
    )
    const nodes = assetNodes.map((n) => ({
      ...assetSummary(n),
      isMaterializable: n.isMaterializable ?? !n.isSource,
      health: healthInfo(n),
      opNames: n.opNames ?? (n.opName ? [n.opName] : []),
      kinds:
        n.kinds ??
        (n.tags ?? [])
          .map((t) => t.key)
          .filter((k) => k.startsWith('dagster/kind/'))
          .map((k) => k.slice('dagster/kind/'.length)),
      tags: Object.fromEntries((n.tags ?? []).map((t) => [t.key, t.value])),
      owners: (n.owners ?? []).map((o) => o.team || o.email || ''),
      keyString: JSON.stringify([n.repository.location.name, n.repository.name, n.assetKey.path]),
      dependencyKeys: [] as string[]
    }))
    const owners = new Map<string, string[]>()
    for (const n of nodes) {
      const key = JSON.stringify(n.assetKey)
      owners.set(key, [...(owners.get(key) ?? []), n.keyString])
    }
    nodes.forEach((node, i) => {
      const row = assetNodes[i]
      const dependencies =
        row.dependencyKeys ?? row.dependencies?.map((d) => d.asset.assetKey) ?? []
      node.dependencyKeys = dependencies.map((dep) => {
        const local = JSON.stringify([node.locationName, node.repositoryName, dep.path])
        const ids = owners.get(JSON.stringify(dep.path)) ?? []
        return ids.includes(local) ? local : ids.length === 1 ? ids[0] : JSON.stringify(dep.path)
      })
    })
    return nodes
  }

  async health(): Promise<HealthRow[]> {
    type Row = Pick<RawAsset, 'assetKey' | 'repository'> & HealthInfo
    const { assetNodes } = await this.compatible('Health', healthFields, (fields) =>
      this.request<{ assetNodes: Row[] }>(
        `query AssetHealthOverlay {assetNodes {assetKey {path} repository {name location {name}} ${Object.values(fields).join(' ')}}}`
      )
    )
    return assetNodes.map((row) => ({
      id: JSON.stringify([row.repository.location.name, row.repository.name, row.assetKey.path]),
      assetKey: row.assetKey.path,
      ...healthInfo(row)
    }))
  }

  async columns(assetKey: string[], partition?: string): Promise<ColumnHistory> {
    type History = { assetMaterializations: { partition?: string; metadataEntries: Entry[] }[] }
    const node = await this.compatible('Columns', { ...metadata, parts: 'partitions' }, (f) =>
      this.operation<History>(
        `query ColumnHistory($key:AssetKeyInput!${partition && f.parts ? ',$parts:[String!]' : ''}) {
          assetNodeOrError(assetKey:$key){${nodeErrors} ... on AssetNode {
          assetMaterializations(limit:2${partition && f.parts ? ',partitions:$parts' : ''}){partition metadataEntries {label ${f.schema} ${f.lineage}}}}}}`,
        { key: { path: assetKey }, parts: partition ? [partition] : undefined },
        'assetNodeOrError',
        'AssetNode'
      )
    )
    const [latest, previous] = node.assetMaterializations.filter(
      (m) => !partition || m.partition === partition
    )
    const schema = (entries?: Entry[]) =>
      entries?.find((e) => e.label === 'dagster/column_schema')?.schema?.columns
    return {
      columns: schema(latest?.metadataEntries),
      previous:
        latest?.partition === previous?.partition ? schema(previous?.metadataEntries) : undefined,
      lineage:
        latest?.metadataEntries.find((e) => e.label === 'dagster/column_lineage')?.lineage ?? []
    }
  }

  private flowRows(keys: Key[] | undefined, partition?: string): Promise<FlowRow[]> {
    return this.compatible(
      'ColumnFlow',
      { lineage: metadata.lineage, scoped: 'assetKeys', parts: 'partitions' },
      async (f) => {
        const parts = partition && f.parts ? { parts: [partition] } : {}
        const declarations = partition && f.parts ? ['$parts:[String!]'] : []
        const entries = `metadataEntries {label ${f.lineage}}`
        const fields = `assetKey {path} repository {name location {name}} isPartitioned
        latest:assetMaterializations(limit:1){partition ${entries}}
        ${partition ? 'scoped:assetMaterializations(limit:1' + (f.parts ? ',partitions:$parts' : '') + '){partition ' + entries + '}' : ''}`
        if (keys && !f.scoped) {
          // Older schemas can still resolve individual assets. Batch the aliases so a
          // scoped click never downloads lineage metadata for unrelated repositories.
          const rows: FlowRow[] = []
          for (let offset = 0; offset < keys.length; offset += 48) {
            const batch = keys.slice(offset, offset + 48)
            const args = batch.map((_, i) => `$k${i}:AssetKeyInput!`).concat(declarations)
            const nodes = batch.map(
              (_, i) =>
                `n${i}:assetNodeOrError(assetKey:$k${i}){${nodeErrors} ... on AssetNode {${fields}}}`
            )
            const values = Object.fromEntries(batch.map((k, i) => ['k' + i, k]))
            const result = await this.request<
              Record<string, FlowRow & { __typename: string; message?: string }>
            >(`query ColumnFlowLegacy(${args.join(',')}){${nodes.join(' ')}}`, {
              ...values,
              ...parts
            })
            for (const row of Object.values(result)) {
              if (row.__typename === 'AssetNode') rows.push(row)
              else if (row.__typename !== 'AssetNotFoundError')
                throw Error(this.redact(row.message || row.__typename))
            }
          }
          return rows
        }
        if (keys) declarations.push('$keys:[AssetKeyInput!]!')
        const args = declarations.length ? '(' + declarations.join(',') + ')' : ''
        const result = await this.request<{ assetNodes: FlowRow[] }>(
          `query ColumnFlow${args} {assetNodes${keys ? '(assetKeys:$keys)' : ''} {${fields}}}`,
          { ...(keys && { keys }), ...parts }
        )
        return result.assetNodes
      }
    )
  }
  async columnFlow(key: string[], column: string, partition?: string, scope?: FlowScope) {
    const cacheKey = JSON.stringify([scope?.assetId, partition])
    const cached = scope?.cache.get(cacheKey)
    let assetNodes: FlowRow[]
    const idFor = (row: FlowRow) =>
      JSON.stringify([row.repository.location.name, row.repository.name, row.assetKey.path])
    if (cached && Date.now() - cached.at < 15000) assetNodes = cached.rows
    else {
      const cone = scope && dependencyCone(scope.topology, scope.assetId)
      const rows = await this.flowRows(
        cone?.map((n) => ({ path: n.assetKey })),
        partition
      )
      const ids = cone && new Set(cone.map((n) => n.keyString))
      assetNodes = ids ? rows.filter((n) => ids.has(idFor(n))) : rows
      if (scope) {
        if (scope.cache.size >= 8) scope.cache.delete(scope.cache.keys().next().value!)
        scope.cache.set(cacheKey, { at: Date.now(), rows: assetNodes })
      }
    }
    const parents = new Map<string, Set<string>>(),
      children = new Map<string, Set<string>>()
    function edge(map: Map<string, Set<string>>, from: string, to: string) {
      const group = map.get(from) ?? new Set<string>()
      group.add(to)
      map.set(from, group)
    }
    const ids = new Map<string, string[]>()
    for (const row of assetNodes) {
      const path = JSON.stringify(row.assetKey.path)
      const owners = ids.get(path) ?? []
      owners.push(idFor(row))
      ids.set(path, owners)
    }
    for (const n of assetNodes)
      for (const row of (n.isPartitioned && partition
        ? n.scoped?.find((m) => m.partition === partition)
        : n.latest[0]
      )?.metadataEntries.find((e) => e.label === 'dagster/column_lineage')?.lineage ?? []) {
        const child = JSON.stringify([idFor(n), row.columnName])
        for (const dep of row.columnDeps) {
          const owners = ids.get(JSON.stringify(dep.assetKey.path)) ?? []
          const local = JSON.stringify([
            n.repository.location.name,
            n.repository.name,
            dep.assetKey.path
          ])
          const owner = owners.includes(local) ? local : owners.length === 1 ? owners[0] : undefined
          if (!owner) continue
          const parent = JSON.stringify([owner, dep.columnName])
          edge(parents, child, parent)
          edge(children, parent, child)
        }
      }
    const candidates = ids.get(JSON.stringify(key)) ?? []
    const startId = scope?.assetId ?? (candidates.length === 1 ? candidates[0] : undefined)
    if (!startId || !candidates.includes(startId)) return []
    const start = JSON.stringify([startId, column]),
      found = new Set([start])
    for (const map of [parents, children]) {
      const queue = new Set([start])
      for (const id of queue)
        for (const next of map.get(id) ?? []) {
          queue.add(next)
          found.add(next)
        }
    }
    return [...found].map((node) => {
      const [id, column] = JSON.parse(node) as [string, string]
      const upstreamIds = [...(parents.get(node) ?? [])]
        .filter((p) => found.has(p))
        .map((p) => JSON.parse(p)[0] as string)
      return { id, column, upstreamIds: [...new Set(upstreamIds)] }
    })
  }

  private lastGrid?: { key: string; catalog: PartitionCatalog }
  async partitionPage(
    assetKey: string[],
    cursor?: string,
    fixed?: Record<string, string>
  ): Promise<PartitionPage> {
    const key = JSON.stringify(assetKey)
    const catalog =
      cursor && this.lastGrid?.key === key
        ? this.lastGrid.catalog
        : await this.partitionCatalog(assetKey)
    this.lastGrid = { key, catalog }
    return partitionPage(catalog, cursor, fixed)
  }
  async partitionCatalog(assetKey: string[]) {
    const one =
      '__typename ... on DefaultPartitionStatuses {materializedPartitions failedPartitions materializingPartitions} ... on TimePartitionStatuses {ranges {startKey endKey status}}'
    return this.operation<PartitionCatalog>(
      `query PartitionGrid($key:AssetKeyInput!){assetNodeOrError(assetKey:$key){${nodeErrors} ... on AssetNode {
      partitionKeysByDimension {name type partitionKeys} assetPartitionStatuses {${one} ... on MultiPartitionStatuses {primaryDimensionName ranges {primaryDimStartKey primaryDimEndKey secondaryDim {${one}}}}}
    }}}`,
      { key: { path: assetKey } },
      'assetNodeOrError',
      'AssetNode'
    )
  }
  async partitionRun(assetKey: string[], partition: string) {
    const node = await this.operation<{ latestRunForPartition?: { runId: string } }>(
      `query PartitionRun($key:AssetKeyInput!,$partition:String!){assetNodeOrError(assetKey:$key){${nodeErrors} ... on AssetNode {latestRunForPartition(partition:$partition){runId}}}}`,
      { key: { path: assetKey }, partition },
      'assetNodeOrError',
      'AssetNode'
    )
    return { runId: node.latestRunForPartition?.runId }
  }
  private backfillParams(input: BackfillSelection) {
    if (!input.keys.length) throw Error('Choose at least one partition.')
    return { assetSelection: [{ path: input.assetKey }], partitionNames: [...new Set(input.keys)] }
  }
  async preview(input: BackfillSelection) {
    return (
      await this.request<{ assetBackfillPreview: BackfillPreview[] }>(
        `query BackfillPreview($params:AssetBackfillPreviewParams!){assetBackfillPreview(params:$params){assetKey {path} partitions {partitionKeys ranges {start end}}}}`,
        { params: this.backfillParams(input) }
      )
    ).assetBackfillPreview
  }
  async launchBackfill(input: BackfillSelection) {
    return this.operation<{ backfillId: string }>(
      `mutation Backfill($params:LaunchBackfillParams!){launchPartitionBackfill(backfillParams:$params){${nodeErrors} ... on LaunchBackfillSuccess {backfillId}}}`,
      { params: this.backfillParams(input) },
      'launchPartitionBackfill',
      'LaunchBackfillSuccess'
    )
  }
  async backfillStatus(id: string) {
    const result = await this.operation<{
      status: string
      assetBackfillData?: {
        assetBackfillStatuses: {
          numPartitionsTargeted?: number
          numPartitionsMaterialized?: number
          numPartitionsFailed?: number
        }[]
      }
    }>(
      `query BackfillStatus($id:String!){partitionBackfillOrError(backfillId:$id){${nodeErrors} ... on PartitionBackfill {status assetBackfillData {assetBackfillStatuses {... on AssetPartitionsStatusCounts {numPartitionsTargeted numPartitionsMaterialized numPartitionsFailed}}}}}}`,
      { id },
      'partitionBackfillOrError',
      'PartitionBackfill'
    )
    return {
      status: result.status,
      progress:
        result.assetBackfillData?.assetBackfillStatuses
          .map(
            (s) =>
              `${s.numPartitionsMaterialized ?? 0}/${s.numPartitionsTargeted ?? 0} materialized, ${s.numPartitionsFailed ?? 0} failed`
          )
          .join(' · ') ?? ''
    }
  }
}

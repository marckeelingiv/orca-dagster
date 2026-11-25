import {
  Button,
  Card,
  Checkbox,
  CodeView,
  Collapsible,
  Input,
  Select,
  Stack,
  Text,
  Row,
  Toolbar,
  plugin,
  read,
  set
} from 'orca'
import { computeVerticalDagLayout } from '../dagster/layout'
import { evaluateAssetSelection } from '../dagster/selection'
import { GraphSurface } from './graph-surface'
import * as x from './explorer-state'
import * as s from './state'
import type { ColumnFlow, GraphNode, HealthRow, RunDetail, Snapshot, SourceIndex } from '../types'

// The same views run in the native component test renderer and the Preact frame.
function memo<Args extends unknown[], T>(compute: (...args: Args) => T) {
  let previous: Args | undefined, result: T
  return (...args: Args) => {
    if (!previous || args.some((arg, i) => arg !== previous![i])) {
      result = compute(...args)
      previous = args
    }
    return result
  }
}
const layoutFor = memo((topology: GraphNode[], sources: boolean) =>
  computeVerticalDagLayout(
    sources
      ? topology
      : topology.filter((n) => n.isMaterializable !== false || n.dependencyKeys.length),
    {
      includeExternalSources: sources,
      nodeWidth: 200,
      nodeHeight: 64,
      horizontalGap: 22,
      verticalGap: 44,
      padding: 24
    }
  )
)
const catalogFor = memo((topology: GraphNode[], assets: Snapshot['assets']) => {
  const latest = new Map(assets.map((a) => [s.keyOf(a), a]))
  const nodes = topology.map((n) => ({ ...n, ...latest.get(n.keyString) }))
  return { nodes, byId: new Map(nodes.map((n) => [n.keyString, n])) }
})
const changesFor = memo((idx: SourceIndex | null) => {
  const files = new Map(idx?.files.map((f) => [f.path, f]))
  const direct = new Set(idx?.changed.flatMap((p) => files.get(p)?.assetIds ?? []))
  const ambiguous = new Set(idx?.ambiguous)
  const details = idx?.changed
    .map((p) => (files.has(p) ? 'Mapped' : ambiguous.has(p) ? 'Ambiguous' : 'Unmapped') + ' · ' + p)
    .join('\n')
  return { direct, details }
})
const impactFor = memo(
  (nodes: GraphNode[], changed: Set<string>) =>
    evaluateAssetSelection('changed*', nodes, { changed }).matchedKeys
)
const healthFor = memo((rows: HealthRow[]) => ({
  metadata: new Map(rows.map((h) => [h.id, h])),
  failed: new Set(rows.filter((h) => h.failed?.length).map((h) => h.id))
}))
const selectionFor = memo(
  (query: string, nodes: GraphNode[], changed: Set<string>, failedChecks: Set<string>) =>
    evaluateAssetSelection(query, nodes, { changed, failedChecks })
)
const chipsFor = memo((nodes: GraphNode[]) => {
  const chips = [
    { label: 'All', value: '' },
    ...['roots(*)', 'sinks(*)'].map((value) => ({ label: value, value }))
  ]
  for (const [attribute, values] of [
    ['kind', nodes.flatMap((n) => n.kinds ?? [])],
    ['group', nodes.map((n) => n.groupName)]
  ] as const) {
    for (const name of [...new Set(values)].sort()) {
      chips.push({ label: attribute + ':' + name, value: attribute + ':' + JSON.stringify(name) })
    }
  }
  return chips
})

const badgesFor = memo(
  (
    nodes: GraphNode[],
    metadata: Map<string, HealthRow>,
    direct: Set<string>,
    impact: Set<string>,
    run: RunDetail | null,
    flow: ColumnFlow[]
  ) => {
    const steps = new Map(run?.steps.map((st) => [st.key.split('[')[0], st]))
    const columns = new Map<string, string[]>()
    for (const item of flow) {
      const names = columns.get(item.id) ?? []
      names.push(item.column)
      columns.set(item.id, names)
    }
    const badges: Record<string, string> = {}
    for (const n of nodes) {
      const health = metadata.get(n.keyString)
      const step =
        run?.repositoryName === n.repositoryName && run.locationName === n.locationName
          ? n.opNames?.map((op) => steps.get(op)).find(Boolean)
          : undefined
      badges[n.keyString] = [
        direct.has(n.keyString) ? 'changed' : impact.has(n.keyString) ? 'downstream' : '',
        n.staleStatus === 'STALE' ? 'stale' : '',
        health?.freshness && !['NOT_APPLICABLE', 'FRESH', 'UNKNOWN'].includes(health.freshness)
          ? health.freshness
          : '',
        health?.failed?.length ? 'checks failed' : '',
        step?.status,
        step?.attempts && step.attempts.length > 1 ? 'attempt ' + step.attempts.length : '',
        ...(columns.get(n.keyString) ?? [])
      ]
        .filter(Boolean)
        .join(' · ')
    }
    return badges
  }
)

export function Graph({ data }: { data: Snapshot }) {
  const topology = read(x.graph),
    sources = read(x.sources)
  const layout = layoutFor(topology, sources)
  const { nodes, byId } = catalogFor(topology, data.assets)
  const query = read(x.query),
    idx = read(x.index)
  const { direct, details } = changesFor(idx)
  const impact = impactFor(topology, direct)
  const { metadata, failed } = healthFor(read(x.health))
  const evalResult = selectionFor(query, nodes, direct, failed)
  const matched = evalResult.matchedKeys
  const stale = nodes.filter((n) => n.staleStatus === 'STALE').map((n) => n.keyString)
  const file = idx?.files.find((f) => f.path === read(x.file))
  const find = (id: string) => {
    const n = byId.get(id)
    if (n) void s.selectAsset(n, false)
  }
  const badges = badgesFor(nodes, metadata, direct, impact, read(s.detail), read(x.flow))
  const groups = new Map<string, GraphNode[]>()
  for (const id of read(x.review)) {
    const n = byId.get(id)
    if (!n) continue
    const group = JSON.stringify([
      n.locationName,
      n.repositoryName,
      n.jobNames.find((j) => j.startsWith('__ASSET_JOB')),
      n.isPartitioned ? n.keyString : ''
    ])
    const members = groups.get(group) ?? []
    members.push(n)
    groups.set(group, members)
  }
  function review(ids: Iterable<string>) {
    set(
      x.review,
      [...ids].filter((id) => byId.get(id)?.isMaterializable !== false)
    )
    set(x.completed, {})
  }
  const focused = byId.get(read(s.assetId))
  const latest = focused?.lastRunId
  const actions: { label: string; ids: Iterable<string>; enabled: boolean }[] = [
    { label: 'Materialize Changed', ids: direct, enabled: direct.size > 0 },
    { label: 'Materialize Changed + Downstream', ids: impact, enabled: impact.size > 0 },
    { label: 'Materialize Stale', ids: stale, enabled: stale.length > 0 }
  ]
  return (
    <Stack>
      <Toolbar label="Asset selection">
        <Input
          label="Asset selection"
          labelHidden
          placeholder="Select assets by key or expression"
          value={read(x.queryDraft)}
          maxLength={2000}
          invalid={!!evalResult.error}
          onChange={(v) => x.changeQuery(v, true)}
          onSubmit={(v) => x.changeQuery(v)}
        />
        <Button
          tone="accent"
          variant="solid"
          icon="play"
          disabled={
            read(x.queryDraft) !== query ||
            !evalResult.matchedAssets.some((n) => n.isMaterializable !== false)
          }
          onPress={() => review(matched)}
        >
          Materialize
        </Button>
      </Toolbar>
      <Toolbar label="Selection filters">
        {chipsFor(topology).map((chip) => (
          <Button
            key={chip.label}
            tooltip={chip.label}
            tone={query === chip.value ? 'accent' : 'neutral'}
            onPress={() => x.changeQuery(chip.value)}
          >
            {chip.label}
          </Button>
        ))}
        <Checkbox label="Sources" checked={sources} onChange={(v) => set(x.sources, v)} />
      </Toolbar>
      {query && evalResult.error && <Text tone="danger">{evalResult.error}</Text>}
      <Toolbar label="Graph tools">
        <Collapsible
          title={
            'Git diff · ' +
            (idx?.gitError
              ? 'Unavailable'
              : idx
                ? idx.changed.length + ' changed · ' + direct.size + ' assets'
                : read(x.indexing)
                  ? 'Indexing…'
                  : 'No index')
          }
          defaultOpen={read(x.pickerOpen)}
        >
          <Stack>
            {idx?.gitError && <Text>{idx.gitError}</Text>}
            {details && <CodeView code={details} language="text" wrap maxHeight="sm" />}
            <Button onPress={() => x.loadGraph(true).catch(s.fail)}>
              Refresh graph and Git changes
            </Button>
            {idx && (
              <Input
                type="search"
                label="Find source file"
                value={read(x.fileSearch)}
                onChange={(v) => set(x.fileSearch, v)}
              />
            )}
            {idx && (
              <Select
                label="Show asset for source file"
                value={read(x.file)}
                options={[
                  { value: '', label: 'Choose source file' },
                  ...idx.files
                    .filter((f) => f.path.toLowerCase().includes(read(x.fileSearch).toLowerCase()))
                    .map((f) => ({ value: f.path, label: f.path }))
                ]}
                onChange={(v) => {
                  set(x.file, v)
                  const ids = idx.files.find((f) => f.path === v)?.assetIds
                  if (ids?.length === 1) find(ids[0])
                }}
              />
            )}
            {file && file.assetIds.length > 1 && (
              <Select
                label="Assets in source file"
                value={read(s.assetId)}
                options={file.assetIds.map((id) => ({
                  value: id,
                  label: byId.get(id)?.assetKey.join('/') ?? id
                }))}
                onChange={find}
              />
            )}
            <Row wrap>
              {actions.map(({ label, ids, enabled }) => (
                <Button key={label} disabled={!enabled} onPress={() => review(ids)}>
                  {label}
                </Button>
              ))}
            </Row>
          </Stack>
        </Collapsible>
        <Select
          label="Run overlay"
          labelHidden
          value={read(s.runId)}
          options={[
            { value: '', label: 'Run overlay' },
            ...data.runs.map((r) => ({
              value: r.runId,
              label: r.pipelineName + ' · ' + r.status + ' · ' + r.runId.slice(0, 8)
            }))
          ]}
          onChange={(id) => s.showRun(id)}
        />
      </Toolbar>
      {read(x.lineage).length > 0 && (
        <Button
          onPress={() => {
            set(x.lineage, [])
            set(x.flow, [])
          }}
        >
          Clear column lineage
        </Button>
      )}
      <GraphSurface
        layout={layout}
        badges={badges}
        checks={metadata}
        assets={byId}
        selected={read(s.assetId)}
        edgeHighlights={
          read(x.lineage).length
            ? new Set(
                read(x.flow).flatMap((f) => f.upstreamIds.map((id) => JSON.stringify([id, f.id])))
              )
            : undefined
        }
        highlighted={read(x.lineage).length ? new Set(read(x.lineage)) : matched}
        select={find}
      />
      {groups.size > 0 && (
        <Stack>
          <Toolbar label="Materialization review">
            <Text>Review {read(x.review).length} selected</Text>
            <Button onPress={() => review([])}>Close review</Button>
          </Toolbar>
          {[...groups.entries()].map(([id, members]) => {
            const first = members[0],
              partitioned = first.isPartitioned,
              done = read(x.completed)[id]
            const page = read(x.pages)
            const keys = page?.rows.map((r) => r.key) ?? read(s.partitions)?.keys ?? []
            const pickPage = async (fixed?: Record<string, string>, more = false) => {
              await x.loadPage(first.assetKey, more, fixed)
              if (read(s.assetId) === first.keyString) {
                const keys = read(x.pages)?.rows.map((r) => r.key) ?? []
                if (!keys.includes(read(s.partition))) set(s.partition, keys[0] ?? '')
              }
            }
            return (
              <Card key={id} title={'Review · ' + first.repositoryName}>
                <Stack>
                  <Text>{members.map((n) => n.assetKey.join('/')).join(', ')}</Text>
                  {partitioned && (
                    <Button
                      onPress={() =>
                        s.action(async () => {
                          await s.selectAsset(first, false)
                          if (read(s.assetId) === first.keyString) await pickPage()
                        }, false)
                      }
                    >
                      Choose partition
                    </Button>
                  )}
                  {partitioned && read(s.assetId) === first.keyString && (
                    <Stack>
                      {page?.dimensions?.map((d) => (
                        <Select
                          key={d.name}
                          label={d.name}
                          value={page.fixed?.[d.name]}
                          options={s.options(d.partitionKeys)}
                          onChange={(v) =>
                            s.action(() => pickPage({ ...page.fixed, [d.name]: v }), false)
                          }
                        />
                      ))}
                      <Select
                        label="Partition to launch"
                        options={s.options(keys)}
                        value={read(s.partition)}
                        onChange={(v) => {
                          set(x.columnHistory, null)
                          set(x.preview, null)
                          set(s.partition, v)
                        }}
                      />
                      {page?.hasMore && (
                        <Button onPress={() => s.action(() => pickPage(page.fixed, true), false)}>
                          More partition keys
                        </Button>
                      )}
                    </Stack>
                  )}
                  {done ? (
                    <Text>{'Launched ' + done}</Text>
                  ) : (
                    <Button
                      disabled={
                        read(s.busy) ||
                        (partitioned &&
                          (read(s.assetId) !== first.keyString ||
                            !keys.includes(read(s.partition))))
                      }
                      onPress={() =>
                        s.action(async () => {
                          const valid = x.guarded()
                          const r = await plugin.request(
                            'dagster.materialize',
                            {
                              ...s.scope(),
                              assetKeys: members.map((n) => n.assetKey),
                              ...(partitioned ? { partitionKey: read(s.partition) } : {})
                            },
                            s.requestOptions()
                          )
                          if (valid()) {
                            set(x.completed, { ...read(x.completed), [id]: r.runId })
                            set(s.launchedRun, r.runId)
                          }
                        })
                      }
                    >
                      Launch reviewed group
                    </Button>
                  )}
                </Stack>
              </Card>
            )
          })}
        </Stack>
      )}
      {focused && (
        <Card title={focused.assetKey.join('/')}>
          <Stack>
            <Text>
              {[
                badges[focused.keyString],
                ...(metadata.get(focused.keyString)?.causes ?? []),
                ...(metadata.get(focused.keyString)?.failed ?? [])
              ]
                .filter(Boolean)
                .join(' · ')}
            </Text>
            {[
              ...layout.edges.filter(
                (e) => e.from === focused.keyString || e.to === focused.keyString
              )
            ].map((e) => {
              const up = e.to === focused.keyString,
                other = up ? e.from : e.to
              return (
                <Button key={e.id} disabled={!byId.has(other)} onPress={() => find(other)}>
                  {(up ? 'Upstream · ' : 'Downstream · ') +
                    (byId.get(other)?.assetKey.join('/') ?? other)}
                </Button>
              )
            })}
            <Button
              onPress={() => {
                x.clearAsset()
                set(s.assetId, '')
              }}
            >
              Close details
            </Button>
            <Button
              onPress={() => {
                set(s.tab, 'assets')
              }}
            >
              Inspect asset
            </Button>
            {latest && (
              <Button
                onPress={() => {
                  set(s.tab, 'runs')
                  void s.showRun(latest)
                }}
              >
                Latest run
              </Button>
            )}
          </Stack>
        </Card>
      )}
    </Stack>
  )
}

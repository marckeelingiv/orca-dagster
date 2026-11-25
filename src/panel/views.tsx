import { Waterfall } from './graph-surface'
import { AssetDetails } from './asset-details'
import * as x from './explorer-state'
import {
  Badge,
  Button,
  Callout,
  Card,
  CodeView,
  Collapsible,
  Heading,
  Input,
  KeyValue,
  Link,
  List,
  ListItem,
  RelativeTime,
  Row,
  Select,
  Skeleton,
  Stack,
  Switch,
  Table,
  Text,
  Textarea,
  Toolbar,
  Tree,
  plugin,
  read,
  set,
  shell,
  type TreeItem
} from 'orca'
import { dagsterUrl } from '../links'
import {
  isActiveRun,
  type JobSummary,
  type ScheduleSummary,
  type SensorSummary,
  type RunFilter,
  type Snapshot,
  type SourceResult,
  type SourceInput
} from '../types'
import * as s from './state'
import { basket, selectionFor, toggleAsset } from './selection'

const { clip, scope, action, requestOptions } = s
function external(snapshot: Snapshot, target: Parameters<typeof dagsterUrl>[1]) {
  if (!snapshot.uiUrl) return ''
  try {
    return dagsterUrl(snapshot.uiUrl, target)
  } catch {
    return ''
  }
}
function Open({ url }: { url: string }) {
  return (
    <Button
      icon="open-external"
      variant="ghost"
      disabled={!url}
      onPress={() => action(() => shell.openExternal(url), false)}
    >
      Open in Dagster UI
    </Button>
  )
}
function Source({ result, reopen }: { result: SourceResult | null; reopen: () => unknown }) {
  if (!result) return null
  if (result.resolvedPath)
    return (
      <Link onPress={reopen}>
        {clip(result.resolvedPath, 240) + (result.lineNumber ? ':' + result.lineNumber : '')}
      </Link>
    )
  let url = ''
  try {
    const candidate = new URL(result.externalUrl ?? '')
    if (
      ['http:', 'https:'].includes(candidate.protocol) &&
      !candidate.username &&
      !candidate.password
    )
      url = candidate.href
  } catch {}
  return (
    <Stack>
      <Text muted>{clip(result.reason || 'Source is available remotely.', 240)}</Text>
      {url && (
        <Link onPress={() => action(() => shell.openExternal(url), false)}>
          Open remote definition
        </Link>
      )}
    </Stack>
  )
}
function materialized(at: number | undefined, synced: number) {
  if (at === undefined) return 'Never materialized'
  const minutes = Math.max(0, Math.floor((synced - at) / 60000))
  return (
    'Materialized ' +
    (minutes < 1
      ? 'just now'
      : minutes < 60
        ? minutes + 'm ago'
        : minutes < 1440
          ? Math.floor(minutes / 60) + 'h ago'
          : Math.floor(minutes / 1440) + 'd ago')
  )
}
const health = {
  success: ['check', 'Healthy'],
  danger: ['error', 'Failed'],
  warning: ['warning', 'Stale or in progress'],
  neutral: ['layers', 'Unknown']
} as const
export function Assets({
  data,
  partitionsOnly = false
}: {
  data: Snapshot
  partitionsOnly?: boolean
}) {
  const searchField = partitionsOnly ? s.partitionSearch : s.assetSearch
  const search = read(searchField).toLowerCase()
  const lineage = external(data, { kind: 'lineage', expression: read(s.expression) })
  const byKey = new Map(data.assets.map((asset) => [s.keyOf(asset), asset]))
  for (const node of read(x.graph)) if (!byKey.has(node.keyString)) byKey.set(node.keyString, node)
  const catalog = [...byKey.values()]
  const assets = catalog
    .filter((asset) => !partitionsOnly || asset.isPartitioned)
    .filter((a) =>
      [a.assetKey.join('/'), a.groupName, a.description].some((v) =>
        v?.toLowerCase().includes(search)
      )
    )
    .slice(0, 80)
  const locations = new Map<string, Map<string, TreeItem[]>>()
  const lookup = new Map<string, (typeof assets)[number]>()
  for (const a of assets) {
    if (!locations.has(a.locationName)) locations.set(a.locationName, new Map())
    const groups = locations.get(a.locationName)!
    if (!groups.has(a.groupName)) groups.set(a.groupName, [])
    const key = s.keyOf(a)
    lookup.set(key, a)
    groups.get(a.groupName)!.push({
      key,
      label: clip(a.assetKey.join('/')),
      description:
        (a.isPartitioned ? 'Partitioned · ' : '') +
        health[a.status][1] +
        ' · ' +
        materialized(a.lastMaterializedAt, data.syncedAt),
      icon: health[a.status][0]
    })
  }
  const items = [...locations].map(([name, groups]) => ({
    key: JSON.stringify([name]),
    label: clip(name),
    children: [...groups].map(([group, children]) => ({
      key: JSON.stringify([name, group]),
      label: clip(group),
      children
    }))
  }))
  const a = catalog.find(
    (a) => (!partitionsOnly || a.isPartitioned) && s.keyOf(a) === read(s.assetId)
  )
  const parts = read(s.partitions),
    source = read(s.source)
  const group = a && selectionFor(catalog, a, parts)
  const selectedPartition = group?.keys.includes(read(s.partition))
    ? read(s.partition)
    : group?.keys[0]
  const saved = read(basket)
  const included = !!a && saved.some((e) => e.id === s.keyOf(a))
  const launchJob =
    a?.jobNames.find(
      (j) =>
        j.startsWith('__ASSET_JOB') && group?.selected.every((item) => item.jobNames.includes(j))
    ) ?? (group?.selected.length === 1 ? a?.jobNames[0] : undefined)
  const launchpad =
    a &&
    launchJob &&
    group?.selected.every(
      (item) => item.locationName === a.locationName && item.repositoryName === a.repositoryName
    ) &&
    external(data, {
      kind: 'launchpad',
      locationName: a.locationName,
      repositoryName: a.repositoryName,
      jobName: launchJob,
      assetKeys: group!.selected.map((item) => item.assetKey)
    })
  return (
    <Stack>
      {!partitionsOnly && (
        <Stack>
          <Input
            label="Dagster asset selection"
            value={read(s.expression)}
            placeholder={'key:"raw_orders"+'}
            maxLength={2000}
            onChange={(v) => set(s.expression, v)}
          />
          <Button
            icon="open-external"
            disabled={!lineage}
            onPress={() => action(() => shell.openExternal(lineage), false)}
          >
            {read(s.expression).trim() ? 'Review selection in Dagster DAG' : 'Open Dagster DAG'}
          </Button>
        </Stack>
      )}
      <Input
        type="search"
        label={partitionsOnly ? 'Search partitioned assets' : 'Search assets'}
        value={read(searchField)}
        maxLength={120}
        onChange={(v) => set(searchField, v)}
      />
      <Tree
        label={partitionsOnly ? 'Partitioned assets' : 'Assets'}
        maxHeight="md"
        items={items}
        defaultExpanded={items.flatMap((l) => [l.key, ...l.children.map((g) => g.key)])}
        selected={a ? s.keyOf(a) : undefined}
        onSelect={({ key }) => {
          const item = lookup.get(key)
          if (item) void s.selectAsset(item, !partitionsOnly)
        }}
      />
      {partitionsOnly && !assets.length && (
        <Text muted>
          {search ? 'No matching partitioned assets.' : 'This catalog has no partitioned assets.'}
        </Text>
      )}
      {partitionsOnly && !!assets.length && !a && (
        <Text muted>Select an asset to see its partitions.</Text>
      )}
      {assets.length === 80 && <Text muted>Showing 80 assets. Search to narrow the list.</Text>}
      {a && (
        <Card title={clip(a.assetKey.join('/'))}>
          <Stack>
            <AssetDetails asset={a} />
            <Source
              result={source}
              reopen={() =>
                action(
                  () =>
                    plugin.request(
                      'source.open',
                      { ...scope(), ...s.asSource(a) },
                      requestOptions()
                    ),
                  false
                )
              }
            />
            <Button
              disabled={!included && (saved.length >= 24 || !!group?.reason)}
              onPress={() => toggleAsset(a, parts)}
            >
              {included ? 'Remove from selection' : 'Keep in selection'}
            </Button>
            {!!saved.length && (
              <Stack>
                <Text>
                  {'Materialize together: ' +
                    clip(group!.selected.map((item) => item.assetKey.join('/')).join(', '), 600)}
                </Text>
                <Button onPress={() => set(basket, [])}>Clear selection</Button>
              </Stack>
            )}
            {group?.reason && <Text tone="warning">{group.reason}</Text>}
            {a.lastMaterializedAt && (
              <Text>
                Last materialized <RelativeTime at={a.lastMaterializedAt} />
              </Text>
            )}
            {a.isPartitioned && data.uiUrl && (
              <Link
                onPress={() =>
                  action(
                    () =>
                      shell.openExternal(
                        external(data, { kind: 'partitions', assetKey: a.assetKey })
                      ),
                    false
                  )
                }
              >
                View all partitions in Dagster
              </Link>
            )}
            {parts?.kind === 'single' && !!group?.keys.length && (
              <Select
                label="Partition"
                options={s.options(group!.keys)}
                value={selectedPartition}
                onChange={(v) => {
                  x.clearAsset()
                  set(s.partition, v)
                }}
              />
            )}
            {parts?.kind === 'multi' && (
              <Callout title="Choose partitions in Dagster">
                {launchpad ? (
                  <Link onPress={() => action(() => shell.openExternal(launchpad), false)}>
                    Open Launchpad
                  </Link>
                ) : (
                  <Text>Launchpad unavailable</Text>
                )}
              </Callout>
            )}
            <Collapsible title="Advanced Config">
              <Textarea
                label="Run config YAML or JSON"
                value={read(s.config)}
                maxLength={3000}
                onChange={(v) => set(s.config, v)}
              />
              <Textarea
                label="Tags (JSON object)"
                value={read(s.tags)}
                maxLength={1000}
                onChange={(v) => set(s.tags, v)}
              />
            </Collapsible>
            <Row wrap>
              <Button
                icon="play"
                tone="accent"
                disabled={read(s.busy) || !!group?.reason}
                onPress={() =>
                  s.launch(async () => {
                    const tags = read(s.tags).trim() ? JSON.parse(read(s.tags)) : undefined
                    if (
                      tags !== undefined &&
                      (!tags ||
                        typeof tags !== 'object' ||
                        Array.isArray(tags) ||
                        Object.values(tags).some((v) => typeof v !== 'string'))
                    )
                      throw Error('Tags must be an object with string values.')
                    return plugin.request(
                      'dagster.materialize',
                      {
                        ...scope(),
                        assetKeys: group!.selected.map((item) => item.assetKey),
                        ...(selectedPartition && {
                          partitionKey: selectedPartition
                        }),
                        ...(read(s.config) && {
                          runConfigYaml: read(s.config)
                        }),
                        ...(tags && { tags })
                      },
                      requestOptions()
                    )
                  })
                }
              >
                Materialize
              </Button>
              <Open url={external(data, s.asSource(a))} />
            </Row>
          </Stack>
        </Card>
      )}
    </Stack>
  )
}
export function Automation({ data }: { data: Snapshot }) {
  const query = read(s.autoSearch).toLowerCase()
  const matches = (a: JobSummary) => a.name.toLowerCase().includes(query)
  return (
    <Stack>
      <Input
        label="Search jobs, schedules, sensors"
        type="search"
        value={read(s.autoSearch)}
        maxLength={120}
        onChange={(v) => set(s.autoSearch, v)}
      />
      {(['jobs', 'schedules', 'sensors'] as const).map((section) => {
        const title = {
          jobs: 'Jobs',
          schedules: 'Schedules',
          sensors: 'Sensors'
        }[section]
        const rows = (data[section] as Array<JobSummary | ScheduleSummary | SensorSummary>).filter(
          matches
        )
        return (
          <Collapsible key={section} title={title + ' (' + rows.length + ')'} defaultOpen>
            <List label={title}>
              {rows.slice(0, 35).map((item) => {
                const { locationName, repositoryName, name } = item
                const kind = {
                  jobs: 'job',
                  schedules: 'schedule',
                  sensors: 'sensor'
                }[section] as SourceInput['kind']
                const identity = JSON.stringify([kind, locationName, repositoryName, name])
                return (
                  <ListItem
                    key={identity}
                    current={identity === read(s.automationSelection)}
                    description={clip(locationName)}
                  >
                    <Button
                      variant="ghost"
                      onPress={() =>
                        s.openAutomation(identity, {
                          kind,
                          name,
                          locationName,
                          repositoryName
                        })
                      }
                    >
                      {clip(name)}
                    </Button>
                    {identity === read(s.automationSelection) && (
                      <Source
                        result={read(s.automationSource)}
                        reopen={() =>
                          s.openAutomation(identity, {
                            kind,
                            name,
                            locationName,
                            repositoryName
                          })
                        }
                      />
                    )}
                    {kind === 'job' && identity === read(s.automationSelection) && (
                      <Collapsible title="Job config">
                        <Textarea
                          label="Job run config YAML or JSON"
                          maxLength={3000}
                          value={read(s.jobConfig)}
                          onChange={(value) => set(s.jobConfig, value)}
                        />
                      </Collapsible>
                    )}
                    {'status' in item ? (
                      <Stack>
                        <Switch
                          label={clip(name)}
                          checked={item.status === 'RUNNING'}
                          disabled={read(s.busy)}
                          onChange={(running) =>
                            action(() =>
                              section === 'schedules'
                                ? plugin.request(
                                    'dagster.toggleSchedule',
                                    {
                                      ...scope(),
                                      locationName,
                                      repositoryName,
                                      scheduleName: name,
                                      running
                                    },
                                    requestOptions()
                                  )
                                : plugin.request(
                                    'dagster.toggleSensor',
                                    {
                                      ...scope(),
                                      locationName,
                                      repositoryName,
                                      sensorName: name,
                                      running
                                    },
                                    requestOptions()
                                  )
                            )
                          }
                        />
                        <Text muted>
                          {'cronSchedule' in item
                            ? clip(item.cronSchedule)
                            : 'Every ' + (item.minIntervalSeconds ?? 30) + 's'}
                        </Text>
                        {item.tick && (
                          <Text>
                            Last tick {clip(item.tick.status)}{' '}
                            <RelativeTime at={item.tick.timestamp} />
                          </Text>
                        )}
                        {item.tick?.error && (
                          <Callout tone="danger" title="Tick failed">
                            <Text>{clip(item.tick.error, 200)}</Text>
                          </Callout>
                        )}
                      </Stack>
                    ) : (
                      <Button
                        icon="play"
                        disabled={read(s.busy)}
                        onPress={() =>
                          s.launch(() =>
                            plugin.request(
                              'dagster.launchJob',
                              {
                                ...scope(),
                                locationName,
                                repositoryName,
                                jobName: name,
                                ...(identity === read(s.automationSelection) &&
                                read(s.jobConfig).trim()
                                  ? { runConfigYaml: read(s.jobConfig) }
                                  : {})
                              },
                              requestOptions()
                            )
                          )
                        }
                      >
                        Launch
                      </Button>
                    )}
                  </ListItem>
                )
              })}
            </List>
            {rows.length > 35 && <Text muted>Search to see more results.</Text>}
          </Collapsible>
        )
      })}
    </Stack>
  )
}
export function Runs({ data }: { data: Snapshot }) {
  const id = read(s.runId),
    run = read(s.detail),
    disabled = read(s.busy)
  if (id)
    return (
      <Stack>
        <Button icon="collapse" onPress={() => s.showRun('')}>
          Back to Runs
        </Button>
        {run ? (
          <Stack>
            <Row>
              <Heading level={3}>{'Run ' + clip(id, 8)}</Heading>
              <Badge tone={run.status === 'FAILURE' ? 'danger' : 'neutral'}>
                {clip(run.status)}
              </Badge>
            </Row>
            <KeyValue
              items={[
                { label: 'Target', value: clip(run.pipelineName) },
                {
                  label: 'Duration',
                  value:
                    run.startTime && run.endTime
                      ? ((run.endTime - run.startTime) / 1000).toFixed(1) + 's'
                      : '—'
                }
              ]}
            />
            {run.parentRunId && (
              <Button onPress={() => s.showRun(run.parentRunId!)}>
                {'Parent run ' + s.clip(run.parentRunId, 12)}
              </Button>
            )}
            {run.startTime && (
              <Text>
                Started <RelativeTime at={run.startTime} />
              </Text>
            )}
            {run.source?.resolvedPath && (
              <Link
                onPress={() =>
                  action(
                    () =>
                      plugin.request(
                        'source.open',
                        {
                          ...scope(),
                          kind: 'job',
                          name: run.pipelineName,
                          ...(run.locationName && {
                            locationName: run.locationName
                          }),
                          ...(run.repositoryName && {
                            repositoryName: run.repositoryName
                          }),
                          codeReferences: [
                            {
                              filePath: run.source!.resolvedPath!,
                              ...(run.source!.lineNumber && {
                                lineNumber: run.source!.lineNumber
                              })
                            }
                          ]
                        },
                        requestOptions()
                      ),
                    false
                  )
                }
              >
                {clip(run.source.resolvedPath, 240) +
                  (run.source.lineNumber ? ':' + run.source.lineNumber : '')}
              </Link>
            )}
            {run.agentFix && (
              <Callout title={run.agentFix === 'edited' ? 'Source file changed' : 'Sent to agent'}>
                <Text>
                  {run.agentFix === 'edited'
                    ? 'Use Re-execute From Failure to verify the edit.'
                    : 'Return to this run to verify the fix after editing.'}
                </Text>
              </Callout>
            )}
            <Toolbar label="Run actions">
              <Button
                disabled={disabled}
                onPress={() =>
                  s.launch(() =>
                    plugin.request(
                      'dagster.reexecuteRun',
                      { ...scope(), runId: id, strategy: 'ALL_STEPS' },
                      requestOptions()
                    )
                  )
                }
              >
                Re-execute
              </Button>
              {run.status === 'FAILURE' && (
                <Button
                  disabled={disabled}
                  onPress={() =>
                    s.launch(() =>
                      plugin.request(
                        'dagster.reexecuteRun',
                        { ...scope(), runId: id, strategy: 'FROM_FAILURE' },
                        requestOptions()
                      )
                    )
                  }
                >
                  Re-execute From Failure
                </Button>
              )}
              {isActiveRun(run.status) && (
                <Button
                  disabled={disabled}
                  onPress={() =>
                    action(() =>
                      plugin.request(
                        'dagster.terminateRun',
                        { ...scope(), runId: id },
                        requestOptions()
                      )
                    )
                  }
                >
                  Terminate
                </Button>
              )}
              <Open url={external(data, { kind: 'run', runId: id })} />
            </Toolbar>
            {run.status === 'FAILURE' && (
              <Stack>
                <Button
                  icon="bot"
                  variant="solid"
                  tone="accent"
                  disabled={disabled}
                  onPress={() =>
                    action(
                      () =>
                        plugin.request(
                          'agent.sendRunError',
                          {
                            ...scope(),
                            runId: id,
                            mode: 'launch',
                            ...(read(x.compareKey)
                              ? { assetKey: JSON.parse(read(x.compareKey)) }
                              : {})
                          },
                          requestOptions()
                        ),
                      false
                    )
                  }
                >
                  Send Error to Agent
                </Button>
                {run.assetKeys.length > 0 && (
                  <Stack>
                    <Select
                      label="Asset for comparison"
                      options={run.assetKeys.map((k) => ({
                        value: JSON.stringify(k),
                        label: k.join('/')
                      }))}
                      value={read(x.compareKey) || JSON.stringify(run.assetKeys[0])}
                      onChange={(v) => {
                        set(x.compareKey, v)
                        set(x.comparison, '')
                      }}
                    />
                    <Button
                      onPress={() =>
                        action(async () => {
                          const valid = x.guarded(),
                            assetKey = read(x.compareKey)
                          const result = await plugin.request(
                            'dagster.compareFailure',
                            {
                              ...scope(),
                              runId: id,
                              assetKey: JSON.parse(assetKey || JSON.stringify(run.assetKeys[0]))
                            },
                            requestOptions()
                          )
                          if (valid() && read(s.runId) === id && read(x.compareKey) === assetKey)
                            set(x.comparison, result.comparison)
                        }, false)
                      }
                    >
                      Compare with last success
                    </Button>
                    {read(x.comparison) && (
                      <CodeView language="text" code={read(x.comparison)} wrap />
                    )}
                  </Stack>
                )}
                {!!read(s.targets).length && (
                  <Row>
                    <Select
                      label="Agent terminal"
                      value={read(s.destination)}
                      options={read(s.targets).map((t) => ({
                        value: t.terminalId,
                        label: clip(t.title)
                      }))}
                      onChange={(v) => set(s.destination, v)}
                    />
                    <Button
                      disabled={disabled}
                      onPress={() =>
                        action(
                          () =>
                            plugin.request(
                              'agent.sendRunError',
                              {
                                ...scope(),
                                runId: id,
                                mode: 'existing',
                                ...(read(x.compareKey)
                                  ? { assetKey: JSON.parse(read(x.compareKey)) }
                                  : {}),
                                terminalId: read(s.destination)
                              },
                              requestOptions()
                            ),
                          false
                        )
                      }
                    >
                      Send to Existing Agent
                    </Button>
                  </Row>
                )}
              </Stack>
            )}
            <Collapsible title={'Steps (' + run.steps.length + ')'}>
              <Waterfall run={run} now={data.syncedAt} />
            </Collapsible>
            <Collapsible title="Logs" defaultOpen>
              <Select
                label="Log level"
                value={read(s.errorsOnly) ? 'Errors Only' : 'All'}
                options={s.options(['Errors Only', 'All'])}
                onChange={(v) => {
                  set(s.errorsOnly, v === 'Errors Only')
                  void s.showRun(id, false)
                }}
              />
              <CodeView
                code={clip(
                  [
                    run.error,
                    ...run.logs
                      .slice(-200)
                      .filter(
                        (l) =>
                          !read(s.errorsOnly) || l.error || ['ERROR', 'CRITICAL'].includes(l.level)
                      )
                      .map((l) =>
                        [
                          new Date(l.timestamp).toLocaleTimeString(),
                          l.level,
                          l.stepKey,
                          clip(l.message, 500),
                          clip(l.error, 750)
                        ]
                          .filter(Boolean)
                          .join(' ')
                      )
                  ]
                    .filter(Boolean)
                    .join('\n')
                    .split('\n')
                    .slice(0, 200)
                    .join('\n'),
                  3800
                )}
                label="Run logs"
                language="text"
                lineNumbers
                wrap
                maxHeight="md"
              />
            </Collapsible>
          </Stack>
        ) : (
          <Skeleton lines={4} />
        )}
      </Stack>
    )
  const query = read(s.runSearch).toLowerCase(),
    page = read(s.page)
  const runs = (page?.runs ?? (read(s.filter) === 'All' ? data.runs : []))
    .filter((r) => (r.runId + ' ' + r.pipelineName).toLowerCase().includes(query))
    .slice(0, 200)
  return (
    <Stack>
      <Select
        label="Run status"
        value={read(s.filter)}
        options={s.options(['All', 'Active', 'Success', 'Failure', 'Canceled'])}
        onChange={(v) => {
          set(s.filter, v as RunFilter)
          void s.loadPage()
        }}
      />
      <Input
        label="Search run ID or job"
        type="search"
        value={read(s.runSearch)}
        maxLength={120}
        onChange={(v) => set(s.runSearch, v)}
      />
      <Table
        label="Runs"
        columns={['Run', 'Target', 'Status', 'Started'].map((label) => ({
          key: label,
          label
        }))}
        rows={runs.map((r) => ({
          key: r.runId,
          cells: {
            Run: clip(r.runId, 8),
            Target: clip(r.pipelineName),
            Status:
              ({ FAILURE: '🔴', SUCCESS: '🟢', CANCELED: '⚪' }[r.status] ??
                (isActiveRun(r.status) ? '🔵' : '⚪')) +
              ' ' +
              clip(r.status),
            Started: r.startTime ? { type: 'relative', at: r.startTime } : '—'
          }
        }))}
        selectionMode="single"
        emptyText="No matching runs"
        onSelectionChange={({ keys }) => {
          if (keys[0]) void s.showRun(keys[0])
        }}
        onRowAction={({ key }) => s.showRun(key)}
        hasMore={page?.hasMore}
        onLoadMore={() => s.loadPage(true)}
      />
    </Stack>
  )
}

import { PartitionGrid } from './graph-surface'
import { Button, Card, Input, Row, Select, Stack, Text, plugin, read, set } from 'orca'
import type { AssetSummary } from '../types'
import * as s from './state'
import * as x from './explorer-state'

export function AssetDetails({ asset }: { asset: AssetSummary }) {
  const key = asset.assetKey,
    history = read(x.columnHistory),
    page = read(x.pages),
    preview = read(x.preview)
  const partition = read(s.partition)
  async function loadColumns() {
    const valid = x.guarded(true)
    const h = await plugin.request(
      'dagster.columns',
      { ...s.scope(), assetKey: key, ...(partition ? { partition } : {}) },
      s.requestOptions()
    )
    if (valid()) set(x.columnHistory, h)
  }
  return (
    <Stack>
      <Button onPress={() => s.action(loadColumns, false)}>Inspect column schema</Button>
      {history && (
        <Card title="Columns">
          <Stack>
            {!history.columns && <Text>Column schema unavailable</Text>}
            {history.columns?.map((c) => {
              const prior = history.previous?.find((p) => p.name === c.name)
              const change = history.previous
                ? !prior
                  ? ' · added'
                  : prior.type !== c.type
                    ? ' · was ' + prior.type
                    : ''
                : ''
              return (
                <Button
                  key={c.name}
                  onPress={() =>
                    s.action(async () => {
                      const valid = x.guarded(true)
                      if (!read(x.graph).length) await x.loadGraph()
                      if (!valid()) return
                      const token = x.graphToken()
                      const lineage = await plugin.request(
                        'dagster.columnFlow',
                        {
                          ...s.scope(),
                          assetKey: key,
                          assetId: s.keyOf(asset),
                          ...(token && { token }),
                          column: c.name,
                          ...(partition ? { partition } : {})
                        },
                        s.requestOptions()
                      )
                      if (!valid()) return
                      set(x.column, c.name)
                      set(
                        x.lineage,
                        lineage.map((l) => l.id)
                      )
                      set(x.flow, lineage)
                      set(s.tab, 'dag')
                    }, false)
                  }
                >
                  {c.name + ' · ' + c.type + change + (c.description ? ' · ' + c.description : '')}
                </Button>
              )
            })}
            {history.columns &&
              history.previous
                ?.filter((p) => !history.columns!.some((c) => c.name === p.name))
                .map((c) => <Text key={c.name}>{c.name + ' · removed'}</Text>)}
            {history.columns && !history.previous && <Text>Previous schema unavailable</Text>}
          </Stack>
        </Card>
      )}
      {asset.isPartitioned && (
        <Card title="Partition status">
          <Stack>
            <Button onPress={() => s.action(() => x.loadPage(key), false)}>
              Load partition grid
            </Button>
            {page?.dimensions?.map((d) => (
              <Select
                key={d.name}
                label={d.name}
                options={s.options(d.partitionKeys)}
                value={page.fixed?.[d.name]}
                onChange={(v) =>
                  s.action(() => x.loadPage(key, false, { ...page.fixed, [d.name]: v }), false)
                }
              />
            ))}
            {page && (
              <PartitionGrid
                page={page}
                select={(partitionKey, status) =>
                  s.action(async () => {
                    const valid = x.guarded(true)
                    const r = await plugin.request(
                      'dagster.partitionRun',
                      { ...s.scope(), assetKey: key, partition: partitionKey },
                      s.requestOptions()
                    )
                    if (!valid()) return
                    set(s.partition, partitionKey)
                    set(x.columnHistory, null)
                    set(x.range, partitionKey)
                    set(x.preview, null)
                    if (status === 'failed' && r.runId) {
                      set(s.tab, 'runs')
                      await s.showRun(r.runId)
                    }
                  }, false)
                }
              />
            )}
            {page?.hasMore && (
              <Button onPress={() => s.action(() => x.loadPage(key, true), false)}>
                More partitions
              </Button>
            )}
            {page && (
              <Input
                label="Backfill partitions"
                placeholder="failed or start..end"
                value={read(x.range)}
                onChange={(v) => {
                  set(x.range, v)
                  set(x.preview, null)
                }}
              />
            )}
            {page && (
              <Button
                disabled={read(s.busy)}
                onPress={() => s.action(() => x.prepareBackfill(key), false)}
              >
                Preview backfill
              </Button>
            )}
            {preview && (
              <Stack>
                <Text>
                  {preview.targets
                    .map(
                      (t) =>
                        t.assetKey.path.join('/') +
                        ' · ' +
                        (t.partitions?.partitionKeys?.join(', ') ||
                          t.partitions?.ranges?.map((r) => r.start + '..' + r.end).join(', ') ||
                          'unpartitioned')
                    )
                    .join('\n')}
                </Text>
                <Button
                  disabled={read(s.busy)}
                  onPress={() =>
                    s.action(async () => {
                      const valid = x.guarded(true)
                      const result = await plugin.request(
                        'dagster.launchBackfill',
                        { ...s.scope(), assetKey: key, keys: preview.keys },
                        s.requestOptions()
                      )
                      if (valid()) {
                        set(x.preview, null)
                        set(x.backfill, result.backfillId)
                        set(x.progress, '')
                        await x.track()
                      }
                    }, false)
                  }
                >
                  {'Launch backfill · ' + preview.keys.length + ' partitions'}
                </Button>
              </Stack>
            )}
          </Stack>
        </Card>
      )}
    </Stack>
  )
}

import { Button, Stack } from 'orca'
import type { DagLayoutResult } from '../dagster/layout'
export interface GraphProps {
  layout: DagLayoutResult
  selected: string
  badges: Record<string, string>
  edgeHighlights?: Set<string>
  highlighted: Set<string>
  checks: Map<string, import('../types').HealthRow>
  assets: Map<string, import('../types').GraphNode>
  select: (id: string) => void
}
export function GraphSurface({ layout, select, badges }: GraphProps) {
  return (
    <Stack>
      {layout.nodes
        .filter((n) => n.asset)
        .map((n) => (
          <Button key={n.key} onPress={() => select(n.key)}>
            {n.label + ' ' + (badges[n.key] ?? '')}
          </Button>
        ))}
    </Stack>
  )
}

export type GridProps = {
  page: import('../types').PartitionPage
  select: (key: string, status: string) => void
}
export function PartitionGrid({ page, select }: GridProps) {
  return (
    <Stack>
      {page.rows.map((r) => (
        <Button key={r.key} onPress={() => select(r.key, r.status)}>
          {r.key + ' · ' + r.status}
        </Button>
      ))}
    </Stack>
  )
}
export type WaterfallProps = { run: import('../types').RunDetail; now: number }
export function Waterfall({ run, now }: WaterfallProps) {
  return (
    <Stack>
      {run.steps.map((s) => (
        <Stack key={s.key}>
          {s.key +
            ' · ' +
            s.status +
            ' · ' +
            (s.startTime ? ((s.endTime ?? now) - s.startTime) / 1000 + 's' : 'pending')}
        </Stack>
      ))}
    </Stack>
  )
}

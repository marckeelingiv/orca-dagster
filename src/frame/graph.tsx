/** @jsxImportSource preact */
import { useLayoutEffect, useRef, useState } from 'preact/hooks'
import type { GraphProps, GridProps, WaterfallProps } from '../panel/graph-surface'
export function GraphSurface({
  layout,
  selected,
  highlighted,
  badges,
  checks,
  assets,
  select,
  edgeHighlights
}: GraphProps) {
  const [scale, zoom] = useState(1)
  const drag = useRef<{ x: number; y: number; left: number; top: number } | null>(null)
  const viewport = useRef<HTMLDivElement>(null)
  const center = useRef<{ x: number; y: number } | null>(null)
  const focused = layout.nodes.find((n) => n.key === selected)
  function moveTo(x: number, y: number, at: number) {
    const el = viewport.current
    if (!el) return
    el.scrollLeft = x * at - el.clientWidth / 2
    el.scrollTop = y * at - el.clientHeight / 2
  }
  function resize(to: number, target?: { x: number; y: number }) {
    const el = viewport.current
    if (!el) return
    const point = target ?? {
      x: (el.scrollLeft + el.clientWidth / 2) / scale,
      y: (el.scrollTop + el.clientHeight / 2) / scale
    }
    if (scale === to) moveTo(point.x, point.y, to)
    else {
      center.current = point
      zoom(to)
    }
  }
  useLayoutEffect(() => {
    if (viewport.current) {
      // Start at a card boundary so a narrow sidebar opens on readable asset keys.
      const midpoint = (layout.width - viewport.current.clientWidth / scale) / 2
      const roots = layout.nodes.filter((n) => n.layer === 0)
      const anchor = roots.reduce(
        (closest, n) => (Math.abs(n.x - midpoint) < Math.abs(closest.x - midpoint) ? n : closest),
        roots[0]
      )
      viewport.current.scrollLeft = anchor && midpoint > 0 ? (anchor.x - 12) * scale : 0
      viewport.current.scrollTop = 0
    }
  }, [layout])
  useLayoutEffect(() => {
    if (center.current) moveTo(center.current.x, center.current.y, scale)
    center.current = null
  }, [scale])
  return (
    <div class="graph-stage">
      <div
        class="canvas"
        ref={viewport}
        onPointerDown={(e) => {
          if ((e.target as Element).closest('[role="button"]')) return
          const el = e.currentTarget
          drag.current = { x: e.clientX, y: e.clientY, left: el.scrollLeft, top: el.scrollTop }
          el.setPointerCapture(e.pointerId)
        }}
        onPointerMove={(e) => {
          if (drag.current) {
            e.currentTarget.scrollLeft = drag.current.left + drag.current.x - e.clientX
            e.currentTarget.scrollTop = drag.current.top + drag.current.y - e.clientY
          }
        }}
        onPointerUp={() => {
          drag.current = null
        }}
        onPointerCancel={() => {
          drag.current = null
        }}
      >
        {!layout.nodes.length && <p class="canvas-empty">No assets in this graph.</p>}
        <svg
          aria-label="Asset dependency graph"
          width={layout.width * scale}
          height={layout.height * scale}
          viewBox={`0 0 ${layout.width} ${layout.height}`}
        >
          {layout.edges.map((e) => (
            <path
              key={e.id}
              d={e.path}
              class={
                (
                  edgeHighlights
                    ? edgeHighlights.has(JSON.stringify([e.from, e.to]))
                    : highlighted.has(e.from) && highlighted.has(e.to)
                )
                  ? highlighted.size < assets.size || edgeHighlights
                    ? 'edge active'
                    : 'edge'
                  : 'edge dim'
              }
            />
          ))}
          {layout.nodes.map((n) => {
            const asset = assets.get(n.key),
              check = checks.get(n.key)
            const count = check?.checkCount ?? 0
            const meta =
              [asset?.groupName, ...(asset?.kinds ?? [])].filter(Boolean).join(' · ') ||
              'External source'
            const status = asset?.status ?? 'neutral'
            const badge = badges[n.key]
            return (
              <g
                key={n.key}
                transform={`translate(${n.x} ${n.y})`}
                class={[
                  'node',
                  selected === n.key ? 'selected' : '',
                  highlighted.has(n.key) || (n.external && highlighted.size === assets.size)
                    ? ''
                    : 'dim',
                  /IN_PROGRESS|STARTED/.test(badges[n.key] ?? '') ? 'running' : ''
                ].join(' ')}
                role={n.asset ? 'button' : undefined}
                tabIndex={n.asset ? 0 : undefined}
                aria-label={n.label}
                onClick={() => n.asset && select(n.key)}
                onKeyDown={(e) => {
                  if (n.asset && ['Enter', ' '].includes(e.key)) {
                    e.preventDefault()
                    select(n.key)
                  }
                }}
              >
                <title>
                  {[n.label, meta, status, count ? count + ' checks' : '', badge]
                    .filter(Boolean)
                    .join(' · ')}
                </title>
                <rect class="node-card" width={n.width} height={n.height} rx="7" />
                <circle class={'status-dot ' + status} cx="14" cy="20" r="3.5" />
                <text class="node-name" x="24" y="24">
                  {n.label.length > 23 ? n.label.slice(0, 22) + '…' : n.label}
                </text>
                <text class="subtitle" x="11" y={badge ? '41' : '47'}>
                  {meta.length > 27 ? meta.slice(0, 26) + '…' : meta}
                </text>
                {badge && (
                  <text class="node-note" x="11" y="55">
                    {badge.slice(0, 30)}
                  </text>
                )}
                {count > 0 && (
                  <g
                    class={'check-badge' + (check?.failed?.length ? ' failed' : '')}
                    transform={`translate(${n.width - 39} 34)`}
                  >
                    <title>{count + ' checks · ' + (check?.failed?.length ?? 0) + ' failed'}</title>
                    <rect width="30" height="18" rx="8" />
                    <text x="15" y="12" text-anchor="middle">
                      {(check?.failed?.length ? '×' : '✓') + count}
                    </text>
                  </g>
                )}
              </g>
            )
          })}
        </svg>
      </div>
      <div class="canvas-footer">
        <span class="canvas-hint">Drag to pan</span>
        <div class="zoom" role="toolbar" aria-label="Graph viewport">
          <button
            aria-label="Zoom out"
            disabled={scale <= 0.05}
            onClick={() => resize(Math.max(0.05, scale / 1.25))}
          >
            −
          </button>
          <button
            class="zoom-level"
            aria-label="Reset zoom to 100%"
            title="Reset zoom to 100%"
            onClick={() => resize(1)}
          >
            {Math.round(scale * 100)}%
          </button>
          <button
            aria-label="Zoom in"
            disabled={scale >= 3}
            onClick={() => resize(Math.min(3, scale * 1.25))}
          >
            +
          </button>
          <button
            disabled={!layout.width}
            onClick={() => {
              const el = viewport.current!
              resize(
                Math.min(
                  1.5,
                  (el.clientWidth - 12) / layout.width,
                  (el.clientHeight - 48) / layout.height
                ),
                { x: layout.width / 2, y: layout.height / 2 }
              )
            }}
          >
            Fit
          </button>
          <button
            disabled={!focused}
            title="Center the selected asset"
            onClick={() => {
              if (focused)
                resize(1, { x: focused.x + focused.width / 2, y: focused.y + focused.height / 2 })
            }}
          >
            Focus
          </button>
        </div>
      </div>
    </div>
  )
}

export function PartitionGrid({ page, select }: GridProps) {
  const daily =
    page.calendar && page.rows.every((r) => /^\d{4}-\d{2}-\d{2}$/.test(r.label ?? r.key))
  if (!daily)
    return (
      <div class="partition-grid">
        {page.rows.map((r) => (
          <button key={r.key} class={r.status} onClick={() => select(r.key, r.status)}>
            {r.label ?? r.key}
            <small>{r.status}</small>
          </button>
        ))}
      </div>
    )
  const months = new Map<string, typeof page.rows>()
  for (const r of page.rows) {
    const month = (r.label ?? r.key).slice(0, 7)
    const rows = months.get(month) ?? []
    rows.push(r)
    months.set(month, rows)
  }
  return (
    <div>
      {[...months].map(([month, rows]) => (
        <section key={month}>
          <h3>{month}</h3>
          <div class="calendar">
            {['S', 'M', 'T', 'W', 'T', 'F', 'S'].map((d, i) => (
              <small key={i}>{d}</small>
            ))}
            {rows.map((r, i) => {
              const date = r.label ?? r.key
              return (
                <button
                  key={r.key}
                  style={
                    i === 0
                      ? { gridColumnStart: new Date(date + 'T00:00:00Z').getUTCDay() + 1 }
                      : undefined
                  }
                  class={r.status}
                  aria-label={r.key + ' · ' + r.status}
                  title={r.status}
                  onClick={() => select(r.key, r.status)}
                >
                  {Number(date.slice(-2))}
                  <small>{r.status}</small>
                </button>
              )
            })}
          </div>
        </section>
      ))}
    </div>
  )
}
export function Waterfall({ run, now }: WaterfallProps) {
  const starts = run.steps.flatMap((s) => (s.startTime ? [s.startTime] : []))
  const start = run.startTime ?? (starts.length ? Math.min(...starts) : now),
    duration = Math.max(1, (run.endTime ?? now) - start)
  return (
    <div class="stack">
      {run.steps.map((step) => (
        <div key={step.key}>
          <strong>{step.key}</strong>
          <small>
            {step.status}
            {step.startTime
              ? ' · ' +
                (Math.max(0, (step.endTime ?? now) - step.startTime) / 1000).toFixed(1) +
                's'
              : ''}
            {step.attempts && step.attempts.length > 1
              ? ' · ' + step.attempts.length + ' attempts'
              : ''}
          </small>
          <div class="track">
            {(step.attempts?.length ? step.attempts : [step]).map((attempt, i) => {
              if (!attempt.startTime) return null
              const left = Math.min(
                100,
                Math.max(0, (100 * (attempt.startTime - start)) / duration)
              )
              const width = Math.min(
                100 - left,
                Math.max(0.5, (100 * ((attempt.endTime ?? now) - attempt.startTime)) / duration)
              )
              return (
                <span
                  key={i}
                  title={'Attempt ' + (i + 1)}
                  class={'bar ' + step.status.toLowerCase()}
                  style={{ left: left + '%', width: width + '%' }}
                />
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}

import { atom, read, set, plugin } from 'orca'
import type {
  BackfillPreview,
  ColumnFlow,
  ColumnHistory,
  GraphNode,
  HealthRow,
  PartitionPage,
  SourceIndex
} from '../types'
import * as s from './state'

export const graph = atom<GraphNode[]>([]),
  health = atom<HealthRow[]>([]),
  index = atom<SourceIndex | null>(null)
export const fileSearch = atom(''),
  pickerOpen = atom(false)
export const sources = atom(true),
  indexing = atom(false),
  queryDraft = atom('')
let queryTimer: ReturnType<typeof setTimeout> | undefined
export function changeQuery(value: string, debounce = false) {
  clearTimeout(queryTimer)
  set(queryDraft, value)
  if (debounce) queryTimer = setTimeout(() => set(query, value), 150)
  else set(query, value)
}
export const query = atom(''),
  file = atom(''),
  review = atom<string[]>([]),
  completed = atom<Record<string, string>>({})
export const pages = atom<PartitionPage | null>(null),
  range = atom('failed'),
  preview = atom<{ keys: string[]; targets: BackfillPreview[] } | null>(null)
export const backfill = atom(''),
  progress = atom(''),
  columnHistory = atom<ColumnHistory | null>(null),
  column = atom('')
export const flow = atom<ColumnFlow[]>([])
export const compareKey = atom(''),
  comparison = atom(''),
  lineage = atom<string[]>([])
let generation = 0,
  focus = 0,
  graphRequest = 0,
  healthFetchedAt = 0
export function clearAsset() {
  focus++
  set(pages, null)
  set(range, 'failed')
  set(preview, null)
  set(columnHistory, null)
  set(column, '')
  set(lineage, [])
  set(flow, [])
}
export function clearCatalog() {
  generation++
  healthFetchedAt = 0
  clearAsset()
  set(fileSearch, '')
  set(graph, [])
  set(health, [])
  set(index, null)
  changeQuery('')
  set(sources, true)
  set(pickerOpen, false)
  cancelIndex()
  token = undefined
  pending = undefined
  set(file, '')
  set(review, [])
  set(completed, {})
}
export function reset() {
  clearCatalog()
  set(backfill, '')
  set(progress, '')
  set(compareKey, '')
  set(comparison, '')
}
export function guarded(asset = false) {
  const epoch = generation,
    id = focus,
    part = read(s.partition)
  return () =>
    epoch === generation &&
    (!asset || (id === focus && part === read(s.partition))) &&
    !s.requestOptions().signal.aborted
}
let token: string | undefined, pending: Promise<void> | undefined
export const graphToken = () => token
let indexRequest: { abort: AbortController; promise: Promise<void> } | undefined
function cancelIndex() {
  indexRequest?.abort.abort()
  indexRequest = undefined
  set(indexing, false)
}
export async function ensureGraphIndex() {
  if (read(index)) return
  const current = guarded()
  if (pending) await pending
  else if (!token) await loadGraph()
  if (current() && token && !read(index)) await loadIndex(token)
}
function loadIndex(graphToken: string) {
  if (indexRequest) return indexRequest.promise
  const valid = guarded(),
    abort = new AbortController()
  const signal = AbortSignal.any([abort.signal, s.requestOptions().signal])
  set(indexing, true)
  const promise = plugin
    .request('source.index', { ...s.scope(), token: graphToken }, { signal, timeoutMs: 120000 })
    .then((result) => {
      if (valid() && token === graphToken) set(index, result)
    })
    .catch((e) => {
      if (!signal.aborted && valid() && token === graphToken) s.fail(e)
    })
    .finally(() => {
      if (indexRequest?.abort === abort) {
        indexRequest = undefined
        set(indexing, false)
      }
    })
  indexRequest = { abort, promise }
  return promise
}
export function loadGraph(withIndex = false) {
  const current = guarded(),
    seq = ++graphRequest
  const valid = () => current() && seq === graphRequest
  token = undefined
  set(review, [])
  set(completed, {})
  set(index, null)
  cancelIndex()
  async function load() {
    let page = await plugin.request('dagster.graph', s.scope(), s.requestOptions())
    const nodes = [...page.nodes]
    while (page.next !== undefined) {
      if (!valid()) return
      page = await plugin.request(
        'dagster.graph',
        { ...s.scope(), token: page.token, offset: page.next },
        s.requestOptions()
      )
      for (const node of page.nodes) nodes.push(node)
    }
    if (!valid()) return
    token = page.token
    set(graph, nodes)
    const bundled = nodes.every((n) => n.health)
    if (bundled) {
      healthFetchedAt = Date.now()
      set(
        health,
        nodes.map((n) => ({ id: n.keyString, assetKey: n.assetKey, ...n.health }))
      )
    }
    if (withIndex) void loadIndex(page.token)
    if (!bundled) await refreshHealth(true)
  }
  const request = load()
    .catch((e) => {
      if (valid()) s.fail(e)
    })
    .finally(() => {
      if (seq === graphRequest) pending = undefined
    })
  pending = request
  return request
}
export async function refreshHealth(force = false) {
  if (!read(graph).length || (!force && Date.now() - healthFetchedAt < 15000)) return
  const valid = guarded(),
    request = graphRequest
  healthFetchedAt = Date.now()
  const rows = await plugin.request('dagster.health', s.scope(), s.requestOptions())
  if (valid() && request === graphRequest) set(health, rows)
}
export async function loadPage(assetKey: string[], more = false, fixed?: Record<string, string>) {
  const valid = guarded(true),
    old = read(pages)
  const result = await plugin.request(
    'dagster.partitionPage',
    {
      ...s.scope(),
      assetKey,
      ...((more || fixed) && old?.token ? { token: old.token } : {}),
      ...((fixed ?? old?.fixed) ? { fixed: fixed ?? old?.fixed } : {}),
      ...(more && old ? { cursor: old.cursor } : {})
    },
    s.requestOptions()
  )
  if (valid()) {
    set(pages, { ...result, rows: [...(more ? (old?.rows ?? []) : []), ...result.rows] })
    set(preview, null)
  }
}
export async function track() {
  const id = read(backfill),
    valid = guarded()
  if (!id || /^(COMPLETED|FAILED|CANCELED)/.test(read(progress))) return
  const r = await plugin.request(
    'dagster.backfillStatus',
    { ...s.scope(), backfillId: id },
    s.requestOptions()
  )
  if (valid() && id === read(backfill)) set(progress, r.status + ' · ' + r.progress)
}
export async function prepareBackfill(assetKey: string[]) {
  const valid = guarded(true),
    selection = read(range).trim()
  const page = read(pages)
  if (!page) throw Error('Load partitions first.')
  const result = await plugin.request(
    'dagster.prepareBackfill',
    {
      ...s.scope(),
      assetKey,
      selection,
      ...(page.fixed && { fixed: page.fixed }),
      ...(page.token && { token: page.token })
    },
    s.requestOptions()
  )
  if (valid() && page === read(pages) && selection === read(range).trim()) set(preview, result)
}

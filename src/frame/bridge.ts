import type {} from 'orca/frame'
import type { Atom, Disposable } from 'orca'

export const { plugin, settings } = window.orca
export const shell = {
  openExternal: (url: string) =>
    window.orca.shell?.openExternal(url) ??
    Promise.reject(Error('Open this link in the desktop app.'))
}
type Cell<T> = { value: T; initial: T; key?: string }
const cells = new Map<string, Cell<unknown>>()
let storeKey = '',
  redraw = () => {},
  pending = false
let writing = Promise.resolve()
export function atom<T>(initial: T): Cell<T>
export function atom<T>(key: string, initial: T, options?: unknown): Cell<T>
export function atom<T>(first: string | T, initial?: T): Cell<T> {
  if (arguments.length === 1) return { value: first as T, initial: first as T }
  const cell = { value: initial as T, initial: initial as T, key: first as string }
  cells.set(cell.key, cell)
  return cell
}
export function read<T>(cell: Cell<T>) {
  return cell.value
}
export function set<T>(cell: Cell<T>, value: T) {
  if (Object.is(cell.value, value)) return
  cell.value = value
  if (cell.key && storeKey) {
    const k = storeKey
    const snapshot = Object.fromEntries([...cells].map(([id, c]) => [id, c.value])) as Record<
      string,
      string | boolean
    >
    writing = writing.then(() => window.orca.storage.set(k, snapshot)).catch(() => {})
  }
  if (!pending) {
    pending = true
    queueMicrotask(() => {
      pending = false
      redraw()
    })
  }
}
export function update<T>(cell: Cell<T>, fn: (value: T) => T) {
  set(cell, fn(cell.value))
}
export async function mountFrame(
  draw: () => void,
  setup: (ctx: import('orca').PanelContext, workspaceReady: () => Promise<void>) => void
) {
  let revision = 0,
    hydration = Promise.resolve()
  function restore(workspaceId?: string | null) {
    const key = 'sidebar.' + (workspaceId ?? 'global')
    const seq = ++revision
    // Suspend writes immediately; a rapid switch back must see previously queued writes.
    storeKey = ''
    hydration = writing.then(async () => {
      const saved = await window.orca.storage.get(key).catch(() => null)
      if (seq !== revision) return
      for (const [id, cell] of cells) {
        const value =
          saved && typeof saved === 'object' && !Array.isArray(saved) ? saved[id] : undefined
        cell.value = typeof value === typeof cell.initial ? value : cell.initial
      }
      storeKey = key
    })
  }
  const visible = atom(!document.hidden),
    controller = new AbortController(),
    subscriptions: Disposable[] = []
  document.addEventListener('visibilitychange', () => set(visible, !document.hidden), {
    signal: controller.signal
  })
  subscriptions.push(plugin.on('dagster.workspaceChanged', (e) => restore(e.workspaceId)))
  const ctx = await window.orca.host.call('workspace.readContext')
  if (!revision) restore(ctx?.workspaceId)
  async function workspaceReady() {
    let current
    do {
      current = hydration
      await current
    } while (hydration !== current)
  }
  await workspaceReady()
  redraw = draw
  setup(
    {
      visible: visible as unknown as Atom<boolean>,
      subscriptions,
      signal: controller.signal,
      panel: { id: 'dagster-sidebar' }
    },
    workspaceReady
  )
  window.addEventListener(
    'pagehide',
    () => {
      controller.abort()
      subscriptions.forEach((d) => d.dispose())
    },
    { once: true }
  )
  draw()
}

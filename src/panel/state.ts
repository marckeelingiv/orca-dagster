import * as explorer from './explorer-state'
import { atom, read, set, plugin, settings, type PanelContext } from 'orca'
import {
  countActiveRuns,
  type AgentTargets,
  type LocalCommand,
  type AssetSummary,
  type Partitions,
  type ProfileList,
  type RunDetail,
  type RunFilter,
  type RunPage,
  type Snapshot,
  type SourceInput,
  type SourceResult
} from '../types'

import { basket, keyOf } from './selection'
export { keyOf } from './selection'

const persist = { persist: 'workspace' as const }
export const tab = atom('dagster.tab', 'assets', persist)
export const editor = atom('dagster.editor', false, persist)
export const expression = atom('')
export const partitionSearch = atom('')
export const assetSearch = atom('dagster.assets.search', '', persist)
export const autoSearch = atom('dagster.automation.search', '', persist)
export const runSearch = atom('dagster.runs.search', '', persist)
export const runId = atom('dagster.run', '', persist)
export const assetId = atom('dagster.asset', '', persist)
const savedScope = atom('dagster.scope', '', persist)
export const profiles = atom<ProfileList | null>(null)
export const snapshot = atom<Snapshot | null>(null)
export const detail = atom<RunDetail | null>(null)
export const targets = atom<AgentTargets['terminals']>([])
export const destination = atom('')
export const filter = atom<RunFilter>('All')
export const page = atom<RunPage | null>(null)
export const errorsOnly = atom(true)
export const source = atom<SourceResult | null>(null)
export const partitions = atom<Partitions | null>(null)
export const partition = atom('')
export const config = atom('')
export const tags = atom('')
export const message = atom('')
export const busy = atom(false)
export const switching = atom(false)
export const launchedRun = atom('')
export const localConfirmation = atom<{
  action: 'local.start' | 'local.restart'
  plan: LocalCommand
} | null>(null)
export function runLocal(
  name: 'local.start' | 'local.restart' | 'local.stop' | 'local.focusLogs',
  confirmed?: LocalCommand
) {
  const id = epoch
  return action(async () => {
    set(localConfirmation, null)
    if (name === 'local.focusLogs' || name === 'local.stop') {
      await plugin.request(name, scope(), requestOptions())
    } else {
      const { state } = await plugin.request(
        name,
        { ...scope(), ...(confirmed && { confirmed }) },
        requestOptions()
      )
      if (current(id) && state.confirmation)
        set(localConfirmation, { action: name, plan: state.confirmation })
    }
  })
}
export const automationSource = atom<SourceResult | null>(null)
export const automationSelection = atom('')
export const jobConfig = atom('')
export const clip = (s: string | undefined, length = 120) => (s ?? '').slice(0, length)
export const options = (values: string[]) => values.map((value) => ({ value, label: value }))
export const scope = () => ({
  ...(read(profiles)?.activeProfileId ? { profileId: read(profiles)!.activeProfileId! } : {}),
  workspaceId: read(profiles)?.workspaceId ?? null
})
export const active = () => read(profiles)?.profiles.find((p) => p.id === scope().profileId)
export const asSource = (a: AssetSummary): SourceInput => ({
  kind: 'asset',
  name: a.assetKey.join('/'),
  assetKey: a.assetKey,
  locationName: a.locationName,
  repositoryName: a.repositoryName,
  ...(a.opNames?.length && { opNames: a.opNames }),
  ...(a.dbtUniqueId && { dbtUniqueId: a.dbtUniqueId }),
  codeReferences: a.codeReferences
})
let openEditor: () => void
let ctx: PanelContext
let epoch = 0,
  generation = 0,
  listSeq = 0,
  runSeq = 0,
  assetSeq = 0,
  pageSeq = 0
let polling = false,
  pendingPage = false,
  timer: ReturnType<typeof setTimeout> | undefined
let idle = 15,
  fast = 3,
  logLimit = 200
const current = (id: number) => id === epoch && !ctx.signal.aborted
export const requestOptions = () => ({ signal: ctx.signal })
export function fail(e: unknown) {
  set(message, clip(e instanceof Error ? e.message : String(e), 700))
}
export async function action(task: () => Promise<unknown>, refresh = true) {
  if (read(busy) || read(switching)) return
  const id = epoch
  set(busy, true)
  set(message, '')
  try {
    await task()
    if (refresh && current(id)) await sync()
  } catch (e) {
    if (current(id)) fail(e)
  } finally {
    if (current(id)) set(busy, false)
  }
}
export function launch(task: () => Promise<{ runId: string }>) {
  const id = epoch
  return action(async () => {
    const result = await task()
    if (current(id)) set(launchedRun, result.runId)
  })
}
let automationSeq = 0
export async function openAutomation(identity: string, input: SourceInput) {
  const seq = ++automationSeq,
    id = epoch
  if (read(automationSelection) !== identity) set(jobConfig, '')
  set(automationSelection, identity)
  set(automationSource, null)
  try {
    const result = await plugin.request('source.open', { ...scope(), ...input }, requestOptions())
    if (current(id) && seq === automationSeq) set(automationSource, result)
  } catch (e) {
    if (current(id) && seq === automationSeq) fail(e)
  }
}
export async function selectAsset(a: AssetSummary, open = true) {
  const seq = ++assetSeq,
    id = epoch
  explorer.clearAsset()
  set(assetId, keyOf(a))
  set(source, null)
  set(partitions, null)
  set(partition, '')
  set(config, '')
  set(tags, '')
  const valid = () => current(id) && seq === assetSeq
  const tasks: Promise<unknown>[] = []
  if (open)
    tasks.push(
      plugin.request('source.open', { ...scope(), ...asSource(a) }, requestOptions()).then((r) => {
        if (valid()) set(source, r)
      })
    )
  if (a.isPartitioned)
    tasks.push(
      plugin
        .request(
          'dagster.assetPartitions',
          { ...scope(), assetKey: a.assetKey, limit: 100 },
          requestOptions()
        )
        .then((r) => {
          if (valid()) {
            set(partitions, r)
            set(partition, r.keys[0] ?? '')
          }
        })
    )
  for (const r of await Promise.allSettled(tasks))
    if (valid() && r.status === 'rejected') fail(r.reason)
}
export async function showRun(id: string, reset = true) {
  const seq = ++runSeq,
    context = epoch
  if (reset) {
    set(explorer.compareKey, '')
    set(explorer.comparison, '')
    set(detail, null)
    set(targets, [])
    set(destination, '')
  }
  set(runId, id)
  if (!id) return
  const valid = () => current(context) && seq === runSeq && read(runId) === id
  try {
    const { run } = await plugin.request(
      'dagster.runDetail',
      { ...scope(), runId: id, errorsOnly: read(errorsOnly), limit: logLimit },
      requestOptions()
    )
    if (!valid()) return
    const becameFailed = read(detail)?.status !== 'FAILURE' && run.status === 'FAILURE'
    set(detail, run)
    if (becameFailed || (run.status === 'FAILURE' && reset)) {
      const result = await plugin.request('agent.listTargets', scope(), requestOptions())
      if (valid()) {
        set(targets, result.terminals.slice(0, 50))
        set(destination, result.terminals[0]?.terminalId ?? '')
      }
    }
  } catch (e) {
    if (valid()) fail(e)
  }
}
export async function loadPage(more = false, refresh = false) {
  if (more && pendingPage) return
  const previous = more ? (read(page)?.runs ?? []) : []
  if (!more && !refresh) set(page, null)
  const seq = ++pageSeq,
    id = epoch
  pendingPage = true
  try {
    const result = await plugin.request(
      'dagster.runs',
      {
        ...scope(),
        filter: read(filter),
        ...(previous.length ? { cursor: previous.at(-1)!.runId } : {}),
        limit: refresh ? Math.max(30, read(page)?.runs.length ?? 0) : 30
      },
      requestOptions()
    )
    if (current(id) && seq === pageSeq) {
      const runs = [
        ...new Map([...previous, ...result.runs].map((r) => [r.runId, r])).values()
      ].slice(0, 200)
      set(page, {
        runs,
        hasMore: result.hasMore && runs.length < 200 && runs.length > previous.length
      })
    }
  } catch (e) {
    if (current(id) && seq === pageSeq) fail(e)
  } finally {
    if (current(id) && seq === pageSeq) pendingPage = false
  }
}
function schedule() {
  clearTimeout(timer)
  if (ctx.signal.aborted) return
  const seconds = countActiveRuns(read(snapshot)) ? fast : idle
  timer = setTimeout(() => {
    if (read(ctx.visible) && scope().profileId && !read(switching)) void sync()
    else schedule()
  }, seconds * 1000)
}
export async function sync() {
  if (polling || !scope().profileId || !read(ctx.visible)) {
    schedule()
    return
  }
  const id = epoch
  polling = true
  try {
    const data = await plugin.request('dagster.snapshot', scope(), requestOptions())
    if (!current(id)) return
    const first = !read(snapshot)
    set(snapshot, data)
    if (first && read(assetId)) {
      const a = data.assets.find((a) => keyOf(a) === read(assetId))
      if (a) void selectAsset(a, false)
    }
    if (read(explorer.backfill)) await explorer.track()
    if (!read(editor) && read(tab) === 'dag') {
      if (read(runId)) await showRun(read(runId), false)
      await explorer.refreshHealth()
    }
    if (!read(editor) && read(tab) === 'runs') {
      if (read(runId)) await showRun(read(runId), !read(detail))
      else if (!pendingPage) await loadPage(false, true)
    }
  } catch (e) {
    if (current(id)) fail(e)
  } finally {
    if (current(id)) {
      polling = false
      schedule()
    }
  }
}
export function invalidate() {
  epoch++
  explorer.reset()
  polling = false
  pendingPage = false
  clearTimeout(timer)
  set(basket, [])
  set(expression, '')
  set(partitionSearch, '')
  set(launchedRun, '')
  set(automationSource, null)
  set(jobConfig, '')
  set(snapshot, null)
  set(detail, null)
  set(source, null)
  set(page, null)
  set(busy, false)
  set(localConfirmation, null)
  set(message, '')
}
export async function reloadProfiles() {
  const seq = ++listSeq,
    session = generation
  try {
    const result = await plugin.request('profiles.list', undefined, requestOptions())
    if (session !== generation || seq !== listSeq || ctx.signal.aborted) return
    const key = JSON.stringify([result.workspaceId, result.activeProfileId])
    if (read(savedScope) !== key) {
      invalidate()
      set(savedScope, key)
      set(assetId, '')
      set(runId, '')
      set(automationSelection, '')
      set(filter, 'All')
    }
    set(profiles, result)
    set(switching, false)
    if (result.showProfiles || read(editor)) openEditor()
    await sync()
    if (
      (result.showSourcePicker || (!read(editor) && read(tab) === 'dag')) &&
      result.activeProfileId &&
      session === generation &&
      seq === listSeq
    ) {
      set(tab, 'dag')
      set(editor, false)
      set(explorer.pickerOpen, !!result.showSourcePicker)
      await explorer.loadGraph(true)
      if (result.sourcePickerPath && session === generation && seq === listSeq)
        await explorer.ensureGraphIndex()
      if (result.sourcePickerPath && session === generation && seq === listSeq) {
        const match = read(explorer.index)?.files.find((f) => f.path === result.sourcePickerPath)
        if (match) {
          set(explorer.file, match.path)
          if (match.assetIds.length === 1) {
            const node = read(explorer.graph).find((n) => n.keyString === match.assetIds[0])
            if (node) await selectAsset(node, false)
          }
        } else set(message, 'No asset found for ' + result.sourcePickerPath)
      }
    }
  } catch (e) {
    if (session === generation && seq === listSeq && !ctx.signal.aborted) {
      set(switching, false)
      fail(e)
    }
  }
}
export async function switchProfile(profileId: string) {
  if (read(switching)) return
  invalidate()
  ++listSeq
  set(switching, true)
  const id = epoch
  try {
    await plugin.request('profiles.select', { ...scope(), profileId }, requestOptions())
  } catch (e) {
    if (current(id)) fail(e)
  } finally {
    if (current(id)) await reloadProfiles()
  }
}
export function setup(
  context: PanelContext,
  ensureEditor: () => void,
  workspaceReady?: () => Promise<void>
) {
  openEditor = ensureEditor
  ctx = context
  generation++
  epoch++
  explorer.reset()
  polling = false
  pendingPage = false
  const session = generation
  async function configure() {
    try {
      const values = await settings.get()
      if (ctx.signal.aborted || generation !== session) return
      idle = Math.min(300, Math.max(5, Number(values.pollIntervalIdleSec) || 15))
      fast = Math.min(60, Math.max(1, Number(values.pollIntervalActiveSec) || 3))
      logLimit = Math.min(1000, Math.max(50, Number(values.maxRunLogLines) || 200))
      schedule()
    } catch (e) {
      if (generation === session && !ctx.signal.aborted) fail(e)
    }
  }
  ctx.subscriptions.push(
    settings.onDidChange(configure),
    plugin.on('dagster.workspaceChanged', async () => {
      invalidate()
      const seq = ++listSeq
      set(switching, true)
      set(profiles, null)
      await workspaceReady?.()
      if (seq === listSeq && !ctx.signal.aborted) await reloadProfiles()
    }),
    plugin.on('dagster.stateChanged', (event) => {
      if (
        !polling &&
        !read(switching) &&
        read(ctx.visible) &&
        event.activeProfileId === scope().profileId &&
        event.syncedAt !== read(snapshot)?.syncedAt
      )
        void sync()
    }),
    plugin.on('dagster.definitionsChanged', (event) => {
      if (
        (!event.profileId || event.profileId === scope().profileId) &&
        (!event.workspaceId || event.workspaceId === scope().workspaceId)
      ) {
        explorer.clearCatalog()
        if (read(tab) === 'dag') void explorer.loadGraph(true).catch(fail)
      }
    }),
    plugin.on('dagster.sourcePicker', () => {
      set(tab, 'dag')
      set(editor, false)
      void reloadProfiles()
    }),
    plugin.on('dagster.showProfiles', () => {
      set(editor, true)
      if (read(profiles)) ensureEditor()
      // Consume the command's pending request on an already-mounted panel too.
      void reloadProfiles()
    })
  )
  void configure()
  void reloadProfiles()
  // The native visibility atom exposes reads but no change subscription.
  let visible = read(ctx.visible)
  const visibilityTimer = setInterval(() => {
    const now = read(ctx.visible)
    if (!visible && now && !read(switching)) void sync()
    visible = now
  }, 250)
  const cleanup = () => {
    clearInterval(visibilityTimer)
    generation++
    epoch++
    clearTimeout(timer)
  }
  ctx.signal.addEventListener('abort', cleanup, { once: true })
}

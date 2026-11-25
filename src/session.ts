import { Explorer } from './dagster/explorer'
import { stat } from 'node:fs/promises'
import type { OrcaWorkerApi } from 'orca/worker'
import type { PluginEvents } from 'orca/host'
import { AuthError } from './dagster/client'
import { LocalController } from './local/controller'
import { CredentialError, graphqlEndpoint, resolveAuthHeaders } from './profiles/credentials'
import { ProfileStore, type Workspace } from './profiles/store'
import {
  countActiveRuns,
  isActiveRun,
  type ConnectionProfile,
  type Scope,
  type Snapshot
} from './types'

type Focus = PluginEvents['workspace.changed']['context']
export type Connection = { workspace: Workspace; profile: ConnectionProfile; revision: number }

async function resolveWorkspace(orca: OrcaWorkerApi, context: Focus): Promise<Workspace> {
  const id = context?.workspaceId ?? null
  if (!id) return { id, path: null }
  const { workspaces } = await orca.host.call('workspace.list')
  const workspace = workspaces.find((w) => w.id === id)
  // A paired/SSH workspace's absolute path may also exist on this machine.
  const local = !workspace?.hostId || workspace.hostId === 'local'
  return { id, path: local ? (workspace?.path ?? null) : null }
}

export function message(error: unknown): string {
  return (error instanceof Error ? error.message : 'The Dagster request failed.').slice(0, 12000)
}
function empty(): Snapshot {
  return {
    status: 'unconfigured',
    syncedAt: Date.now(),
    locations: [],
    jobs: [],
    schedules: [],
    sensors: [],
    assets: [],
    runs: []
  }
}
export class ScopeChangedError extends Error {}
function scopeChanged(): never {
  throw new ScopeChangedError('The workspace or connection changed. Refresh and try again.')
}

/** Snapshot responses have a 1 MiB Orca IPC limit. Reserve room for each type of catalog item. */
function sidebarSnapshot(snapshot: Snapshot): Snapshot {
  if (Buffer.byteLength(JSON.stringify(snapshot)) < 950_000) return snapshot
  const result = {
    ...snapshot,
    status: 'degraded' as const,
    error: 'This catalog exceeds the sidebar size limit. Open Dagster for the complete catalog.'
  }
  function withinBudget<T>(rows: T[], bytes: number): T[] {
    let remaining = bytes
    return rows.filter((row) => {
      const size = Buffer.byteLength(JSON.stringify(row)) + 1
      if (size > remaining) return false
      remaining -= size
      return true
    })
  }
  return {
    ...result,
    assets: withinBudget(result.assets, 480_000),
    locations: withinBudget(result.locations, 80_000),
    jobs: withinBudget(result.jobs, 90_000),
    schedules: withinBudget(result.schedules, 90_000),
    sensors: withinBudget(result.sensors, 90_000),
    runs: withinBudget(result.runs, 60_000)
  }
}

export class Session {
  readonly store: ProfileStore
  readonly local: LocalController
  private focus: Promise<Workspace>
  private revision = 0
  private observation = 0
  private lastPublished = 0
  private previousRuns = new Map<string, string>()
  private statusKey = ''
  private startup?: { timer?: NodeJS.Timeout; abort: AbortController }
  private last?: { id: string; workspaceId: string | null; snapshot: Snapshot }
  showProfiles = false

  constructor(readonly orca: OrcaWorkerApi) {
    this.store = new ProfileStore(orca)
    this.focus = orca.host
      .call('workspace.readContext')
      .then((context) => resolveWorkspace(orca, context))
    this.local = new LocalController(orca, async (profile) =>
      (await this.client(profile, undefined, 2000)).ping()
    )
  }
  async workspace(scope: Scope = {}): Promise<Workspace> {
    let focus = this.focus
    let workspace = await focus
    while (focus !== this.focus) {
      focus = this.focus
      workspace = await focus
    }
    if (scope.workspaceId !== undefined && scope.workspaceId !== workspace.id) scopeChanged()
    return workspace
  }
  async changeWorkspace(context: Focus) {
    const before = this.focus
    const next = resolveWorkspace(this.orca, context)
    this.focus = next
    const [previous, current] = await Promise.all([before.catch(() => undefined), next])
    if (next !== this.focus) return
    if (previous?.id === current.id && previous.path === current.path) {
      // The host sends the current focus immediately on subscription and again for terminal
      // changes. Neither event invalidates the panel's first in-flight snapshot or selection.
      if (!this.observation && !this.statusKey) await this.snapshot({ workspaceId: current.id })
      return
    }
    this.store.workspaceChanged()
    this.showProfiles = false
    await this.profilesChanged(current)
    // The status bar also exists before anyone opens the panel.
    await this.snapshot({ workspaceId: current.id })
  }
  async profilesChanged(workspace: Workspace) {
    if (workspace.id !== (await this.workspace()).id) return
    this.stopStartupMonitor()
    this.revision++
    this.observation++
    this.last = undefined
    this.previousRuns.clear()
    this.statusKey = ''
    const revision = this.revision
    const { activeProfileId, profiles } = await this.store.list(workspace)
    if (revision !== this.revision || workspace.id !== (await this.workspace()).id) return
    const profile = profiles.find((p) => p.id === activeProfileId)
    this.orca.panels.post('dagster.workspaceChanged', {
      workspaceId: workspace.id,
      activeProfileId
    })
    await this.present(profile, { ...empty(), status: profile ? 'connecting' : 'unconfigured' })
  }
  async connection(scope: Scope = {}): Promise<Connection> {
    const workspace = await this.workspace(scope)
    const revision = this.revision
    const { profiles, activeProfileId } = await this.store.list(workspace)
    if (scope.profileId !== undefined && scope.profileId !== activeProfileId) scopeChanged()
    const profile = profiles.find((p) => p.id === activeProfileId)
    if (!profile) throw new Error('Add or select a Dagster connection profile.')
    const connection = { workspace, profile, revision }
    await this.confirm(connection)
    return connection
  }
  async sourceWorkspace(connection: Connection): Promise<Workspace> {
    const id = connection.profile.sourceWorkspaceId
    if (!id) return connection.workspace
    const { workspaces } = await this.orca.host.call('workspace.list')
    const bound = workspaces.find((w) => w.id === id && (!w.hostId || w.hostId === 'local'))
    if (!bound || !(await stat(bound.path).catch(() => null))?.isDirectory())
      throw Error('Bound source worktree is unavailable. Rebind it in Manage profiles.')
    await this.confirm(connection)
    return { id, path: bound.path }
  }
  async confirm(connection: Connection) {
    await this.workspace({ workspaceId: connection.workspace.id })
    if (connection.revision !== this.revision) scopeChanged()
  }
  stopStartupMonitor() {
    if (!this.startup) return
    clearTimeout(this.startup.timer)
    this.startup.abort.abort()
    this.startup = undefined
  }
  /** A palette launch can start before the sidebar opens. Observe only that process's startup. */
  monitorStartup(connection: Connection) {
    this.stopStartupMonitor()
    const monitor = { abort: new AbortController(), timer: undefined as NodeJS.Timeout | undefined }
    this.startup = monitor
    const deadline = Date.now() + 35_000
    const probe = async () => {
      try {
        await this.confirm(connection)
        const state = await this.snapshot(
          { workspaceId: connection.workspace.id, profileId: connection.profile.id },
          monitor.abort.signal
        )
        if (
          this.startup === monitor &&
          state.localState?.status === 'starting' &&
          Date.now() < deadline
        ) {
          monitor.timer = setTimeout(probe, 1000).unref()
          return
        }
      } catch {
        /* The profile or workspace can change while a startup probe is in flight. */
      }
      if (this.startup === monitor) this.stopStartupMonitor()
    }
    monitor.timer = setTimeout(probe, 1000).unref()
  }
  async client(
    profile: ConnectionProfile,
    signal?: AbortSignal,
    timeoutMs?: number,
    secretOverride?: string,
    beforeMutation?: () => Promise<void>
  ) {
    const endpoint = graphqlEndpoint(profile, process.env)
    const headers = await resolveAuthHeaders(profile, {
      env: process.env,
      secrets: this.orca.secrets,
      secretOverride
    })
    return new Explorer(endpoint, headers, { signal, timeoutMs, beforeMutation })
  }
  cached({ profile, workspace }: Connection) {
    return this.last?.id === profile.id && this.last.workspaceId === workspace.id
      ? this.last.snapshot
      : undefined
  }
  async snapshot(scope: Scope, signal?: AbortSignal): Promise<Snapshot> {
    const revision = this.revision
    const order = ++this.observation
    const workspace = await this.workspace(scope)
    const { profiles, activeProfileId } = await this.store.list(workspace)
    if (scope.profileId !== undefined && scope.profileId !== activeProfileId) scopeChanged()
    const profile = profiles.find((p) => p.id === activeProfileId)
    const snapshot = empty()
    if (profile) {
      try {
        snapshot.uiUrl = graphqlEndpoint(profile, process.env).replace(/\/graphql$/, '')
        if (profile.kind === 'local' && workspace.id) {
          const state = await this.local.probeState(profile, workspace.id, workspace.path)
          if (state.diagnostics) state.diagnostics = state.diagnostics.slice(-12000)
          snapshot.localState = state
        }
        switch (snapshot.localState?.status) {
          case 'stopped':
            snapshot.status = 'stopped'
            break
          case 'starting':
            snapshot.status = 'connecting'
            break
          case 'crashed':
            snapshot.status = 'error'
            snapshot.error =
              snapshot.localState.diagnostics || 'The managed Dagster process failed to start.'
            break
          default:
            Object.assign(snapshot, await (await this.client(profile, signal)).fetchSnapshot())
            snapshot.status = snapshot.locations.some((l) => l.error) ? 'degraded' : 'connected'
        }
      } catch (error) {
        if (signal?.aborted) throw error
        snapshot.status =
          error instanceof AuthError || error instanceof CredentialError ? 'auth-error' : 'error'
        snapshot.error = message(error)
      }
    }
    snapshot.syncedAt = Date.now()
    if (revision === this.revision && order > this.lastPublished && !signal?.aborted) {
      this.lastPublished = order
      if (profile) this.last = { id: profile.id, workspaceId: workspace.id, snapshot }
      await this.present(profile, snapshot)
    }
    return sidebarSnapshot(snapshot)
  }

  /** Only post semantic changes. Posting every poll makes two open panels wake each other indefinitely. */
  private async present(profile: ConnectionProfile | undefined, snapshot: Snapshot) {
    const { runs, status, syncedAt } = snapshot
    const active = countActiveRuns(snapshot)
    const failed = runs.filter((r) => r.status === 'FAILURE')
    const newlyFailed = failed.filter((r) => isActiveRun(this.previousRuns.get(r.runId) ?? ''))
    if (status === 'connected' || status === 'degraded')
      this.previousRuns = new Map(runs.map((r) => [r.runId, r.status]))
    const key = JSON.stringify([
      profile?.id,
      profile?.name,
      status,
      active,
      failed.length,
      snapshot.localState?.status
    ])
    if (this.statusKey !== key) {
      let suffix = ''
      let icon: 'database' | 'activity' | 'warning' | 'plug' = 'database'
      let tone: 'neutral' | 'warning' | 'accent' | 'danger' = 'neutral'
      if (status !== 'connected') {
        const label =
          status === 'auth-error'
            ? 'auth failed'
            : status === 'unconfigured'
              ? 'no profile'
              : status
        suffix = ' · ' + label
        icon = 'plug'
        if (status === 'error' || status === 'auth-error') tone = 'danger'
        else if (status === 'degraded' || status === 'connecting') tone = 'warning'
      } else if (active) {
        suffix = ' · ' + active + ' running'
        icon = 'activity'
        tone = 'accent'
      } else if (failed.length) {
        suffix = ' · ' + failed.length + ' failed'
        icon = 'warning'
        tone = 'danger'
      }
      const label = profile ? 'Dagster: ' + profile.name : 'Dagster'
      const text = label.slice(0, 64 - suffix.length) + suffix
      await this.orca.host.call('statusBar.update', { id: 'dagster-status', text, tone, icon })
      this.statusKey = key
      this.orca.panels.post('dagster.stateChanged', {
        activeProfileId: profile?.id ?? null,
        activeRunCount: active,
        status,
        syncedAt
      })
    }
    if (newlyFailed.length && (await this.orca.settings.get('notifyOnRunFailure')) !== false) {
      await this.orca.host.call('notifications.show', {
        title: 'Dagster run failed',
        body: newlyFailed
          .map((r) => r.pipelineName + ' (' + r.runId.slice(0, 8) + ')')
          .join(', ')
          .slice(0, 240)
      })
    }
  }
}

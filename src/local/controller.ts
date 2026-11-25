import { isAbsolute, relative, resolve, sep } from 'node:path'
import type { OrcaWorkerApi } from 'orca/worker'
import type { ConnectionProfile, LocalCommand, LocalState } from '../types'
import { DagsterApiError, DagsterClient } from '../dagster/client'
import { CredentialError, interpolateEnv, resolveAuthHeaders } from '../profiles/credentials'
import { localPort } from './port'

type RedactDiagnostics = (profile: ConnectionProfile, text: string) => string | Promise<string>
type Tracked = { workspaceId: string; terminalId: string; startedAt: number }
const storeKey = 'local.terminals'
const key = (p: ConnectionProfile, workspace: string) => JSON.stringify([workspace, p.id])

export class LocalController {
  private actions = new Map<string, Promise<unknown>>()
  private writing: Promise<void> = Promise.resolve()

  constructor(
    private orca: OrcaWorkerApi,
    private ping: (p: ConnectionProfile) => Promise<unknown>,
    private redactDiagnostics: RedactDiagnostics = async (p, text) => {
      try {
        const headers = await resolveAuthHeaders(p, { secrets: orca.secrets, env: process.env })
        return new DagsterClient(p.url, headers).redact(text)
      } catch (e) {
        if (!(e instanceof CredentialError)) throw e
        return 'The managed process stopped. Open Logs for diagnostics.'
      }
    }
  ) {}

  private async records() {
    return ((await this.orca.storage.get(storeKey)) ?? {}) as Record<string, Tracked>
  }

  private save(p: ConnectionProfile, workspace: string, entry?: Tracked) {
    const write = this.writing.then(async () => {
      const records = await this.records()
      if (Object.hasOwn(records, p.id) && records[p.id]?.workspaceId === workspace)
        delete records[p.id]
      if (entry) records[key(p, workspace)] = entry
      else delete records[key(p, workspace)]
      await this.orca.storage.set(storeKey, records)
    })
    this.writing = write.catch(() => {})
    return write
  }

  private async tracked(p: ConnectionProfile, workspaceId: string) {
    const records = await this.records()
    const entry = records[key(p, workspaceId)] ?? records[p.id]
    if (entry?.workspaceId !== workspaceId) return
    const list = await this.orca.host.call('terminal.list', { workspaceId })
    if (list.terminals.some((t) => t.id === entry.terminalId)) return entry
    if (list.truncated)
      throw new Error(
        'Orca returned a partial terminal list. Unable to verify the managed terminal.'
      )
  }

  private async healthy(p: ConnectionProfile) {
    let timer: ReturnType<typeof setTimeout> | undefined
    try {
      const timeout = new Promise<boolean>((resolve) => {
        timer = setTimeout(() => resolve(false), 2000)
      })
      return await Promise.race([this.ping(p).then(() => true), timeout])
    } catch (error) {
      // An HTTP or GraphQL response proves a listener exists, even if login failed.
      if (error instanceof DagsterApiError && ['TIMEOUT', 'REQUEST_ERROR'].includes(error.code))
        return false
      if (error instanceof Error && /connection refused|ECONNREFUSED/i.test(error.message))
        return false
      throw error
    } finally {
      clearTimeout(timer)
    }
  }

  async probeState(
    p: ConnectionProfile,
    workspaceId: string,
    _workspacePath: string | null
  ): Promise<LocalState> {
    const [entry, probe] = await Promise.all([
      this.tracked(p, workspaceId),
      this.healthy(p).catch((e: unknown) => {
        if (e instanceof CredentialError) return e
        throw e
      })
    ])
    if (probe instanceof CredentialError) {
      // Only a tracked terminal can supply crash diagnostics. For an unmanaged
      // endpoint or a reachable process, keep the credential error visible.
      if (!entry || (await this.healthy({ ...p, auth: { mode: 'none' } }))) throw probe
    }
    const healthy = probe === true
    if (!entry) return { status: healthy ? 'attached' : 'stopped' }
    const { terminalId } = entry
    if (healthy) return { status: 'managed-running', terminalId }
    if (Date.now() - entry.startedAt < 30_000) return { status: 'starting', terminalId }
    const tail = await this.orca.host.call('terminal.read', { terminalId, maxLines: 50 })
    return {
      status: 'crashed',
      terminalId,
      diagnostics: await this.redactDiagnostics(p, tail.lines.slice(-50).join('\n'))
    }
  }

  private async action<T>(p: ConnectionProfile, workspace: string, operation: () => Promise<T>) {
    if (p.kind !== 'local' || !workspace) throw new Error('Select a local profile and workspace.')
    const id = key(p, workspace)
    const pending = (this.actions.get(id) ?? Promise.resolve()).catch(() => {}).then(operation)
    this.actions.set(id, pending)
    try {
      return await pending
    } finally {
      if (this.actions.get(id) === pending) this.actions.delete(id)
    }
  }

  private launchCommand(p: ConnectionProfile, workspacePath: string | null): LocalCommand {
    const port = localPort(p, interpolateEnv(p.url, process.env))
    if (!Number.isInteger(port) || port < 1 || port > 65535)
      throw new Error('Invalid Dagster port.')
    let command = p.local?.command?.trim() || 'uv run dagster dev'
    if (command === 'uv run dagster dev' && port !== 3000) command += ' -p ' + port
    if (p.local?.cwd) {
      const root = workspacePath || resolve('.')
      const path = relative(root, resolve(root, p.local.cwd))
      if (
        isAbsolute(p.local.cwd) ||
        isAbsolute(path) ||
        path === '..' ||
        path.startsWith('..' + sep)
      ) {
        throw new Error('Choose a working directory relative to the workspace.')
      }
    }
    const cwd = workspacePath ? resolve(workspacePath, p.local?.cwd || '.') : p.local?.cwd
    if ((p.origin === 'repo' || p.repositoryCommand) && !workspacePath)
      throw Error('Open the repository workspace to confirm its working directory.')
    return { cwd: cwd ?? '', command }
  }

  private requiresConfirmation(p: ConnectionProfile, plan: LocalCommand, confirmed?: LocalCommand) {
    return (
      (p.origin === 'repo' || !!p.repositoryCommand) &&
      !!p.local?.command?.trim() &&
      (plan.command !== confirmed?.command || plan.cwd !== confirmed?.cwd)
    )
  }

  private async create(
    p: ConnectionProfile,
    workspaceId: string,
    plan: LocalCommand
  ): Promise<LocalState> {
    const command = plan.cwd
      ? "cd -- '" + plan.cwd.replaceAll("'", "'\\''") + "' && " + plan.command
      : plan.command
    const { terminalId } = await this.orca.host.call('terminal.create', {
      workspaceId,
      title: 'Dagster: ' + p.name,
      command
    })
    try {
      await this.save(p, workspaceId, { workspaceId, terminalId, startedAt: Date.now() })
    } catch (err) {
      await this.orca.host.call('terminal.close', { terminalId })
      throw err
    }
    return { status: (await this.healthy(p)) ? 'managed-running' : 'starting', terminalId }
  }

  start(p: ConnectionProfile, workspace: string, path: string | null, confirmed?: LocalCommand) {
    return this.action(p, workspace, async () => {
      const state = await this.probeState(p, workspace, path)
      if (state.status !== 'stopped') return state
      const plan = this.launchCommand(p, path)
      if (this.requiresConfirmation(p, plan, confirmed)) return { ...state, confirmation: plan }
      return this.create(p, workspace, plan)
    })
  }

  private async close(p: ConnectionProfile, workspace: string) {
    const entry = await this.tracked(p, workspace)
    if (!entry) return false
    await this.orca.host.call('terminal.close', { terminalId: entry.terminalId })
    await this.save(p, workspace)
    return true
  }

  stop(p: ConnectionProfile, workspace: string, path: string | null): Promise<LocalState> {
    return this.action(p, workspace, async () =>
      (await this.close(p, workspace)) ? { status: 'stopped' } : this.probeState(p, workspace, path)
    )
  }

  restart(p: ConnectionProfile, workspace: string, path: string | null, confirmed?: LocalCommand) {
    return this.action(p, workspace, async () => {
      const state = await this.probeState(p, workspace, path)
      if (state.status === 'attached') return state
      const plan = this.launchCommand(p, path)
      if (this.requiresConfirmation(p, plan, confirmed)) return { ...state, confirmation: plan }
      if (state.terminalId) {
        await this.close(p, workspace)
        if (await this.healthy(p)) return { status: 'attached' } as LocalState
      }
      return this.create(p, workspace, plan)
    })
  }

  focusLogs(p: ConnectionProfile, workspace: string, _path: string | null) {
    return this.action(p, workspace, async () => {
      const entry = await this.tracked(p, workspace)
      return entry
        ? this.orca.host.call('terminal.focus', { terminalId: entry.terminalId })
        : { focused: false }
    })
  }
}

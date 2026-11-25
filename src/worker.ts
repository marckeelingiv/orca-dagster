import type { PluginRequestInput, PluginRequestOutput, PluginRequests } from 'orca/host'
import type { OrcaWorkerApi, PanelRequestContext } from 'orca/worker'
import { listTargets, sendRunError } from './agent/handoff'
import type { Explorer as DagsterClient, FlowCache } from './dagster/explorer'
import { sourceIndex, revisionDiff } from './source/index'
import { backfillKeys, partitionPage, type PartitionCatalog } from './dagster/partition-status'
import { ReloadWatcher } from './local/reload-watcher'
import { readFile, stat } from 'node:fs/promises'
import { resolve, isAbsolute, relative, sep } from 'node:path'
import { openDagsterPanel, runOrca } from './open-panel'
import { graphqlEndpoint } from './profiles/credentials'
import { parseProfile } from './profiles/store'
import { randomUUID } from 'node:crypto'
import type { GraphNode } from './types'
import { resolveRunSource } from './run-source'
import { Session, ScopeChangedError, message, type Connection } from './session'
import { openSource, resolveSource } from './source/resolver'
import type { LocalCommand, LocalState, RunDetail, Scope } from './types'

function workspaceId(connection: Connection): string {
  if (!connection.workspace.id) throw new Error('Open a workspace in Orca first.')
  return connection.workspace.id
}

/** Keep the most recent log entries that fit a panel request, including the failing traceback. */
function sidebarDetail(run: RunDetail): RunDetail {
  if (Buffer.byteLength(JSON.stringify(run)) < 950_000) return run
  const logs: RunDetail['logs'] = []
  let bytes = 0
  for (let i = run.logs.length - 1; i >= 0; i--) {
    const row = run.logs[i]
    const clipped = {
      ...row,
      message: row.message.slice(0, 12000),
      error: row.error?.slice(-12000)
    }
    bytes += Buffer.byteLength(JSON.stringify(clipped)) + 1
    if (bytes > 650_000) break
    logs.push(clipped)
  }
  return {
    ...run,
    logs: logs.reverse(),
    error: run.error?.slice(-12000),
    steps: run.steps.slice(0, 500),
    assetKeys: run.assetKeys.slice(0, 200)
  }
}

const watcher = new ReloadWatcher()
let running: Session | undefined
export function deactivate() {
  watcher.stop()
  running?.stopStartupMonitor()
  running = undefined
}

export default function activate(
  orca: OrcaWorkerApi,
  reveal = openDagsterPanel,
  fileCommand = runOrca
) {
  const session = new Session(orca)
  running = session
  const fixes = new Map<
    string,
    { revision: number; workspaceId: string | null; source?: string; stamp?: number }
  >()
  function handle<K extends keyof PluginRequests>(
    name: K,
    handler: (
      input: PluginRequestInput<K>,
      context: PanelRequestContext
    ) => PluginRequestOutput<K> | Promise<PluginRequestOutput<K>>
  ) {
    return orca.panels.handle(name, async (input, context) => {
      // Orca validates the object before transport. Omit unset optional fields at the boundary.
      return JSON.parse(JSON.stringify(await handler(input, context)))
    })
  }
  const register = orca.commands.register.bind(orca.commands)

  async function connected<T>(
    scope: Scope,
    signal: AbortSignal,
    operation: (client: DagsterClient, connection: Connection) => Promise<T>
  ) {
    const connection = await session.connection(scope)
    const client = await session.client(connection.profile, signal, undefined, undefined, () =>
      session.confirm(connection)
    )
    await session.confirm(connection)
    signal.throwIfAborted()
    return operation(client, connection)
  }
  async function reload(scope: Scope, names: string[] | undefined, signal: AbortSignal) {
    return connected(scope, signal, async (client) => {
      const locations = names ?? (await client.fetchLocationNames())
      const results = []
      for (const locationName of new Set(locations)) {
        signal.throwIfAborted()
        try {
          await client.reloadLocation(locationName)
          results.push({ locationName, ok: true })
        } catch (error) {
          results.push({ locationName, ok: false, error: message(error) })
        }
      }
      if (results.some((r) => r.ok)) orca.panels.post('dagster.definitionsChanged', scope)
      return { results }
    })
  }
  async function runDetail(
    client: DagsterClient,
    connection: Connection,
    id: string,
    errorsOnly?: boolean,
    limit?: number
  ) {
    const setting = Number(await orca.settings.get('maxRunLogLines')) || 200
    const run = await client.fetchRunDetail(id, {
      errorsOnly,
      limit: Math.min(setting, limit ?? setting, 1000)
    })
    const cached = session.cached(connection)
    // Run-read permission can be broader than catalog permission. A missing catalog still
    // permits a useful traceback and a local symbol lookup.
    const catalog =
      cached?.assets ??
      (await client.fetchSnapshot().then(
        (s) => s.assets,
        () => []
      ))
    const uiUrl = graphqlEndpoint(connection.profile, process.env).replace(/\/graphql$/, '')
    const ws = await session.sourceWorkspace(connection).catch(() => null)
    if (ws)
      run.source = await resolveRunSource({ ...connection, workspace: ws }, run, catalog, uiUrl)
    const fix = fixes.get(id)
    if (fix && fix.revision === connection.revision && fix.workspaceId === ws?.id) {
      const stamp =
        fix.source && ws?.path
          ? (await stat(resolve(ws.path, fix.source)).catch(() => null))?.mtimeMs
          : undefined
      run.agentFix = stamp !== undefined && stamp !== fix.stamp ? 'edited' : 'sent'
    }
    return run
  }
  async function local(
    action: 'start' | 'stop' | 'restart',
    scope: Scope & { confirmed?: LocalCommand }
  ): Promise<{ state: LocalState }> {
    const connection = await session.connection(scope)
    const state = await session.local[action](
      connection.profile,
      workspaceId(connection),
      connection.workspace.path,
      scope.confirmed
    )
    if (state.confirmation) return { state }
    if (state.diagnostics) state.diagnostics = state.diagnostics.slice(-12000)
    if (state.status === 'starting') session.monitorStartup(connection)
    else session.stopStartupMonitor()
    return { state }
  }

  let watcherRevision = 0
  function stopWatcher() {
    watcherRevision++
    watcher.stop()
  }
  let showSourcePicker = false
  let sourcePickerPath: string | undefined
  handle('profiles.list', async () => {
    const result = await session.store.list(await session.workspace())
    if (result.workspaceId)
      result.sourceWorkspaces = (await orca.host.call('workspace.list')).workspaces
        .filter((w) => !w.hostId || w.hostId === 'local')
        .map((w) => ({ id: w.id, name: w.name }))
    if (showSourcePicker) {
      result.showSourcePicker = true
      result.sourcePickerPath = sourcePickerPath
      showSourcePicker = false
      sourcePickerPath = undefined
    }
    result.credentialEnvPresent = Object.fromEntries(
      result.profiles
        .filter((p) => p.auth.source === 'env-var' && p.auth.envVar)
        .map((p) => [p.id, Boolean(process.env[p.auth.envVar!])])
    )
    if (session.showProfiles) {
      result.showProfiles = true
      session.showProfiles = false
    }
    return result
  })
  handle('profiles.save', async (input) => {
    const ws = await session.workspace(input)
    const saved = await session.store.save(
      ws,
      input.profile,
      input.pinToWorkspace,
      input.secretValue,
      input.duplicateFrom
    )
    stopWatcher()
    await session.profilesChanged(ws)
    return saved
  })
  handle('profiles.delete', async (input) => {
    const ws = await session.workspace(input)
    const result = await session.store.delete(ws, input.profileId)
    stopWatcher()
    await session.profilesChanged(ws)
    return result
  })
  handle('profiles.select', async (input) => {
    const ws = await session.workspace(input)
    const result = await session.store.select(ws, input.profileId, input.pinToWorkspace)
    stopWatcher()
    await session.profilesChanged(ws)
    return result
  })
  handle('profiles.test', async (input, { signal }) => {
    const start = Date.now()
    try {
      const profile = parseProfile(input.profile, true)
      const client = await session.client(profile, signal, undefined, input.secretValue)
      const version = await client.ping()
      return { ok: true, version, latencyMs: Date.now() - start }
    } catch (error) {
      return { ok: false, error: message(error), latencyMs: Date.now() - start }
    }
  })
  handle('dagster.snapshot', async (scope, { signal }) => {
    const revision = ++watcherRevision
    const stale = () => signal.aborted || revision !== watcherRevision || running !== session
    const state = await session.snapshot(scope ?? {}, signal)
    if (stale()) return state
    if (state.status === 'connected' || state.status === 'degraded') {
      let connection: Connection
      try {
        connection = await session.connection(scope)
      } catch (e) {
        if (stale() || e instanceof ScopeChangedError) return state
        throw e
      }
      if (stale()) return state
      const location = connection.profile.reloadLocationOnSave
      if (connection.profile.kind === 'local' && location) {
        const ws = await session.sourceWorkspace(connection).catch(() => null)
        if (stale()) return state
        if (ws?.path)
          watcher.configure(
            JSON.stringify([connection.revision, ws.id, location]),
            ws.path,
            async (watchSignal) => {
              const client = await session.client(
                connection.profile,
                watchSignal,
                undefined,
                undefined,
                () => session.confirm(connection)
              )
              watchSignal.throwIfAborted()
              try {
                await client.reloadLocation(location)
                await session.confirm(connection)
                orca.panels.post('dagster.definitionsChanged', scope ?? {})
              } finally {
                if (!watchSignal.aborted) await session.snapshot(scope ?? {}, watchSignal)
              }
            },
            (e) => {
              void orca.host
                .call('notifications.show', {
                  title: 'Dagster reload',
                  body: message(e).slice(0, 300)
                })
                .catch(() => {})
            }
          )
        else watcher.stop()
      } else watcher.stop()
    } else watcher.stop()
    return state
  })
  handle('dagster.reload', (input, { signal }) => reload(input, input.locationNames, signal))
  handle('dagster.runs', (input, { signal }) => connected(input, signal, (c) => c.fetchRuns(input)))
  handle('dagster.assetPartitions', (input, { signal }) =>
    connected(input, signal, (c) => c.fetchAssetPartitions(input.assetKey, input.limit))
  )
  handle('dagster.materialize', (input, { signal }) =>
    connected(input, signal, (c) => c.materializeAssets(input))
  )
  handle('dagster.launchJob', (input, { signal }) =>
    connected(input, signal, (c) => c.launchJob(input))
  )
  handle('dagster.toggleSchedule', (input, { signal }) =>
    connected(input, signal, (c) => c.toggleSchedule(input))
  )
  handle('dagster.toggleSensor', (input, { signal }) =>
    connected(input, signal, (c) => c.toggleSensor(input))
  )
  handle('dagster.runDetail', (input, { signal }) =>
    connected(input, signal, async (client, connection) => ({
      run: sidebarDetail(
        await runDetail(client, connection, input.runId, input.errorsOnly, input.limit)
      )
    }))
  )
  handle('dagster.reexecuteRun', (input, { signal }) =>
    connected(input, signal, async (c) => {
      const result = await c.reexecuteRun(input.runId, input.strategy)
      fixes.delete(input.runId)
      return result
    })
  )
  handle('dagster.terminateRun', (input, { signal }) =>
    connected(input, signal, (c) => c.terminateRun(input.runId))
  )
  handle('local.start', (input) => local('start', input))
  handle('local.stop', (input) => local('stop', input))
  handle('local.restart', (input) => local('restart', input))
  handle('local.focusLogs', async (input) => {
    const connection = await session.connection(input)
    return session.local.focusLogs(
      connection.profile,
      workspaceId(connection),
      connection.workspace.path
    )
  })
  handle('source.open', async (input) => {
    const connection = await session.connection(input)
    const { profile } = connection
    const workspace = await session.sourceWorkspace(connection)
    const url = graphqlEndpoint(profile, process.env).replace(/\/graphql$/, '')
    return openSource(orca, workspace.id ?? '', workspace.path, profile, input, url, fileCommand)
  })
  handle('agent.listTargets', async (scope) =>
    listTargets(orca, (await session.sourceWorkspace(await session.connection(scope))).id ?? '')
  )
  handle('agent.sendRunError', (input, { signal }) =>
    connected(input, signal, async (client, connection) => {
      const run = await runDetail(client, connection, input.runId, true)
      const ws = await session.sourceWorkspace(connection)
      const key = input.assetKey ?? run.assetKeys[0]
      if (key) {
        run.comparison = await comparison(client, run, key, ws.path)
        const asset = session
          .cached(connection)
          ?.assets.find(
            (a) =>
              a.locationName === run.locationName &&
              a.repositoryName === run.repositoryName &&
              JSON.stringify(a.assetKey) === JSON.stringify(key)
          )
        if (asset && ws.path)
          run.source = await resolveSource(
            ws.path,
            connection.profile,
            {
              kind: 'asset',
              name: key.at(-1)!,
              assetKey: key,
              dbtUniqueId: asset.dbtUniqueId,
              codeReferences: asset.codeReferences
            },
            graphqlEndpoint(connection.profile, process.env)
          )
      }
      if (run.source?.resolvedPath && ws.path) {
        const source = await readFile(resolve(ws.path, run.source.resolvedPath), 'utf8').catch(
          () => ''
        )
        const start = Math.max(0, (run.source.lineNumber ?? 1) - 2)
        run.comparison =
          (run.comparison ?? '') +
          '\n' +
          client
            .redact(
              source
                .split(/\r?\n/)
                .slice(start, start + 10)
                .join('\n')
            )
            .slice(0, 550)
      }
      signal.throwIfAborted()
      await session.confirm(connection)
      const source = run.source?.resolvedPath
      const stamp =
        ws.path && source
          ? (await stat(resolve(ws.path, source)).catch(() => null))?.mtimeMs
          : undefined
      const result = await sendRunError(orca, ws.id ?? '', connection.profile, run, input)
      if (fixes.size >= 64) fixes.delete(fixes.keys().next().value!)
      fixes.set(run.runId, { revision: connection.revision, workspaceId: ws.id, source, stamp })
      return result
    })
  )

  async function comparison(
    client: DagsterClient,
    run: RunDetail,
    key: string[],
    root: string | null
  ) {
    if (!run.assetKeys.some((k) => JSON.stringify(k) === JSON.stringify(key)))
      throw Error('Select an asset in the failed run.')
    const [diff, columns] = await Promise.all([
      client
        .compareFailure(run, key, (before, after) => revisionDiff(root, before, after))
        .catch(() => 'Comparison unavailable.'),
      client
        .columns(key, run.partitionKey)
        .then((h) => h.columns?.map((c) => c.name + ' ' + c.type).join(', '))
        .catch(() => undefined)
    ])
    const first = run.steps
      .filter((s) => ['FAILURE', 'FAILED'].includes(s.status))
      .sort((a, b) => (a.endTime ?? Infinity) - (b.endTime ?? Infinity))[0]
    return [
      diff,
      'First failing step: ' + (first?.key ?? 'unknown'),
      'Failed checks in this run: ' +
        (run.failedChecks
          ?.filter((c) => JSON.stringify(c.assetKey) === JSON.stringify(key))
          .map((c) => c.name)
          .join(', ') || 'none recorded'),
      'Latest columns: ' + (columns ?? 'unavailable')
    ].join('\n')
  }
  handle('dagster.compareFailure', (i, { signal }) =>
    connected(i, signal, async (c, cn) => ({
      comparison: await comparison(
        c,
        await runDetail(c, cn, i.runId, true),
        i.assetKey,
        (await session.sourceWorkspace(cn).catch(() => null))?.path ?? null
      )
    }))
  )
  // One immutable topology per request chain. Never calculate impact from a shortened sidebar snapshot.
  const graphs = new Map<string, { revision: number; nodes: GraphNode[]; flow: FlowCache }>()
  function getGraph(token: string, connection: Connection) {
    const saved = graphs.get(token)
    if (!saved || saved.revision !== connection.revision)
      throw Error('Graph expired. Refresh the DAG.')
    return saved
  }
  handle('dagster.graph', (input, { signal }) =>
    connected(input, signal, async (client, connection) => {
      let token = input.token
      if (!token) {
        const nodes = await client.graph()
        await session.confirm(connection)
        token = randomUUID()
        if (graphs.size >= 4) graphs.delete(graphs.keys().next().value!)
        graphs.set(token, { revision: connection.revision, nodes, flow: new Map() })
      }
      const { nodes } = getGraph(token, connection)
      let offset = input.offset ?? 0
      if (!Number.isSafeInteger(offset) || offset < 0 || offset > nodes.length)
        throw Error('Invalid graph cursor.')
      const page: GraphNode[] = []
      let bytes = 0
      while (offset < nodes.length) {
        const node = nodes[offset],
          size = Buffer.byteLength(JSON.stringify(node)) + 1
        if (size > 850000) throw Error('An asset definition exceeds the panel size limit.')
        if (bytes + size > 850000) break
        page.push(node)
        bytes += size
        offset++
      }
      return { nodes: page, token, ...(offset < nodes.length ? { next: offset } : {}) }
    })
  )
  handle('dagster.health', (i, { signal }) => connected(i, signal, (c) => c.health()))
  handle('source.index', (i, { signal }) =>
    connected(i, signal, async (_, cn) =>
      sourceIndex(
        (await session.sourceWorkspace(cn)).path,
        cn.profile,
        getGraph(i.token, cn).nodes,
        signal,
        graphqlEndpoint(cn.profile, process.env)
      )
    )
  )
  handle('dagster.columnFlow', (i, { signal }) =>
    connected(i, signal, (c, cn) => {
      const graph = i.token && getGraph(i.token, cn)
      const scope =
        graph && i.assetId
          ? { topology: graph.nodes, assetId: i.assetId, cache: graph.flow }
          : undefined
      return c.columnFlow(i.assetKey, i.column, i.partition, scope)
    })
  )
  handle('dagster.columns', (i, { signal }) =>
    connected(i, signal, (c) => c.columns(i.assetKey, i.partition))
  )
  // A cursor and a backfill review both refer to the immutable catalog shown in the grid.
  const grids = new Map<string, { revision: number; asset: string; catalog: PartitionCatalog }>()
  async function grid(client: DagsterClient, cn: Connection, assetKey: string[], token?: string) {
    const asset = JSON.stringify(assetKey)
    if (token) {
      const entry = grids.get(token)
      if (!entry || entry.revision !== cn.revision || entry.asset !== asset)
        throw Error('Partition grid expired. Reload the grid.')
      return { token, catalog: entry.catalog }
    }
    const catalog = await client.partitionCatalog(assetKey)
    await session.confirm(cn)
    token = randomUUID()
    if (grids.size >= 8) grids.delete(grids.keys().next().value!)
    grids.set(token, { revision: cn.revision, asset, catalog })
    return { token, catalog }
  }
  handle('dagster.partitionPage', (i, { signal }) =>
    connected(i, signal, async (c, cn) => {
      const { catalog, token } = await grid(c, cn, i.assetKey, i.token)
      return { ...partitionPage(catalog, i.cursor, i.fixed), token }
    })
  )
  handle('dagster.prepareBackfill', (i, { signal }) =>
    connected(i, signal, async (c, cn) => {
      const { catalog } = await grid(c, cn, i.assetKey, i.token)
      const keys = backfillKeys(catalog, i.selection, i.fixed)
      return { keys, targets: await c.preview({ assetKey: i.assetKey, keys }) }
    })
  )
  handle('dagster.partitionRun', (i, { signal }) =>
    connected(i, signal, (c) => c.partitionRun(i.assetKey, i.partition))
  )
  handle('dagster.previewBackfill', (i, { signal }) => connected(i, signal, (c) => c.preview(i)))
  handle('dagster.launchBackfill', (i, { signal }) =>
    connected(i, signal, (c) => c.launchBackfill(i))
  )
  handle('dagster.backfillStatus', (i, { signal }) =>
    connected(i, signal, (c) => c.backfillStatus(i.backfillId))
  )
  handle('agent.loadError', (i, { signal }) =>
    connected(i, signal, async (c, cn) => {
      const location = (await c.fetchSnapshot()).locations.find((l) => l.name === i.locationName)
      if (!location?.error) throw Error('This location has no current load error.')
      const ws = await session.sourceWorkspace(cn)
      if (!ws.id) throw Error('Select a source workspace.')
      await session.confirm(cn)
      await orca.host.call('agent.launch', {
        workspaceId: ws.id,
        agent: String((await orca.settings.get('defaultAgent')) || 'claude'),
        prompt: (
          'Fix the Dagster code location load error for ' +
          location.name +
          '\n' +
          location.error
        ).slice(0, 4096)
      })
      return { launched: true }
    })
  )
  register('dagster.showAsset', async (args) => {
    sourcePickerPath =
      args && typeof args === 'object' && 'path' in args && typeof args.path === 'string'
        ? args.path
        : undefined
    if (sourcePickerPath && isAbsolute(sourcePickerPath)) {
      const ws = await session.sourceWorkspace(await session.connection())
      if (!ws.path) throw Error('Choose a local source workspace.')
      sourcePickerPath = relative(ws.path, sourcePickerPath).split(sep).join('/')
    }
    showSourcePicker = true
    try {
      await reveal()
    } catch (error) {
      showSourcePicker = false
      sourcePickerPath = undefined
      throw error
    }
    orca.panels.post('dagster.sourcePicker', {})
    return 'Choose a source file in the DAG tab.'
  })
  register('dagster.openPanel', async () => {
    await reveal()
    return 'Opened Dagster.'
  })
  register('dagster.switchProfile', async () => {
    session.showProfiles = true
    try {
      await reveal()
      orca.panels.post('dagster.showProfiles', {})
    } catch (error) {
      session.showProfiles = false
      throw error
    }
    return 'Choose a connection profile in the sidebar.'
  })
  register('dagster.reloadDefinitions', async (_, { signal }) => {
    const { results } = await reload({}, undefined, signal)
    await session.snapshot({}, signal)
    return (
      results.map((r) => r.locationName + (r.ok ? ' reloaded' : ' · ' + r.error)).join('\n') ||
      'No code locations.'
    )
  })
  for (const action of ['start', 'stop', 'restart'] as const) {
    register('dagster.local.' + action, async (_, { signal }) => {
      const { state } = await local(action, {})
      if (state.confirmation) {
        await reveal()
        return (
          'Choose ' +
          (action === 'restart' ? 'Restart' : 'Start') +
          ' in the sidebar to review the repository command.'
        )
      }
      await session.snapshot({}, signal)
      return state.status
    })
  }
  let watcherWorkspace: string | null | undefined
  orca.events.on('workspace.changed', ({ context }) => {
    const id = context?.workspaceId ?? null
    if (id !== watcherWorkspace) {
      watcherWorkspace = id
      stopWatcher()
    }
    return session.changeWorkspace(context)
  })
}

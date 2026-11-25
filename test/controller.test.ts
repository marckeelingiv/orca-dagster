import { assert, mockHost, mountWorker, test } from 'orca/testing'
import { AuthError } from '../src/dagster/client'
import { LocalController } from '../src/local/controller'
import type { ConnectionProfile } from '../src/types'

const profile: ConnectionProfile = {
  id: 'dev',
  name: 'Dev',
  kind: 'local',
  url: 'http://localhost:3000',
  auth: { mode: 'none' }
}

test('auto-attach and workspace-scoped managed lifecycle', async () => {
  const host = mockHost({
    results: {
      'terminal.list': { terminals: [], truncated: false },
      'terminal.create': { terminalId: 'one' },
      'terminal.close': { closed: true },
      'terminal.focus': { focused: true }
    }
  })
  let healthy = true
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(orca) {
        controller = new LocalController(orca, async () => {
          if (!healthy) throw new Error('connection refused')
          return '1.11'
        })
      }
    },
    { host }
  )
  try {
    assert.equal((await controller.start(profile, 'ws', null)).status, 'attached')
    assert.equal((await controller.stop(profile, 'ws', null)).status, 'attached')
    assert.equal(host.callsOf('terminal.create').length, 0)
    healthy = false
    host.respond('terminal.list', { terminals: [{ id: 'one' }], truncated: false })
    await Promise.all([
      controller.start(profile, 'ws', null),
      controller.start(profile, 'ws', null)
    ])
    assert.equal(host.callsOf('terminal.create').length, 1)
    assert.equal((await controller.probeState(profile, 'ws', null)).status, 'starting')
    assert.equal((await controller.stop(profile, 'other', null)).status, 'stopped')
    assert.equal(host.callsOf('terminal.close').length, 0)
    assert.deepEqual(await controller.focusLogs(profile, 'ws', null), { focused: true })
    healthy = true
    assert.equal((await controller.probeState(profile, 'ws', null)).status, 'managed-running')
    healthy = false
    assert.equal((await controller.restart(profile, 'ws', null)).status, 'starting')
    assert.equal(host.callsOf('terminal.create').length, 2)
    assert.equal((await controller.stop(profile, 'ws', null)).status, 'stopped')
    assert.equal(host.callsOf('terminal.close').length, 2)
  } finally {
    await worker.deactivate()
  }
})

test('a worker restores a tracked terminal and reports its crash tail', async () => {
  const host = mockHost({
    storage: { 'local.terminals': { dev: { workspaceId: 'ws', terminalId: 'one', startedAt: 0 } } },
    results: {
      'terminal.list': { terminals: [{ id: 'one' }], truncated: false },
      'terminal.read': { lines: ['Traceback', 'ImportError'], truncated: false }
    }
  })
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(orca) {
        controller = new LocalController(orca, async () => {
          throw new Error('connection refused')
        })
      }
    },
    { host }
  )
  try {
    const state = await controller.probeState(profile, 'ws', null)
    assert.deepEqual(state, {
      status: 'crashed',
      terminalId: 'one',
      diagnostics: 'Traceback\nImportError'
    })
    assert.deepEqual(host.callsOf('terminal.read'), [{ terminalId: 'one', maxLines: 50 }])
  } finally {
    await worker.deactivate()
  }
})

test('an authorization error prevents start and restart, including on a managed terminal', async () => {
  const host = mockHost({
    results: { 'terminal.list': { terminals: [{ id: 'term-1' }], truncated: false } },
    storage: {
      'local.terminals': { dev: { workspaceId: 'w1', terminalId: 'term-1', startedAt: 1 } }
    }
  })
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(orca) {
        controller = new LocalController(orca, async () => {
          throw new AuthError('HTTP 401', 401)
        })
      }
    },
    { host }
  )
  try {
    for (const workspace of ['w1', 'external']) {
      await assert.rejects(controller.start(profile, workspace, '/workspace'), /401/)
      await assert.rejects(controller.restart(profile, workspace, '/workspace'), /401/)
    }
    assert.equal(host.callsOf('terminal.create').length, 0)
    assert.equal(host.callsOf('terminal.close').length, 0)
  } finally {
    await worker.deactivate()
  }
})

test('persists terminal ownership for workspace IDs containing paths and runs commands from that workspace', async () => {
  const path =
    "/tmp/orca/engineering/warehouse/data-platform/dagster/worktrees/release-candidate/Marc's workspace"
  const id = '550e8400-e29b-41d4-a716-446655440000::' + path
  const host = mockHost({
    results: {
      'terminal.list': { terminals: [{ id: 'term-1' }], truncated: false },
      'terminal.create': { terminalId: 'term-1' }
    }
  })
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(orca) {
        controller = new LocalController(orca, async () => {
          throw new Error('connection refused')
        })
      }
    },
    { host }
  )
  try {
    await assert.rejects(
      controller.start({ ...profile, local: { cwd: '../other-repo' } }, id, path),
      /relative to the workspace/
    )
    await controller.start(
      { ...profile, url: 'http://localhost:4300', local: { command: 'uv run dagster dev' } },
      id,
      path
    )
    const calls = host.callsOf('terminal.create') as { workspaceId: string; command: string }[]
    assert.equal(calls[0].workspaceId, id)
    assert.equal(
      calls[0].command,
      "cd -- '" + path.replace("'", "'\\''") + "' && uv run dagster dev -p 4300"
    )
    assert.equal((await controller.probeState(profile, id, path)).terminalId, 'term-1')
  } finally {
    await worker.deactivate()
  }
})

test('redacts resolved keyring credentials from crash diagnostics', async () => {
  const host = mockHost({
    storage: { 'local.terminals': { dev: { terminalId: 'one', workspaceId: 'ws', startedAt: 0 } } },
    secrets: { 'profile:dev:token': 'test-credential-value' },
    results: {
      'terminal.list': { terminals: [{ id: 'one' }], truncated: false },
      'terminal.read': { lines: ['Authorization: Bearer test-credential-value'], truncated: false }
    }
  })
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(orca) {
        controller = new LocalController(orca, async () => {
          throw new Error('connection refused')
        })
      }
    },
    { host }
  )
  try {
    const state = await controller.probeState(
      { ...profile, auth: { mode: 'bearer', source: 'orca-secret' } },
      'ws',
      null
    )
    assert.equal(state.diagnostics, 'Authorization: [redacted]')
  } finally {
    await worker.deactivate()
  }
})

test('repository start and restart require confirmation of this exact command and directory before touching terminals', async () => {
  const host = mockHost({
    results: {
      'terminal.list': { terminals: [{ id: 'owned' }], truncated: false },
      'terminal.create': { terminalId: 'owned' },
      'terminal.close': { closed: true }
    }
  })
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(api) {
        controller = new LocalController(api, async () => {
          throw Error('connection refused')
        })
      }
    },
    { host }
  )
  const p = {
    ...profile,
    origin: 'repo' as const,
    local: { command: 'uv run dagster dev -p 3800', cwd: 'project' }
  }
  try {
    const review = await controller.start(p, 'w', '/work')
    assert.deepEqual(review.confirmation, { cwd: '/work/project', command: p.local.command })
    assert.equal(host.callsOf('terminal.create').length, 0)
    const differentCwd = await controller.start(p, 'w', '/other', review.confirmation)
    assert.equal(differentCwd.confirmation?.cwd, '/other/project')
    assert.equal(host.callsOf('terminal.create').length, 0)
    await controller.start(p, 'w', '/work', review.confirmation)
    assert.equal(host.callsOf('terminal.create').length, 1)
    const edited = { ...p, local: { ...p.local, command: 'uv run dagster dev' } }
    const restart = await controller.restart(edited, 'w', '/work', review.confirmation)
    assert.equal(restart.confirmation?.command, edited.local.command)
    assert.equal(host.callsOf('terminal.close').length, 0)
    await controller.restart(edited, 'w', '/work', restart.confirmation)
    assert.equal(host.callsOf('terminal.close').length, 1)
    assert.equal(host.callsOf('terminal.create').length, 2)
  } finally {
    await worker.deactivate()
  }
})

test('probe timeout leaves a managed process starting and a missing redaction credential preserves crashed status', async () => {
  const host = mockHost({
    storage: {
      'local.terminals': { dev: { terminalId: 'one', workspaceId: 'ws', startedAt: Date.now() } }
    },
    results: {
      'terminal.list': { terminals: [{ id: 'one' }], truncated: false },
      'terminal.read': { lines: ['unredactable tail'], truncated: false }
    }
  })
  let controller!: LocalController
  const worker = await mountWorker(
    {
      default(api) {
        controller = new LocalController(api, async () => new Promise(() => {}))
      }
    },
    { host }
  )
  try {
    const p = { ...profile, auth: { mode: 'bearer' as const, source: 'orca-secret' as const } }
    assert.equal((await controller.probeState(p, 'ws', null)).status, 'starting')
    const staleHost = mockHost({
      storage: {
        'local.terminals': { dev: { terminalId: 'one', workspaceId: 'ws', startedAt: 0 } }
      },
      results: {
        'terminal.list': { terminals: [{ id: 'one' }], truncated: false },
        'terminal.read': { lines: ['unredactable tail'], truncated: false }
      }
    })
    const stale = await mountWorker(
      {
        default(api) {
          controller = new LocalController(api, async () => {
            throw Error('ECONNREFUSED')
          })
        }
      },
      { host: staleHost }
    )
    try {
      const state = await controller.probeState(p, 'ws', null)
      assert.equal(state.status, 'crashed')
      assert.ok(!state.diagnostics?.includes('unredactable tail'))
    } finally {
      await stale.deactivate()
    }
  } finally {
    await worker.deactivate()
  }
})

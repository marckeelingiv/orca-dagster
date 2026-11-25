import { assert, mockHost, mountWorker, test } from 'orca/testing'
import type { OrcaWorkerApi } from 'orca/worker'
import { listTargets, sendRunError } from '../src/agent/handoff'
import type { ConnectionProfile, RunDetail } from '../src/types'

test('launches configured agent with bounded traceback, and revalidates existing agent terminals', async () => {
  const agents = [
    {
      terminalId: 'active',
      title: 'Investigation',
      agent: 'codex',
      state: 'waiting',
      stateStartedAt: 0
    },
    { terminalId: 'old', title: null, agent: 'codex', state: 'done', stateStartedAt: 0 },
    { terminalId: 'shell', title: 'Shell', agent: null, state: 'waiting', stateStartedAt: 0 }
  ]
  const host = mockHost({
    settings: { defaultAgent: 'codex' },
    results: {
      'agents.list': { agents },
      'agent.launch': { terminalId: 'fresh' },
      'terminal.sendText': { accepted: true }
    }
  })
  let api!: OrcaWorkerApi
  const worker = await mountWorker(
    {
      default(orca) {
        api = orca
      }
    },
    { host }
  )
  const p: ConnectionProfile = {
    id: 'p',
    name: 'Dev',
    url: 'http://localhost:3000',
    kind: 'local',
    auth: { mode: 'none' }
  }
  const run: RunDetail = {
    runId: 'abc',
    pipelineName: 'orders',
    status: 'FAILURE',
    steps: [{ key: 'load', status: 'FAILURE' }],
    assetKeys: [],
    logs: [],
    error: 'Traceback\n' + 'x'.repeat(5000) + 'ValueError',
    source: { opened: false, resolvedPath: 'assets.py', lineNumber: 21 }
  }
  try {
    const targets = await listTargets(api, 'ws')
    assert.equal(targets.defaultAgent, 'codex')
    assert.deepEqual(
      targets.terminals.map((t) => t.terminalId),
      ['active']
    )
    assert.deepEqual(await sendRunError(api, 'ws', p, run, { runId: 'abc', mode: 'launch' }), {
      launched: true,
      terminalId: 'fresh'
    })
    const launch = host.callsOf('agent.launch')[0] as { agent: string; prompt: string }
    assert.equal(launch.agent, 'codex')
    assert.match(launch.prompt, /assets\.py \(line 21\)/)
    assert.match(launch.prompt, /ValueError/)
    assert.equal(launch.prompt.length, 4096)
    assert.ok(launch.prompt.split('```\n')[1].trimEnd().length <= 4000)
    await sendRunError(api, 'ws', p, run, { runId: 'abc', mode: 'existing', terminalId: 'active' })
    assert.deepEqual(host.callsOf('terminal.sendText'), [
      { terminalId: 'active', enter: true, text: launch.prompt }
    ])
    for (const [trace, expected] of [
      ['Traceback\n```\nValueError', /~~~\nTraceback\n```\nValueError\n~~~/],
      ['Traceback\n```\n~~~\n' + 'x'.repeat(6000) + 'ValueError', / {4}Traceback\n {4}```\n {4}~~~/]
    ] as const) {
      await sendRunError(
        api,
        'ws',
        p,
        { ...run, error: trace, comparison: 'c'.repeat(6000) },
        { runId: run.runId, mode: 'launch' }
      )
      const { prompt } = host.callsOf('agent.launch').at(-1) as { prompt: string }
      assert.match(prompt, expected)
      assert.match(prompt, /ValueError/)
      assert.ok(prompt.length <= 4096)
    }
    host.respond('agents.list', { agents: [] })
    await assert.rejects(
      sendRunError(api, 'other', p, run, { runId: 'abc', mode: 'existing', terminalId: 'active' }),
      /running agent/
    )
    assert.equal(host.callsOf('terminal.sendText').length, 1)
    assert.deepEqual(host.callsOf('agents.list').at(-1), { workspaceId: 'other' })
  } finally {
    await worker.deactivate()
  }
})

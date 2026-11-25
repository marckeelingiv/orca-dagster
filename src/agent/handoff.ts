import type { OrcaWorkerApi } from 'orca/worker'
import type { AgentTargets, ConnectionProfile, RunDetail, SendErrorInput } from '../types'

async function defaultAgent(orca: OrcaWorkerApi) {
  const { settings } = await orca.host.call('settings.get', {})
  return typeof settings.defaultAgent === 'string' && settings.defaultAgent.trim()
    ? settings.defaultAgent
    : 'claude'
}

async function terminals(orca: OrcaWorkerApi, workspaceId: string) {
  if (!workspaceId) throw new Error('Select a workspace for the agent.')
  const { agents } = await orca.host.call('agents.list', { workspaceId })
  return agents.flatMap(({ terminalId, title, agent, state }) =>
    terminalId && agent && state !== 'done'
      ? [{ terminalId, title: title || agent, agent, state }]
      : []
  )
}

export async function listTargets(orca: OrcaWorkerApi, workspaceId: string): Promise<AgentTargets> {
  const [agent, targets] = await Promise.all([defaultAgent(orca), terminals(orca, workspaceId)])
  return { defaultAgent: agent, terminals: targets }
}

function promptFor(profile: ConnectionProfile, run: RunDetail) {
  const steps = run.steps
    .filter((step) => ['FAILURE', 'FAILED'].includes(step.status))
    .map((step) => step.key)
  const errors = run.logs.filter((log) => log.error || ['ERROR', 'CRITICAL'].includes(log.level))
  const failing = [
    ...new Set([...steps, ...errors.flatMap((log) => (log.stepKey ? [log.stepKey] : []))])
  ]
  const trace =
    run.error ||
    errors.map((log) => log.error || log.message).join('\n') ||
    'No traceback available.'
  const source = run.source?.resolvedPath
  const header = [
    'Investigate and fix this failing Dagster run:',
    '',
    '- **Run ID**: ' + run.runId.slice(0, 100),
    '- **Target**: ' + run.pipelineName.slice(0, 200),
    '- **Profile**: ' + profile.name.slice(0, 160) + ' (' + profile.url.slice(0, 320) + ')',
    '- **Resolved Source File**: ' +
      (source || 'Unavailable').slice(0, 500) +
      (source && run.source?.lineNumber ? ' (line ' + run.source.lineNumber + ')' : ''),
    '- **Failing Step(s)**: ' + (failing.join(', ') || 'Unknown').slice(0, 400),
    '',
    ...(run.comparison ? [run.comparison.slice(0, 1500), ''] : []),
    '### Error Traceback'
  ].join('\n')
  // Pick a fence absent from the payload. Indentation handles even a traceback
  // containing arbitrarily long runs of both Markdown fence characters.
  const delimiter = !trace.includes('```') ? '```' : !trace.includes('~~~') ? '~~~' : undefined
  const footer =
    '\n\nInspect the source file, identify the root cause, and propose or implement a fix.'
  const prefix = delimiter ? header + '\n' + delimiter + '\n' : header + '\n\n'
  const suffix = (delimiter ? '\n' + delimiter : '') + footer
  const budget = Math.max(0, Math.min(4000, 4096 - prefix.length - suffix.length))
  const block = delimiter
    ? trace
    : trace
        .split('\n')
        .map((line) => '    ' + line)
        .join('\n')
  const marker = delimiter ? '\n…\n' : '\n    …\n    '
  const head = Math.floor((budget - marker.length) / 4)
  let excerpt = block
  if (block.length > budget) {
    excerpt =
      budget <= marker.length
        ? block.slice(0, budget)
        : block.slice(0, head) + marker + block.slice(-(budget - head - marker.length))
  }
  return prefix + excerpt + suffix
}

export async function sendRunError(
  orca: OrcaWorkerApi,
  workspaceId: string,
  profile: ConnectionProfile,
  run: RunDetail,
  input: SendErrorInput
) {
  if (!workspaceId || input.runId !== run.runId || run.status !== 'FAILURE')
    throw new Error('Select a failed run in this workspace.')
  const prompt = promptFor(profile, run)
  if (input.mode === 'launch') {
    const { terminalId } = await orca.host.call('agent.launch', {
      workspaceId,
      agent: await defaultAgent(orca),
      prompt
    })
    return { launched: true, terminalId }
  }
  if (
    input.mode !== 'existing' ||
    !(await terminals(orca, workspaceId)).some((t) => t.terminalId === input.terminalId)
  )
    throw new Error('Select a running agent in this workspace.')
  const { accepted } = await orca.host.call('terminal.sendText', {
    terminalId: input.terminalId!,
    text: prompt,
    enter: true
  })
  if (!accepted) throw new Error('The agent terminal did not accept the prompt.')
  return { launched: false, terminalId: input.terminalId }
}

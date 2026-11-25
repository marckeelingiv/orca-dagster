import { execFile } from 'node:child_process'
import { promisify } from 'node:util'

const exec = promisify(execFile)
type PluginListing = { pluginKey: string; status: string }

export async function runOrca(args: string[]): Promise<string> {
  const cli = process.env.ORCA_CLI_COMMAND || (process.platform === 'linux' ? 'orca-ide' : 'orca')
  const { stdout } = await exec(cli, [...args, '--environment', 'local', '--json'], {
    timeout: 8000,
    maxBuffer: 4_000_000
  })
  return stdout
}

/** Host API 1.10 has no reveal-panel method. The installed Orca CLI exposes the same sidebar action. */
export async function openDagsterPanel(
  command = (args: string[]) => runOrca(['plugin', ...args])
): Promise<void> {
  try {
    const plugins: PluginListing[] = JSON.parse(await command(['list'])).result.plugins
    const candidates = plugins.filter(
      (p) => p.pluginKey.endsWith('.dagster') && ['idle', 'running'].includes(p.status)
    )
    const selected = candidates.find((p) => p.pluginKey === 'local.dagster') ?? candidates[0]
    if (!selected) throw new Error('Dagster is not enabled.')
    await command(['open', selected.pluginKey, '--panel', 'dagster-sidebar'])
  } catch {
    throw new Error(
      'Open Dagster in Orca’s right sidebar. The Orca CLI could not reveal the panel.'
    )
  }
}

import { readFile, realpath, stat } from 'node:fs/promises'
import { isAbsolute, posix, relative, resolve, sep } from 'node:path'
import type { OrcaWorkerApi } from 'orca/worker'
import type { ConnectionProfile, SourceInput, SourceResult } from '../types'
import { dagsterUrl } from '../links'
import { runOrca } from '../open-panel'
import { scan } from './scanner'

async function localFile(
  root: string,
  path: string,
  lineNumber?: number
): Promise<SourceResult | undefined> {
  try {
    const actual = await realpath(resolve(root, path))
    const rel = relative(root, actual)
    if (
      !rel ||
      isAbsolute(rel) ||
      rel === '..' ||
      rel.startsWith('..' + sep) ||
      !(await stat(actual)).isFile()
    )
      return
    return {
      opened: false,
      resolvedPath: rel.split(sep).join('/'),
      ...(lineNumber && lineNumber > 0 ? { lineNumber } : {})
    }
  } catch {
    return
  }
}

export async function resolveSource(
  workspacePath: string | null,
  profile: ConnectionProfile,
  input: SourceInput,
  uiUrl: string,
  lookup = scan
): Promise<SourceResult> {
  const refs = input.codeReferences ?? []
  const root = workspacePath && (await realpath(workspacePath).catch(() => null))
  if (root) {
    const mappings = Object.entries(profile.local?.pathPrefixMap ?? {})
      .map(([from, to]) => ({
        from: posix.normalize(from.replaceAll('\\', '/')).replace(/\/$/, ''),
        to
      }))
      .sort((a, b) => b.from.length - a.from.length)
    for (const ref of refs) {
      if (!('filePath' in ref)) continue
      const path = posix.normalize(ref.filePath.replaceAll('\\', '/'))
      const direct = await localFile(root, path, ref.lineNumber)
      if (direct) return direct
      for (const { from, to } of mappings) {
        if (!path.startsWith(from + '/') && path !== from) continue
        const mapped = await localFile(
          root,
          resolve(root, to, '.' + path.slice(from.length)),
          ref.lineNumber
        )
        if (mapped) return mapped
      }
    }
    const symbol = await lookup(root, input)
    if (symbol) {
      const found = await localFile(root, symbol.path, symbol.lineNumber)
      if (found && input.kind === 'asset' && /\.ya?ml$/i.test(found.resolvedPath ?? '')) {
        const name = input.assetKey?.at(-1) ?? input.name
        const lines = (await readFile(resolve(root, found.resolvedPath!), 'utf8')).split(/\r?\n/)
        const hits = lines.flatMap((line, index) => {
          const match = /^\s*-\s+name:\s*["']?([^"' #]+)["']?\s*(?:#.*)?$/.exec(line)
          return match?.[1] === name ? [index + 1] : []
        })
        if (hits.length === 1) found.lineNumber = hits[0]
      }
      if (found) return found
    }
  }
  const link = refs.find((ref) => 'url' in ref && /^https?:\/\//i.test(ref.url))
  return {
    opened: false,
    externalUrl: link && 'url' in link ? link.url : dagsterUrl(uiUrl, input),
    reason: root
      ? 'Open the Orca workspace containing this deployment’s source, or map its container paths to files in this workspace.'
      : 'No local workspace is available.'
  }
}

export async function openSource(
  orca: OrcaWorkerApi,
  workspaceId: string,
  workspacePath: string | null,
  profile: ConnectionProfile,
  input: SourceInput,
  uiUrl: string,
  command = runOrca
): Promise<SourceResult> {
  const result = await resolveSource(workspacePath, profile, input, uiUrl)
  if (!result.resolvedPath) return result
  const selector = workspaceId.includes('::') ? 'id:' + workspaceId : workspaceId
  try {
    await command([
      'file',
      'open',
      '--path',
      result.resolvedPath,
      '--worktree',
      selector,
      ...(result.lineNumber ? ['--line', String(result.lineNumber)] : []),
      '--focus'
    ])
  } catch {
    // Older Orca versions can still open the file via the host API.
    await orca.host.call('editor.open', { workspaceId, path: result.resolvedPath })
  }
  return { ...result, opened: true }
}

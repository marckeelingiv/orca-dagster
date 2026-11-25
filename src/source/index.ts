import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import type { ConnectionProfile, GraphNode, SourceIndex } from '../types'
import { inventory } from './scanner'
import { relative } from 'node:path'
import { resolveSource } from './resolver'

const git = promisify(execFile)
export function changedPaths(status: string) {
  const records = status.split('\0')
  const paths = new Set<string>()
  for (let i = 0; i < records.length; i++) {
    const row = records[i]
    if (!row) continue
    const candidates = [row.slice(3)]
    if (/[RC]/.test(row.slice(0, 2))) candidates.push(records[++i])
    for (const path of candidates) if (/\.(py|sql)$/i.test(path)) paths.add(path)
  }
  return [...paths].sort()
}

export async function sourceIndex(
  root: string | null,
  profile: ConnectionProfile,
  nodes: GraphNode[],
  signal: AbortSignal,
  uiUrl = profile.url
): Promise<SourceIndex> {
  if (!root) throw Error('Select a local source worktree.')
  const files = new Map<string, Set<string>>()
  const lookup = await inventory(root, signal)
  const ambiguous = new Set<string>()
  for (const node of nodes) {
    signal.throwIfAborted()
    const refs = node.codeReferences.filter((ref) => 'filePath' in ref)
    const input = {
      kind: 'asset' as const,
      name: node.assetKey.at(-1)!,
      assetKey: node.assetKey,
      opNames: node.opNames,
      dbtUniqueId: node.dbtUniqueId
    }
    const candidates = lookup(input)
    for (const ref of refs.length && !node.dbtUniqueId ? refs : [...refs, undefined]) {
      const result = await resolveSource(
        root,
        profile,
        { ...input, codeReferences: ref ? [ref] : [] },
        uiUrl,
        async () => (candidates.length === 1 ? candidates[0] : undefined)
      )
      if (!result.resolvedPath) {
        if (candidates.length > 1)
          for (const match of candidates) ambiguous.add(relative(root, match.path))
        continue
      }
      const ids = files.get(result.resolvedPath) ?? new Set<string>()
      ids.add(node.keyString)
      files.set(result.resolvedPath, ids)
    }
  }
  const result: SourceIndex = {
    files: Array.from(files, ([path, ids]) => ({ path, assetIds: [...ids] })).sort((a, b) =>
      a.path.localeCompare(b.path)
    ),
    changed: [],
    ambiguous: [...ambiguous].sort()
  }
  try {
    const { stdout } = await git(
      'git',
      ['-C', root, 'status', '--porcelain=v1', '-z', '--untracked-files=all', '--', '.'],
      { signal, maxBuffer: 8_000_000 }
    )
    const { stdout: prefix } = await git('git', ['-C', root, 'rev-parse', '--show-prefix'], {
      signal
    })
    result.changed = changedPaths(stdout)
      .filter((p) => p.startsWith(prefix.trimEnd()))
      .map((p) => p.slice(prefix.trimEnd().length))
  } catch {
    signal.throwIfAborted()
    result.gitError = 'Git status unavailable for this worktree.'
  }
  return result
}

/** Revisions come from run tags. Only local Git object names can reach the diff command. */
export async function revisionDiff(
  root: string | null,
  before: string,
  after: string,
  signal?: AbortSignal
) {
  if (!root || before === after || ![before, after].every((ref) => /^[a-f0-9]{7,40}$/i.test(ref)))
    return ''
  try {
    const { stdout } = await git(
      'git',
      ['-C', root, 'diff', '--no-ext-diff', '--stat=80,48,8', before, after, '--'],
      { signal, timeout: 5000, maxBuffer: 128000 }
    )
    return stdout.trim().slice(0, 900)
  } catch {
    return 'Revision diff unavailable in the source worktree.'
  }
}

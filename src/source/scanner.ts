import { readdir, readFile, stat } from 'node:fs/promises'
import { basename, join, sep } from 'node:path'
import type { SourceInput } from '../types'

const skipped = new Set([
  '.git',
  '.venv',
  'node_modules',
  '__pycache__',
  '.dagster',
  'target',
  'dbt_packages',
  'build',
  'dist',
  'venv'
])
type Symbol = { kind: SourceInput['kind']; path: string[]; lineNumber: number; dbt: boolean }
const decorators: Record<string, Symbol['kind']> = {
  asset: 'asset',
  multi_asset: 'asset',
  dbt_assets: 'asset',
  job: 'job',
  schedule: 'schedule',
  sensor: 'sensor'
}
const constructors: Record<string, Symbol['kind']> = {
  AssetSpec: 'asset',
  define_asset_job: 'job',
  ScheduleDefinition: 'schedule',
  SensorDefinition: 'sensor'
}
type Token = { value: string; line: number }

function symbols(source: string) {
  const tokens: Token[] = []
  let line = 1
  // Keep quoted strings intact so parentheses, comments and decorators inside them stay inert.
  for (const match of source.matchAll(
    /#[^\n]*|"""[\s\S]*?"""|'''[\s\S]*?'''|"(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*'|\w+|\n|[^\s]/g
  )) {
    const value = match[0]
    if (value !== '\n' && !value.startsWith('#')) tokens.push({ value, line })
    line += value.split('\n').length - 1
  }
  const value = (i: number) => tokens[i]?.value
  // Pair brackets once. Skip whole nested expressions while reading top-level arguments,
  // but still visit nested constructors (e.g. @multi_asset(specs=[AssetSpec(...)])).
  const close = new Map<number, number>(),
    open: number[] = []
  for (let i = 0; i < tokens.length; i++) {
    if (['(', '[', '{'].includes(value(i))) open.push(i)
    else if ([')', ']', '}'].includes(value(i)) && open.length) close.set(open.pop()!, i)
  }
  function qualified(i: number) {
    while (value(i + 1) === '.') i += 2
    return i
  }
  function callArgs(i: number) {
    const args = new Map<string, [number, number]>()
    const end = close.get(i)
    if (end === undefined) return args
    for (let j = i + 1, position = 0; j < end; position++) {
      const key = value(j + 1) === '=' ? value(j) : String(position)
      if (value(j + 1) === '=') j += 2
      const start = j
      while (j < end && value(j) !== ',') j = (close.get(j) ?? j) + 1
      args.set(key, [start, j])
      j++
    }
    return args
  }
  // Accept literal strings, lists/tuples, and (qualified) AssetKey calls.
  // Computed Python expressions cannot reliably identify a particular asset.
  function pathLiteral(span?: [number, number]): string[] | undefined {
    if (!span) return
    let [start, end] = span
    const name = qualified(start)
    if (value(name) === 'AssetKey' && close.get(name + 1) === end - 1) {
      start = name + 2
      end--
    }
    if ((value(start) === '[' || value(start) === '(') && close.get(start) === end - 1) {
      start++
      end--
    }
    const path = []
    for (let i = start; i < end; i += 2) {
      const match = /^(["'])([^"'\\\n]+)\1$/.exec(value(i))
      if (!match || (i + 1 < end && value(i + 1) !== ',')) return
      path.push(match[2])
    }
    return path.length ? path : undefined
  }
  const result: Symbol[] = []
  for (let i = 0; i < tokens.length; i++) {
    const decorator = value(i) === '@'
    const nameAt = decorator ? qualified(i + 1) : i
    const name = value(nameAt),
      names = decorator ? decorators : constructors
    if (!Object.hasOwn(names, name)) continue
    let next = nameAt + 1
    const args = callArgs(next)
    if (value(next) === '(') next = (close.get(next) ?? tokens.length) + 1
    else if (!decorator) continue
    // Other decorators may intervene between @asset and the function.
    while (decorator && value(next) === '@') {
      next = qualified(next + 1) + 1
      if (value(next) === '(') next = (close.get(next) ?? tokens.length) + 1
    }
    if (value(next) === 'async') next++
    if (decorator && value(next) !== 'def') continue

    const explicit = args.get('key') ?? (name === 'AssetSpec' ? args.get('0') : undefined)
    const prefix = args.get('key_prefix')
    const named = args.get('name') ?? (!decorator ? args.get('0') : undefined)
    const base = named ? pathLiteral(named) : decorator ? [value(next + 1)] : undefined
    const prefixPath = prefix ? pathLiteral(prefix) : []
    const path = explicit
      ? pathLiteral(explicit)
      : base && prefixPath
        ? [...prefixPath, ...base]
        : undefined
    if (path?.length)
      result.push({
        kind: names[name],
        path,
        lineNumber: tokens[i].line,
        dbt: name === 'dbt_assets'
      })
  }
  return result
}

const cache = new Map<string, { stamp: number; size: number; symbols: Symbol[] }>()

type DbtNode = { original_file_path: string; name: string; source_name?: string }

type Definition = { path: string; lineNumber: number }
type Index = Map<string, Definition[]>
function add(index: Index, key: string, path: string, lineNumber = 1) {
  const matches = index.get(key) ?? []
  matches.push({ path, lineNumber })
  index.set(key, matches)
}

// Build once per index refresh. Parsing Python files is cached across refreshes.
export async function inventory(root: string, signal?: AbortSignal) {
  const python: Index = new Map(),
    dbt: Index = new Map(),
    models: Index = new Map()
  const projects: string[] = [],
    sql: string[] = [],
    directories = [root]
  let pythonFiles = 0
  while (directories.length) {
    signal?.throwIfAborted()
    const dir = directories.pop()!
    const entries = await readdir(dir, { withFileTypes: true }).catch(() => [])
    if (entries.some((e) => e.isFile() && e.name === 'dbt_project.yml')) projects.push(dir)
    for (const entry of entries) {
      const path = join(dir, entry.name)
      if (entry.isDirectory() && !skipped.has(entry.name)) directories.push(path)
      if (!entry.isFile()) continue
      if (entry.name.endsWith('.sql')) sql.push(path)
      if (!entry.name.endsWith('.py') || ++pythonFiles > 2000) continue
      try {
        const info = await stat(path)
        let indexed = cache.get(path)
        if (!indexed || indexed.stamp !== info.mtimeMs || indexed.size !== info.size) {
          indexed = {
            stamp: info.mtimeMs,
            size: info.size,
            symbols: symbols(await readFile(path, 'utf8'))
          }
          if (cache.size >= 4000) cache.delete(cache.keys().next().value!)
          cache.set(path, indexed)
        }
        for (const symbol of indexed.symbols) {
          add(python, JSON.stringify([symbol.kind, symbol.path]), path, symbol.lineNumber)
          if (symbol.dbt)
            add(python, JSON.stringify(['dbt_assets', symbol.path]), path, symbol.lineNumber)
        }
      } catch {
        /* A checkout may change while reading. */
      }
    }
  }
  for (const project of projects) {
    try {
      const manifest = JSON.parse(
        await readFile(join(project, 'target/manifest.json'), 'utf8')
      ) as Record<string, Record<string, DbtNode>>
      for (const section of ['nodes', 'sources', 'exposures'])
        for (const [id, node] of Object.entries(manifest[section] ?? {})) {
          if (!node.original_file_path) continue
          const path = join(project, node.original_file_path)
          add(dbt, id, path)
          if (node.source_name) add(dbt, JSON.stringify([node.source_name, node.name]), path)
        }
    } catch {
      /* Fresh checkouts can use unique SQL model names. */
    }
  }
  for (const file of sql)
    if (projects.some((p) => file.startsWith(join(p, 'models') + sep)))
      add(models, basename(file, '.sql'), file)
  return (input: SourceInput) => {
    const name = input.assetKey?.at(-1) ?? input.name
    const def =
      input.kind === 'asset'
        ? dbt.get(input.dbtUniqueId ?? JSON.stringify(input.assetKey))
        : undefined
    if (def?.length) return def
    const path = input.assetKey ?? [input.name]
    const hits = python.get(JSON.stringify([input.kind, path]))
    if (hits?.length && !input.dbtUniqueId) return hits
    if (input.kind !== 'asset') return []
    const id = input.dbtUniqueId?.split('.')
    const versioned = id?.[0] === 'model' && id.length === 4 && /^v[0-9]+$/.test(id[3])
    const modelName = versioned ? id[2] : (id?.at(-1) ?? name)
    const sql = (versioned && models.get(modelName + '_' + id[3])) || models.get(modelName)
    if (sql?.length) return sql
    return (input.opNames ?? []).flatMap(
      (op) => python.get(JSON.stringify(['dbt_assets', [op]])) ?? []
    )
  }
}
export async function scan(root: string, input: SourceInput) {
  const matches = (await inventory(root))(input)
  if (matches.length === 1) return matches[0]
}

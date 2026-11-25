import type { GraphNode as AssetSummary } from '../types'
export interface SelectionEvaluationResult {
  matchedKeys: Set<string>
  matchedAssets: AssetSummary[]
  error?: string
  isSelectionSyntax: boolean
}

type TokenKind =
  | 'LPAREN'
  | 'RPAREN'
  | 'COMMA'
  | 'PLUS'
  | 'STAR'
  | 'COLON'
  | 'AND'
  | 'OR'
  | 'NOT'
  | 'FUNC'
  | 'WORD'
  | 'STRING'
  | 'EOF'

interface Token {
  kind: TokenKind
  value: string
  pos: number
}

const SELECTION_SYNTAX_HINT_RE = /[+*(),:]|\b(?:and|or|not|roots|sinks)\b/i

export function looksLikeSelectionSyntax(raw: string): boolean {
  const trimmed = raw.trim()
  if (!trimmed) return false
  return SELECTION_SYNTAX_HINT_RE.test(trimmed)
}

function tokenize(input: string): Token[] {
  const tokens: Token[] = []
  let i = 0
  const len = input.length

  while (i < len) {
    const ch = input[i]
    if (/\s/.test(ch)) {
      i += 1
      continue
    }

    if (ch === '(') {
      tokens.push({ kind: 'LPAREN', value: '(', pos: i })
      i += 1
      continue
    }
    if (ch === ')') {
      tokens.push({ kind: 'RPAREN', value: ')', pos: i })
      i += 1
      continue
    }
    if (ch === ',') {
      tokens.push({ kind: 'COMMA', value: ',', pos: i })
      i += 1
      continue
    }
    if (ch === '+') {
      tokens.push({ kind: 'PLUS', value: '+', pos: i })
      i += 1
      continue
    }
    if (ch === '*') {
      // Distinguish standalone/traversal `*` from glob pattern inside a word (e.g. `base_*` or `*orders*`).
      // A `*` starts a word if preceded by word chars without whitespace, or followed by word chars only when it's a glob.
      // We emit STAR here and let the parser handle `*expr`, `expr*`, standalone `*`, and `*foo*` cleanly!
      tokens.push({ kind: 'STAR', value: '*', pos: i })
      i += 1
      continue
    }
    if (ch === ':') {
      tokens.push({ kind: 'COLON', value: ':', pos: i })
      i += 1
      continue
    }
    if (ch === '"' || ch === "'") {
      const quote = ch
      const start = i
      i += 1
      let buf = ''
      while (i < len && input[i] !== quote) {
        if (input[i] === '\\' && i + 1 < len) {
          buf += input[i + 1]
          i += 2
        } else {
          buf += input[i]
          i += 1
        }
      }
      if (i >= len) {
        throw new Error(`Unterminated quoted string at position ${start + 1}`)
      }
      i += 1 // closing quote
      tokens.push({ kind: 'STRING', value: buf, pos: start })
      continue
    }

    // Read bare word: letters, digits, _, -, /, ., @, =
    const start = i
    while (i < len && !/[\s(),+*:"']/.test(input[i])) {
      i += 1
    }
    const word = input.slice(start, i)
    const lower = word.toLowerCase()

    // Check if immediately followed by '(' for built-in functions roots(...) / sinks(...)
    let look = i
    while (look < len && /\s/.test(input[look])) look += 1
    if ((lower === 'roots' || lower === 'sinks') && input[look] === '(') {
      tokens.push({ kind: 'FUNC', value: lower, pos: start })
      continue
    }

    if (lower === 'and') {
      tokens.push({ kind: 'AND', value: word, pos: start })
    } else if (lower === 'or') {
      tokens.push({ kind: 'OR', value: word, pos: start })
    } else if (lower === 'not') {
      tokens.push({ kind: 'NOT', value: word, pos: start })
    } else {
      tokens.push({ kind: 'WORD', value: word, pos: start })
    }
  }

  tokens.push({ kind: 'EOF', value: '', pos: len })
  return tokens
}

// Only * is special. Match literal spans in order without regex backtracking.
function glob(pattern: string) {
  const parts = pattern.toLowerCase().split(/\*+/)
  const first = parts[0],
    last = parts.at(-1)!
  return (raw: string) => {
    const text = raw.toLowerCase()
    if (parts.length === 1) return text === first
    if (!text.startsWith(first) || !text.endsWith(last)) return false
    let pos = first.length
    const end = text.length - last.length
    for (let i = 1; i < parts.length - 1; i++) {
      pos = text.indexOf(parts[i], pos)
      if (pos < 0) return false
      pos += parts[i].length
      if (pos > end) return false
    }
    return pos <= end
  }
}

type Overlay = { changed?: Set<string>; failedChecks?: Set<string> }
interface GraphContext extends Overlay {
  assets: readonly AssetSummary[]
  allKeys: Set<string>
  upstream: Map<string, Set<string>>
  downstream: Map<string, Set<string>>
}

function buildGraphContext(assets: readonly AssetSummary[], overlay: Overlay): GraphContext {
  const allKeys = new Set<string>()
  const upstream = new Map<string, Set<string>>()
  const downstream = new Map<string, Set<string>>()

  for (const a of assets) {
    allKeys.add(a.keyString)
    upstream.set(a.keyString, new Set())
    downstream.set(a.keyString, new Set())
  }

  for (const a of assets) {
    for (const dep of a.dependencyKeys ?? []) {
      if (allKeys.has(dep)) {
        upstream.get(a.keyString)!.add(dep)
        downstream.get(dep)!.add(a.keyString)
      }
    }
    for (const child of a.dependedByKeys ?? []) {
      if (allKeys.has(child)) {
        downstream.get(a.keyString)!.add(child)
        upstream.get(child)!.add(a.keyString)
      }
    }
  }

  return { ...overlay, assets, allKeys, upstream, downstream }
}

function expandNeighbors(
  seed: Set<string>,
  adjacency: Map<string, Set<string>>,
  maxDepth: number
): Set<string> {
  const result = new Set<string>(seed)
  if (maxDepth === 0) return result

  let frontier = new Set<string>(seed)
  let depth = 0

  while (frontier.size > 0 && depth < maxDepth) {
    const next = new Set<string>()
    for (const key of frontier) {
      const neighbors = adjacency.get(key)
      if (!neighbors) continue
      for (const nb of neighbors) {
        if (!result.has(nb)) {
          result.add(nb)
          next.add(nb)
        }
      }
    }
    frontier = next
    depth += 1
  }

  return result
}

function setUnion(a: Set<string>, b: Set<string>): Set<string> {
  const out = new Set<string>(a)
  for (const v of b) out.add(v)
  return out
}

function setIntersect(a: Set<string>, b: Set<string>): Set<string> {
  const out = new Set<string>()
  for (const v of a) {
    if (b.has(v)) out.add(v)
  }
  return out
}

function setComplement(a: Set<string>, universe: Set<string>): Set<string> {
  const out = new Set<string>()
  for (const v of universe) {
    if (!a.has(v)) out.add(v)
  }
  return out
}

function matchKeyPattern(pattern: string, ctx: GraphContext): Set<string> {
  const out = new Set<string>()
  const trimmed = pattern.trim()
  if (!trimmed || trimmed === '*') {
    return new Set(ctx.allKeys)
  }
  const matches = glob(trimmed)
  for (const asset of ctx.assets) {
    if (matches(asset.assetKey.join('/')) || matches(asset.assetKey.at(-1) ?? ''))
      out.add(asset.keyString)
  }
  return out
}

function matchAttribute(attr: string, rawVal: string, ctx: GraphContext): Set<string> {
  const key = attr.toLowerCase()
  const val = rawVal.trim()
  const matches = glob(val)
  const out = new Set<string>()

  switch (key) {
    case 'key':
    case 'key_substring': {
      if (key === 'key_substring') {
        const sub = val.toLowerCase()
        for (const a of ctx.assets) {
          if (a.assetKey.join('/').toLowerCase().includes(sub)) out.add(a.keyString)
        }
        return out
      }
      return matchKeyPattern(val, ctx)
    }
    case 'group': {
      for (const a of ctx.assets) {
        if (matches(a.groupName)) out.add(a.keyString)
      }
      return out
    }
    case 'kind': {
      for (const a of ctx.assets) {
        const kinds = a.kinds ?? []
        if (kinds.some((k) => matches(k))) out.add(a.keyString)
      }
      return out
    }
    case 'tag': {
      const eq = val.indexOf('=')
      if (eq >= 0) {
        const tKey = val.slice(0, eq).trim()
        const tVal = val.slice(eq + 1).trim()
        const keyMatches = glob(tKey)
        const valueMatches = glob(tVal)
        for (const a of ctx.assets) {
          const entries = Object.entries(a.tags ?? {})
          if (entries.some(([k, v]) => keyMatches(k) && valueMatches(v))) {
            out.add(a.keyString)
          }
        }
      } else {
        for (const a of ctx.assets) {
          const keys = Object.keys(a.tags ?? {})
          if (keys.some((k) => matches(k))) out.add(a.keyString)
        }
      }
      return out
    }
    case 'owner': {
      for (const a of ctx.assets) {
        const owners = a.owners ?? []
        if (owners.some((o) => matches(o) || matches(o.replace(/^team:/i, '')))) {
          out.add(a.keyString)
        }
      }
      return out
    }
    case 'code_location':
    case 'location': {
      for (const a of ctx.assets) {
        if (matches(a.locationName)) out.add(a.keyString)
      }
      return out
    }
    case 'checks':
      return val.toLowerCase() === 'failed' ? new Set(ctx.failedChecks) : out
    case 'status': {
      const target = val.toLowerCase()
      for (const a of ctx.assets) {
        if (target === 'stale' && a.staleStatus === 'STALE') out.add(a.keyString)
        if (
          (target === 'failed' || target === 'failure' || target === 'danger') &&
          a.status === 'danger'
        ) {
          out.add(a.keyString)
        } else if (
          (target === 'materialized' || target === 'ok' || target === 'success') &&
          a.status === 'success'
        ) {
          out.add(a.keyString)
        } else if (
          ((target === 'running' || target === 'materializing') &&
            (a.running ?? (a.status === 'warning' && a.staleStatus !== 'STALE'))) ||
          (target === 'warning' && a.status === 'warning')
        ) {
          out.add(a.keyString)
        } else if (
          (target === 'missing' || target === 'unmaterialized' || target === 'neutral') &&
          a.status === 'neutral'
        ) {
          out.add(a.keyString)
        }
      }
      return out
    }
    case 'partitioned': {
      const want = val.toLowerCase() !== 'false' && val !== '0' && val.toLowerCase() !== 'no'
      for (const a of ctx.assets) {
        if (a.isPartitioned === want) out.add(a.keyString)
      }
      return out
    }
    default:
      throw new Error(
        `Unknown selector "${attr}:". Supported: key, group, kind, tag, owner, code_location, status, partitioned.`
      )
  }
}

class SelectionParser {
  private readonly tokens: Token[]
  private readonly ctx: GraphContext
  private idx = 0

  constructor(tokens: Token[], ctx: GraphContext) {
    this.tokens = tokens
    this.ctx = ctx
  }

  private peek(offset = 0): Token {
    return this.tokens[Math.min(this.idx + offset, this.tokens.length - 1)]
  }

  private consume(): Token {
    const tok = this.peek()
    if (tok.kind !== 'EOF') this.idx += 1
    return tok
  }

  private canStartFactor(tok: Token): boolean {
    return (
      tok.kind === 'NOT' ||
      tok.kind === 'LPAREN' ||
      tok.kind === 'FUNC' ||
      tok.kind === 'PLUS' ||
      tok.kind === 'STAR' ||
      tok.kind === 'WORD' ||
      tok.kind === 'STRING'
    )
  }

  parse(): Set<string> {
    if (this.peek().kind === 'EOF') {
      return new Set(this.ctx.allKeys)
    }
    const res = this.parseOr()
    if (this.peek().kind !== 'EOF') {
      throw new Error(`Unexpected token "${this.peek().value}" at position ${this.peek().pos + 1}`)
    }
    return res
  }

  private parseOr(): Set<string> {
    let left = this.parseAnd()
    while (true) {
      const tok = this.peek()
      if (tok.kind === 'OR' || tok.kind === 'COMMA') {
        this.consume()
        if (this.peek().kind === 'EOF' || this.peek().kind === 'RPAREN') {
          break
        }
        const right = this.parseAnd()
        left = setUnion(left, right)
      } else if (this.canStartFactor(tok)) {
        // Space-separated terms act as union in Dagster selection syntax.
        const right = this.parseAnd()
        left = setUnion(left, right)
      } else {
        break
      }
    }
    return left
  }

  private parseAnd(): Set<string> {
    let left = this.parseNot()
    while (this.peek().kind === 'AND') {
      this.consume()
      const right = this.parseNot()
      left = setIntersect(left, right)
    }
    return left
  }

  private parseNot(): Set<string> {
    if (this.peek().kind === 'NOT') {
      this.consume()
      const inner = this.parseNot()
      return setComplement(inner, this.ctx.allKeys)
    }
    return this.parseTraversal()
  }

  private parseTraversal(): Set<string> {
    // Leading upstream operators: `+`, `2+`, `++`, `*`
    let upDepth = 0

    if (
      this.peek().kind === 'WORD' &&
      /^\d+$/.test(this.peek().value) &&
      this.peek(1).kind === 'PLUS' &&
      this.peek(1).pos === this.peek().pos + this.peek().value.length
    ) {
      upDepth = Number(this.consume().value)
      this.consume() // PLUS
    } else if (this.peek().kind === 'PLUS') {
      while (this.peek().kind === 'PLUS') {
        this.consume()
        upDepth += 1
      }
    } else if (this.peek().kind === 'STAR') {
      const starTok = this.peek()
      const nextTok = this.peek(1)
      // Standalone `*` (followed by EOF / RPAREN / COMMA / OR / AND) means "all assets"
      if (
        nextTok.kind === 'EOF' ||
        nextTok.kind === 'RPAREN' ||
        nextTok.kind === 'COMMA' ||
        nextTok.kind === 'OR' ||
        nextTok.kind === 'AND'
      ) {
        this.consume()
        return new Set(this.ctx.allKeys)
      }
      // If immediately adjacent to a WORD without space, check if it's `*asset` (upstream traversal) vs `*sub*` glob.
      // In Dagster, `*asset` means "all upstream of asset", while `key:*sub*` uses `key:` prefix.
      // However, if someone writes `*orders*` with trailing `*` right after word, we check if an exact asset matches `orders`;
      // if so, `*orders*` is upstream+downstream of `orders`! That's actually standard Dagster syntax!
      if (
        nextTok.pos === starTok.pos + 1 &&
        (nextTok.kind === 'WORD' ||
          nextTok.kind === 'STRING' ||
          nextTok.kind === 'LPAREN' ||
          nextTok.kind === 'FUNC')
      ) {
        this.consume()
        upDepth = Number.POSITIVE_INFINITY
      } else {
        this.consume()
        upDepth = Number.POSITIVE_INFINITY
      }
    }

    const base = this.parsePrimary()

    // Trailing downstream operators: `+`, `+2`, `++`, `*`
    let downDepth = 0
    if (this.peek().kind === 'PLUS') {
      while (this.peek().kind === 'PLUS') {
        this.consume()
        downDepth += 1
      }
      if (downDepth === 1 && this.peek().kind === 'WORD' && /^\d+$/.test(this.peek().value)) {
        downDepth = Number(this.consume().value)
      }
    } else if (this.peek().kind === 'STAR') {
      // Only treat trailing `*` as downstream traversal if immediately adjacent to previous token (no whitespace)
      const prev = this.tokens[this.idx - 1]
      const cur = this.peek()
      const prevEnd = prev ? prev.pos + prev.value.length + (prev.kind === 'STRING' ? 2 : 0) : -1
      if (cur.pos === prevEnd) {
        this.consume()
        downDepth = Number.POSITIVE_INFINITY
      }
    }

    let result = base
    if (upDepth > 0) {
      result = setUnion(result, expandNeighbors(base, this.ctx.upstream, upDepth))
    }
    if (downDepth > 0) {
      result = setUnion(result, expandNeighbors(base, this.ctx.downstream, downDepth))
    }
    return result
  }

  private readPatternValue(): string {
    const tok = this.peek()
    if (tok.kind === 'STRING') {
      this.consume()
      return tok.value
    }
    if (tok.kind !== 'WORD' && tok.kind !== 'STAR') {
      throw new Error(`Expected selector value at position ${tok.pos + 1}`)
    }
    // Concatenate adjacent WORD / STAR / COLON segments (e.g. `team:data*`, `base_*`, `*foo*`)
    let value = ''
    let lastEnd = -1
    while (true) {
      const cur = this.peek()
      if (cur.kind !== 'WORD' && cur.kind !== 'STAR' && cur.kind !== 'COLON') {
        break
      }
      if (lastEnd !== -1 && cur.pos !== lastEnd) {
        break
      }
      this.consume()
      value += cur.value
      lastEnd = cur.pos + cur.value.length
    }
    return value
  }

  private parsePrimary(): Set<string> {
    const tok = this.peek()

    if (tok.kind === 'LPAREN') {
      this.consume()
      const inner = this.parseOr()
      if (this.peek().kind !== 'RPAREN') {
        throw new Error(`Expected ")" at position ${this.peek().pos + 1}`)
      }
      this.consume()
      return inner
    }

    if (tok.kind === 'FUNC') {
      const fn = this.consume().value
      if (this.peek().kind !== 'LPAREN') {
        throw new Error(`Expected "(" after ${fn} at position ${this.peek().pos + 1}`)
      }
      this.consume()
      const inner = this.parseOr()
      if (this.peek().kind !== 'RPAREN') {
        throw new Error(`Expected ")" to close ${fn}(...) at position ${this.peek().pos + 1}`)
      }
      this.consume()

      const out = new Set<string>()
      if (fn === 'roots') {
        for (const key of inner) {
          const ups = this.ctx.upstream.get(key)
          const hasUpstreamInSet = ups ? [...ups].some((u) => inner.has(u)) : false
          if (!hasUpstreamInSet) out.add(key)
        }
      } else if (fn === 'sinks') {
        for (const key of inner) {
          const downs = this.ctx.downstream.get(key)
          const hasDownstreamInSet = downs ? [...downs].some((d) => inner.has(d)) : false
          if (!hasDownstreamInSet) out.add(key)
        }
      }
      return out
    }

    if (tok.kind === 'STRING') {
      this.consume()
      return matchKeyPattern(tok.value, this.ctx)
    }

    if (tok.kind === 'WORD') {
      // Check for attribute selector: `attr:value`
      if (this.peek(1).kind === 'COLON' && this.peek(1).pos === tok.pos + tok.value.length) {
        const attr = this.consume().value
        this.consume() // COLON
        const val = this.readPatternValue()
        return matchAttribute(attr, val, this.ctx)
      }

      // Bare asset key or glob (e.g. `base_*`, `raw_orders`)
      const word = this.consume().value
      if (word.toLowerCase() === 'changed' && this.ctx.changed) return new Set(this.ctx.changed)
      // If followed immediately by `*` AND another WORD (like `base_*_v2`), combine as glob
      let pattern = word
      let lastEnd = tok.pos + word.length
      while (
        this.peek().kind === 'STAR' &&
        this.peek().pos === lastEnd &&
        this.peek(1).kind === 'WORD' &&
        this.peek(1).pos === this.peek().pos + 1
      ) {
        const star = this.consume()
        const nextWord = this.consume()
        pattern += star.value + nextWord.value
        lastEnd = nextWord.pos + nextWord.value.length
      }

      // Check for prefix wildcard like `base_*` (trailing `*` when no exact key matches `base_`)
      if (
        this.peek().kind === 'STAR' &&
        this.peek().pos === lastEnd &&
        matchKeyPattern(pattern, this.ctx).size === 0
      ) {
        const star = this.consume()
        pattern += star.value
      }

      return matchKeyPattern(pattern, this.ctx)
    }

    throw new Error(`Unexpected token "${tok.value || 'end of input'}" at position ${tok.pos + 1}`)
  }
}

/**
 * Evaluates a Dagster asset selection expression against a list of assets.
 * If the input is a plain search string (no selection operators) and does not exactly match
 * an asset key, falls back to substring matching across key, group, description, and kinds.
 */
export function evaluateAssetSelection(
  rawQuery: string,
  assets: readonly AssetSummary[],
  overlay: Overlay = {}
): SelectionEvaluationResult {
  const trimmed = rawQuery.trim()
  if (!trimmed) {
    return {
      matchedKeys: new Set(assets.map((a) => a.keyString)),
      matchedAssets: [...assets],
      isSelectionSyntax: false
    }
  }

  const syntaxMode =
    looksLikeSelectionSyntax(trimmed) ||
    (Boolean(overlay.changed) && trimmed.toLowerCase() === 'changed')
  const ctx = buildGraphContext(assets, overlay)

  try {
    const tokens = tokenize(trimmed)
    const parser = new SelectionParser(tokens, ctx)
    const matchedKeys = parser.parse()

    // If user typed a plain partial search term (no operators) and exact key match found 0 results,
    // fall back gracefully to substring search so quick typing still filters assets naturally.
    if (!syntaxMode && matchedKeys.size === 0) {
      const q = trimmed.toLowerCase()
      const subMatched = assets.filter(
        (a) =>
          a.assetKey.join('/').toLowerCase().includes(q) ||
          a.groupName.toLowerCase().includes(q) ||
          (a.description ?? '').toLowerCase().includes(q) ||
          (a.kinds ?? []).some((k) => k.toLowerCase().includes(q))
      )
      return {
        matchedKeys: new Set(subMatched.map((a) => a.keyString)),
        matchedAssets: subMatched,
        isSelectionSyntax: false
      }
    }

    return {
      matchedKeys,
      matchedAssets: assets.filter((a) => matchedKeys.has(a.keyString)),
      isSelectionSyntax: syntaxMode || matchedKeys.size > 0
    }
  } catch (err) {
    return {
      matchedKeys: new Set(),
      matchedAssets: [],
      error: err instanceof Error ? err.message : String(err),
      isSelectionSyntax: true
    }
  }
}

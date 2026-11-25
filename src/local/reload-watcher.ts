import { watch, type FSWatcher } from 'node:fs'
import { lstat, readdir } from 'node:fs/promises'
import { join, sep } from 'node:path'

const ignored = new Set(['.git', '.venv', 'node_modules', 'target', 'dbt_packages', '__pycache__'])
const sourceFile = /\.(py|sql|ya?ml)$/

export class ReloadWatcher {
  private watchers = new Map<string, FSWatcher>()
  private abort?: AbortController
  private timer?: NodeJS.Timeout
  private key = ''
  private generation = 0
  private queue = Promise.resolve()
  stop() {
    this.generation++
    this.abort?.abort()
    this.key = ''
    clearTimeout(this.timer)
    for (const watcher of this.watchers.values()) watcher.close()
    this.watchers.clear()
  }
  configure(
    key: string,
    root: string,
    reload: (signal: AbortSignal) => Promise<void>,
    error: (e: unknown) => void
  ) {
    if (this.key === key) return
    this.stop()
    this.key = key
    this.abort = new AbortController()
    const { signal } = this.abort
    const generation = this.generation
    const current = () => generation === this.generation
    const fail = (e: unknown) => {
      if (!current()) return
      this.stop()
      error(e)
    }
    const changed = () => {
      clearTimeout(this.timer)
      this.timer = setTimeout(() => {
        this.queue = this.queue
          .then(async () => {
            if (current()) await reload(signal)
          })
          .catch((e) => {
            if (current()) error(e)
          })
      }, 400).unref()
    }
    const vanished = (e: unknown) =>
      ['ENOENT', 'ENOTDIR'].includes((e as NodeJS.ErrnoException).code ?? '')
    const detach = (path: string) => {
      for (const [dir, watcher] of this.watchers) {
        if (dir !== path && !dir.startsWith(path + sep)) continue
        watcher.close()
        this.watchers.delete(dir)
      }
    }
    // Attach before reading children so directory creation during the walk is observed.
    const attach = async (dir: string, movedIn = false) => {
      if (!current() || this.watchers.has(dir)) return
      try {
        const watcher = watch(dir, { persistent: false }, (event, file) => {
          if (!attached() || !file || ignored.has(file)) return
          if (sourceFile.test(file)) changed()
          if (event !== 'rename') return
          const path = join(dir, file)
          if (this.watchers.has(path)) {
            detach(path)
            changed()
          }
          // A rename may replace an inode, or move in an entire populated subtree.
          void lstat(path)
            .then((info) => {
              if (attached() && info.isDirectory()) return attach(path, true)
            })
            .catch((e) => {
              if (attached() && !vanished(e)) fail(e)
            })
        })
        this.watchers.set(dir, watcher)
        const attached = () => current() && this.watchers.get(dir) === watcher
        watcher.on('error', (e) => {
          if (attached()) fail(e)
        })
        const entries = await readdir(dir, { withFileTypes: true })
        for (const entry of entries) {
          if (!attached()) return
          if (entry.isDirectory() && !ignored.has(entry.name))
            await attach(join(dir, entry.name), movedIn)
          else if (movedIn && entry.isFile() && sourceFile.test(entry.name)) changed()
        }
      } catch (e) {
        if (!current()) return
        if (dir === root || !vanished(e)) fail(e)
      }
    }
    return attach(root)
  }
}

import { readFile } from 'node:fs/promises'
import { join } from 'node:path'
import type { OrcaWorkerApi } from 'orca/worker'
import type { ConnectionProfile, ProfileList } from '../types'
import { keyFor, normalizeGraphqlUrl } from './credentials'

export type Workspace = { id: string | null; path: string | null }
type Services = Pick<OrcaWorkerApi, 'storage' | 'secrets' | 'host'>
type RepoDefaults = { profiles: ConnectionProfile[]; defaultProfile?: string; warning?: string }

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}
function fields(value: unknown, allowed: string[]): value is Record<string, unknown> {
  return record(value) && Object.keys(value).every((key) => allowed.includes(key))
}
function optionalString(value: unknown): boolean {
  return value === undefined || typeof value === 'string'
}
function isStringMap(value: unknown): value is Record<string, string> {
  return record(value) && Object.values(value).every((v) => typeof v === 'string')
}

/** Allowlist every nested property before a profile reaches storage or the panel. */
export function parseProfile(value: unknown, fromPanel = false): ConnectionProfile {
  const allowed = [
    'id',
    'name',
    'kind',
    'url',
    'auth',
    'local',
    'sourceWorkspaceId',
    'reloadLocationOnSave'
  ]
  if (fromPanel) allowed.push('origin', 'repositoryCommand')
  if (!fields(value, allowed))
    throw new Error(
      'Unsupported profile fields. Store credentials using a keyring or environment reference.'
    )
  if (typeof value.id !== 'string' || !/^[a-z0-9][a-z0-9_-]{0,63}$/.test(value.id)) {
    throw new Error(
      'Enter a profile ID of up to 64 lowercase letters, numbers, dashes, or underscores.'
    )
  }
  if (typeof value.name !== 'string' || !value.name.trim() || value.name.length > 80)
    throw new Error('Enter a profile name of 1–80 characters.')
  if (!['local', 'remote-oss', 'dagster-plus'].some((kind) => kind === value.kind))
    throw new Error('Choose a supported profile kind.')
  if (typeof value.url !== 'string' || !value.url.trim()) throw new Error('Enter a Dagster URL.')
  const sampleUrl = value.url
    .replace(/^\$\{[A-Za-z_][A-Za-z0-9_]*\}(?=:\/\/)/, 'https')
    .replace(/^\$\{[A-Za-z_][A-Za-z0-9_]*\}/, 'https://example.org')
    .replace(/\$\{[A-Za-z_][A-Za-z0-9_]*\}/g, '1')
  normalizeGraphqlUrl(sampleUrl)
  const auth = value.auth
  if (!fields(auth, ['mode', 'source', 'envVar', 'secretKey', 'username', 'headerName']))
    throw new Error('Auth accepts credential references only.')
  if (
    !['none', 'bearer', 'basic', 'dagster-plus-token', 'custom-header'].some(
      (mode) => mode === auth.mode
    )
  )
    throw new Error('Choose a supported auth mode.')
  if (auth.source !== undefined && auth.source !== 'env-var' && auth.source !== 'orca-secret')
    throw new Error('Choose a credential source.')
  if (![auth.secretKey, auth.headerName, auth.username].every(optionalString))
    throw new Error('Invalid credential reference.')
  if (
    auth.envVar !== undefined &&
    (typeof auth.envVar !== 'string' || !/^[A-Z_][A-Z0-9_]*$/.test(auth.envVar))
  )
    throw new Error('Invalid environment variable name.')
  if (auth.mode !== 'none' && (!auth.source || (auth.source === 'env-var' && !auth.envVar)))
    throw new Error('Choose a credential source and enter its reference.')
  if (auth.mode === 'basic' && !auth.username) throw new Error('Enter the Basic auth username.')
  if (auth.mode === 'custom-header' && !auth.headerName)
    throw new Error('Enter the custom header name.')

  if (![value.sourceWorkspaceId, value.reloadLocationOnSave].every(optionalString))
    throw Error('Invalid source workspace or reload location.')
  const local = value.local
  if (local !== undefined) {
    if (
      !fields(local, ['command', 'port', 'cwd', 'pathPrefixMap']) ||
      ![local.command, local.cwd].every(optionalString)
    )
      throw new Error('Invalid local profile configuration.')
    if (
      local.port !== undefined &&
      (typeof local.port !== 'number' ||
        !Number.isInteger(local.port) ||
        local.port < 1 ||
        local.port > 65535)
    )
      throw new Error('Enter a port from 1 to 65535.')
    if (local.pathPrefixMap !== undefined && !isStringMap(local.pathPrefixMap))
      throw new Error('Path mappings must map prefixes to workspace paths.')
  }
  const { origin: _origin, repositoryCommand: _provenance, ...profile } = value
  const parsed = profile as ConnectionProfile
  if (parsed.auth.source === 'orca-secret') keyFor(parsed)
  return parsed
}

async function readRepo(workspacePath: string | null): Promise<RepoDefaults> {
  if (!workspacePath) return { profiles: [] }
  let source: string
  try {
    source = await readFile(join(workspacePath, '.orca-dagster.json'), 'utf8')
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'ENOENT'
      ? { profiles: [] }
      : { profiles: [], warning: 'Could not read .orca-dagster.json.' }
  }
  try {
    let config: unknown
    try {
      config = JSON.parse(source)
    } catch {
      throw new Error('Invalid JSON.')
    }
    if (
      !fields(config, ['$schema', 'defaultProfile', 'profiles']) ||
      !Array.isArray(config.profiles) ||
      !config.profiles.length ||
      !optionalString(config.$schema) ||
      !optionalString(config.defaultProfile)
    ) {
      throw new Error('Expected a non-empty profiles array and supported configuration fields.')
    }
    const profiles = config.profiles.map((p) => parseProfile(p))
    if (profiles.some((p) => p.sourceWorkspaceId || p.reloadLocationOnSave))
      throw Error('Set source worktree and reload-on-save preferences in Manage profiles.')
    if (new Set(profiles.map((p) => p.id)).size !== profiles.length)
      throw new Error('Profile IDs must be unique.')
    const defaultProfile = config.defaultProfile as string | undefined
    if (defaultProfile && !profiles.some((p) => p.id === defaultProfile))
      throw new Error('The default profile ID is missing.')
    return { profiles, defaultProfile }
  } catch (error) {
    return { profiles: [], warning: '.orca-dagster.json: ' + (error as Error).message }
  }
}

const USERS = 'profiles.user'
const PINS = 'profiles.pins'
const ACTIVE = 'profiles.active'

type Store<T> = {
  get(key: string): Promise<T | null>
  set(key: string, value: T): Promise<void>
  delete(key: string): Promise<void>
}

// Orca exposes individual key writes. Record the old value before each call,
// including calls that could fail after the host applied the change.
class ProfileEdit {
  private readonly undo: Array<() => Promise<void>> = []
  async write<T>(store: Store<T>, key: string, value: T | null) {
    const before = await store.get(key)
    this.undo.push(() => (before === null ? store.delete(key) : store.set(key, before)))
    if (value === null) await store.delete(key)
    else await store.set(key, value)
  }
  async restore() {
    try {
      while (this.undo.length) {
        await this.undo[this.undo.length - 1]()
        this.undo.pop()
      }
    } catch {
      throw new Error('Profile update failed. Retry after storage and keyring access are restored.')
    }
  }
}

export class ProfileStore {
  private writes: Promise<void> = Promise.resolve()
  private recovery?: ProfileEdit
  private selection: { workspaceId: string | null; id: string } | undefined

  constructor(private readonly orca: Services) {}

  private serialize<T>(operation: (edit: ProfileEdit) => Promise<T>): Promise<T> {
    const result = this.writes.then(async () => {
      if (this.recovery) {
        await this.recovery.restore()
        this.recovery = undefined
      }
      const edit = new ProfileEdit()
      const selection = this.selection
      try {
        return await operation(edit)
      } catch (error) {
        this.selection = selection
        this.recovery = edit
        await edit.restore()
        this.recovery = undefined
        throw error
      }
    })
    this.writes = result.then(
      () => {},
      () => {}
    )
    return result
  }

  workspaceChanged() {
    this.selection = undefined
  }

  private async users() {
    const stored = await this.orca.storage.get(USERS)
    const records = Array.isArray(stored) ? stored : []
    const users: ConnectionProfile[] = []
    for (const item of records) {
      try {
        if (!record(item)) continue
        const { repositoryCommand, ...fields } = item
        const profile = parseProfile(fields)
        if (repositoryCommand === true) profile.repositoryCommand = true
        users.push(profile)
      } catch {
        /* Preserve malformed records in storage while keeping valid connections usable. */
      }
    }
    return { users, records }
  }
  private async pins(): Promise<Record<string, string>> {
    const stored = await this.orca.storage.get(PINS)
    return isStringMap(stored) ? stored : {}
  }
  private async merged(workspace: Workspace) {
    const [repo, { users, records }] = await Promise.all([readRepo(workspace.path), this.users()])
    const byId = new Map<string, ConnectionProfile>()
    for (const profile of repo.profiles) byId.set(profile.id, { ...profile, origin: 'repo' })
    for (const profile of users) {
      // Migrate existing overrides whose launch command still comes from this checkout.
      const command = profile.local?.command?.trim()
      if (command && byId.get(profile.id)?.local?.command?.trim() === command)
        profile.repositoryCommand = true
      byId.set(profile.id, { ...profile, origin: 'user' })
    }
    return { repo, users, records, byId }
  }
  private async keyring(): Promise<ProfileList['keyringStatus']> {
    const result = await this.orca.host.call('secrets.protection')
    return result?.status === 'os-keyring' ? 'os-keyring' : 'unavailable'
  }
  list(workspace: Workspace): Promise<ProfileList> {
    return this.serialize(() => this.readList(workspace))
  }
  private async readList(workspace: Workspace): Promise<ProfileList> {
    const [{ byId, repo }, pins, last, keyringStatus] = await Promise.all([
      this.merged(workspace),
      this.pins(),
      this.orca.storage.get(ACTIVE),
      this.keyring()
    ])
    const pinned = workspace.id && pins[workspace.id]
    const candidates = [
      this.selection?.workspaceId === workspace.id ? this.selection.id : null,
      pinned,
      repo.defaultProfile,
      last
    ]
    const profiles = Array.from(byId.values())
    const active = candidates.find((id): id is string => typeof id === 'string' && byId.has(id))
    return {
      profiles,
      workspaceId: workspace.id,
      activeProfileId: active ?? profiles[0]?.id ?? null,
      pinnedProfileId: pinned && byId.has(pinned) ? pinned : null,
      keyringStatus,
      ...(repo.warning ? { warning: repo.warning } : {})
    }
  }

  private async selectId(edit: ProfileEdit, workspace: Workspace, id: string, pin?: boolean) {
    if (workspace.id && pin !== undefined) {
      const pins = await this.pins()
      if (pin) await edit.write(this.orca.storage, PINS, { ...pins, [workspace.id]: id })
      else {
        delete pins[workspace.id]
        await edit.write(this.orca.storage, PINS, pins)
      }
    }
    await edit.write(this.orca.storage, ACTIVE, id)
    this.selection = { workspaceId: workspace.id, id }
  }
  select(workspace: Workspace, id: string, pin?: boolean) {
    return this.serialize(async (edit) => {
      if (!(await this.merged(workspace)).byId.has(id))
        throw new Error('This profile no longer exists.')
      await this.selectId(edit, workspace, id, pin)
      return { activeProfileId: id }
    })
  }
  /** Supplied references may be shared. New secrets belong to the edited profile. */
  private async cleanSecret(
    edit: ProfileEdit,
    old: ConnectionProfile | undefined,
    remaining: ConnectionProfile[]
  ) {
    if (old?.auth.source !== 'orca-secret') return
    const key = keyFor(old)
    if (
      key !== 'profile:' + old.id + ':token' ||
      remaining.some((p) => p.auth.source === 'orca-secret' && keyFor(p) === key)
    )
      return
    await edit.write(this.orca.secrets, key, null)
  }
  save(
    workspace: Workspace,
    input: ConnectionProfile,
    pin?: boolean,
    secretValue?: string,
    duplicateFrom?: string
  ) {
    return this.serialize(async (edit) => {
      const profile = parseProfile(input, true)
      const { users, records, repo, byId } = await this.merged(workspace)
      const previous = users.find((p) => p.id === profile.id)
      const inherited = byId.get(duplicateFrom || profile.id)
      if (
        profile.local?.command?.trim() &&
        profile.local.command.trim() === inherited?.local?.command?.trim() &&
        (inherited.origin === 'repo' || inherited.repositoryCommand)
      )
        profile.repositoryCommand = true
      if (duplicateFrom) {
        const original = byId.get(duplicateFrom)
        if (!original || byId.has(profile.id))
          throw new Error('Choose a new name for this profile copy.')
        if (
          profile.auth.source === 'orca-secret' &&
          original.auth.source === 'orca-secret' &&
          !secretValue
        ) {
          // Duplicate the encrypted credential inside the worker. Both profiles can then rotate
          // and delete their own keys independently. A missing credential stays unconfigured.
          try {
            secretValue = (await this.orca.secrets.get(keyFor(original))) ?? undefined
          } catch {
            throw new Error('Unlock the OS keyring before copying this credential.')
          }
          profile.auth = { ...profile.auth, secretKey: 'profile:' + profile.id + ':token' }
        }
      }
      if (secretValue) {
        if (profile.auth.mode === 'none' || profile.auth.source !== 'orca-secret')
          throw new Error('Choose encrypted secret storage for this credential.')
        if ((await this.keyring()) !== 'os-keyring')
          throw new Error(
            'The OS keyring is unavailable. Unlock it or choose an environment variable.'
          )
        profile.auth = { ...profile.auth, secretKey: 'profile:' + profile.id + ':token' }
        try {
          await edit.write(this.orca.secrets, keyFor(profile), secretValue)
        } catch {
          throw new Error('Saving the encrypted credential failed. Check keyring access and retry.')
        }
      }
      const updated = users.filter((p) => p.id !== profile.id).concat(profile)
      await edit.write(this.orca.storage, USERS, [
        ...records.filter((r) => !record(r) || r.id !== profile.id),
        profile
      ])
      await this.selectId(edit, workspace, profile.id, pin)
      await this.cleanSecret(edit, previous, [...updated, ...repo.profiles])
      return { profile: { ...profile, origin: 'user' as const } }
    })
  }
  delete(workspace: Workspace, id: string) {
    return this.serialize(async (edit) => {
      const { users, records, repo } = await this.merged(workspace)
      const old = users.find((p) => p.id === id)
      if (!old) throw new Error('Edit .orca-dagster.json to remove a repository profile.')
      const updated = users.filter((p) => p.id !== id)
      // If removing an override, the repository profile (and its pins) becomes active again.
      if (!repo.profiles.some((p) => p.id === id)) {
        const pins = await this.pins()
        await edit.write(
          this.orca.storage,
          PINS,
          Object.fromEntries(Object.entries(pins).filter(([, value]) => value !== id))
        )
        if ((await this.orca.storage.get(ACTIVE)) === id)
          await edit.write(this.orca.storage, ACTIVE, null)
      }
      await this.cleanSecret(edit, old, [...updated, ...repo.profiles])
      await edit.write(
        this.orca.storage,
        USERS,
        records.filter((r) => !record(r) || r.id !== id)
      )
      if (this.selection?.id === id) this.selection = undefined
      return { ok: true as const }
    })
  }
}

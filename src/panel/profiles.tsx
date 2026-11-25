import {
  Badge,
  Button,
  Callout,
  Checkbox,
  Heading,
  Input,
  List,
  ListItem,
  Row,
  Select,
  Stack,
  Text,
  Textarea,
  atom,
  plugin,
  read,
  set,
  update
} from 'orca'
import type { AuthConfig, ConnectionProfile, ProfileKind } from '../types'
import { localPort } from '../local/port'
import * as s from './state'

const draft = atom<ConnectionProfile | null>(null)
const originalId = atom('')
const secret = atom(''),
  mapping = atom(''),
  port = atom(''),
  pinned = atom(false)
const result = atom<{ ok: boolean; text: string } | null>(null)
let revision = 0
export function edit(profile?: ConnectionProfile, kind: ProfileKind = 'remote-oss') {
  revision++
  set(originalId, '')
  set(s.editor, true)
  set(secret, '')
  set(result, null)
  set(
    draft,
    profile ?? {
      id: '',
      name: '',
      url: kind === 'local' ? 'http://localhost:3000' : '',
      kind,
      auth: { mode: 'none' }
    }
  )
  set(
    mapping,
    Object.entries(profile?.local?.pathPrefixMap ?? {})
      .map(([a, b]) => a + '=' + b)
      .join('\n')
  )
  set(port, String(profile ? localPort(profile) : 3000))
  set(pinned, !!profile && profile.id === read(s.profiles)?.pinnedProfileId)
}
export function ensureEditor() {
  if (!read(draft)) edit(s.active())
  else set(s.editor, true)
}
export function clearDraft() {
  revision++
  set(originalId, '')
  set(draft, null)
  set(secret, '')
  set(result, null)
}
function change(fields: Partial<ConnectionProfile>) {
  revision++
  set(result, null)
  update(draft, (p) => (p ? { ...p, ...fields } : null))
}
function profileId(name: string) {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^[^a-z0-9]+|-+$/g, '')
    .slice(0, 64)
}
function duplicate(p: ConnectionProfile) {
  const ids = new Set(read(s.profiles)?.profiles.map((item) => item.id))
  let name = 'Copy of ' + p.name
  for (let n = 2; ids.has(profileId(name)); n++) name = 'Copy ' + n + ' of ' + p.name
  // Preserve the reference only. The original keyring value never travels through the panel.
  const auth = { ...p.auth }
  if (auth.source === 'orca-secret') auth.secretKey ||= 'profile:' + p.id + ':token'
  edit({ ...p, id: '', name: name.slice(0, 80), auth })
  set(originalId, p.id)
}
function build(p: ConnectionProfile): ConnectionProfile {
  const name = p.name.trim(),
    url = p.url.trim(),
    id = p.id || profileId(name)
  if (!p.id && read(s.profiles)?.profiles.some((existing) => existing.id === id))
    throw Error(
      'A profile with this name already exists. Edit the saved profile or choose another name.'
    )
  if (!id || !name || !url) throw Error('Enter a profile name and URL.')
  const auth: AuthConfig = { mode: p.auth.mode }
  if (auth.mode !== 'none') {
    auth.source = p.auth.source ?? 'env-var'
    if (auth.source === 'env-var') auth.envVar = p.auth.envVar ?? ''
    else auth.secretKey = p.auth.secretKey || 'profile:' + id + ':token'
    if (auth.mode === 'basic') auth.username = p.auth.username ?? ''
    if (auth.mode === 'custom-header') auth.headerName = p.auth.headerName ?? ''
  }
  const profile: ConnectionProfile = { id, name, url, auth, kind: p.kind }
  if (p.sourceWorkspaceId) profile.sourceWorkspaceId = p.sourceWorkspaceId
  if (p.kind === 'local' && p.reloadLocationOnSave?.trim())
    profile.reloadLocationOnSave = p.reloadLocationOnSave.trim()
  const entries = read(mapping)
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const at = line.indexOf('=')
      if (at < 1 || !line.slice(at + 1).trim())
        throw Error('Enter each path mapping as remote=workspace.')
      return [line.slice(0, at).trim(), line.slice(at + 1).trim()]
    })
  if (p.kind === 'local') {
    const value = Number(read(port))
    if (!Number.isInteger(value) || value < 1 || value > 65535)
      throw Error('Enter a port between 1 and 65535.')
    profile.local = {
      ...p.local,
      port: value,
      pathPrefixMap: Object.fromEntries(entries)
    }
  } else if (entries.length) profile.local = { pathPrefixMap: Object.fromEntries(entries) }
  return profile
}
export function Profiles() {
  const list = read(s.profiles),
    p = read(draft)
  const disabled = read(s.busy) || read(s.switching),
    feedback = read(result)
  const authChange = (fields: Partial<AuthConfig>) => {
    set(secret, '')
    change({ auth: { ...p!.auth, ...fields } })
  }
  function submit(test: boolean) {
    if (!p) return
    const rev = revision,
      workspace = s.scope().workspaceId
    return s.action(async () => {
      const profile = build(p)
      const secretValue =
        profile.auth.source === 'orca-secret' ? read(secret) || undefined : undefined
      if (test) {
        const r = await plugin.request(
          'profiles.test',
          { profile, ...(secretValue ? { secretValue } : {}) },
          s.requestOptions()
        )
        if (rev === revision && workspace === s.scope().workspaceId)
          set(result, {
            ok: r.ok,
            text: r.ok
              ? 'Connected · Dagster ' + (r.version ?? 'version unknown')
              : s.clip(r.error || 'Connection failed', 400)
          })
      } else {
        await plugin.request(
          'profiles.save',
          {
            ...s.scope(),
            profile,
            ...(read(originalId) ? { duplicateFrom: read(originalId) } : {}),
            ...(secretValue ? { secretValue } : {}),
            ...(read(pinned) || read(s.profiles)?.pinnedProfileId === profile.id
              ? { pinToWorkspace: read(pinned) }
              : {})
          },
          s.requestOptions()
        )
        if (workspace !== s.scope().workspaceId) return
        if (rev === revision) {
          clearDraft()
          set(s.editor, false)
        }
        await s.reloadProfiles()
      }
    }, false)
  }
  return (
    <Stack>
      <Row justify="between">
        <Button
          icon="collapse"
          onPress={() => {
            clearDraft()
            set(s.editor, false)
          }}
        >
          Done
        </Button>
        <Heading level={3}>Connection Profiles</Heading>
        <Button icon="add" onPress={() => edit()}>
          New
        </Button>
      </Row>
      <List label="Saved profiles">
        {list?.profiles.slice(0, 50).map((item) => (
          <ListItem
            key={item.id}
            title={s.clip(item.name)}
            description={s.clip(item.url)}
            onPress={() => edit(item)}
          >
            <Badge>{item.origin ?? 'user'}</Badge>
          </ListItem>
        ))}
      </List>
      {p && (
        <Stack>
          <Input label="Name" value={p.name} maxLength={80} onChange={(name) => change({ name })} />
          <Select
            label="Kind"
            options={s.options(['local', 'remote-oss', 'dagster-plus'])}
            value={p.kind}
            onChange={(kind) => change({ kind: kind as ProfileKind })}
          />
          <Select
            label="Source worktree"
            value={p.sourceWorkspaceId || ''}
            options={[
              { value: '', label: 'Current workspace' },
              ...(list?.sourceWorkspaces ?? []).map((w) => ({ value: w.id, label: w.name }))
            ]}
            onChange={(sourceWorkspaceId) => change({ sourceWorkspaceId })}
          />
          {p.kind === 'local' && (
            <Input
              label="Reload on save · code location (optional)"
              value={p.reloadLocationOnSave || ''}
              onChange={(reloadLocationOnSave) => change({ reloadLocationOnSave })}
            />
          )}
          <Input label="URL" value={p.url} maxLength={1000} onChange={(url) => change({ url })} />
          <Select
            label="Auth mode"
            options={s.options(['none', 'bearer', 'basic', 'dagster-plus-token', 'custom-header'])}
            value={p.auth.mode}
            onChange={(mode) => authChange({ mode: mode as AuthConfig['mode'] })}
          />
          {p.auth.mode !== 'none' && (
            <Stack>
              {p.auth.mode === 'basic' && (
                <Input
                  label="Username"
                  value={p.auth.username ?? ''}
                  onChange={(username) => authChange({ username })}
                />
              )}
              {p.auth.mode === 'custom-header' && (
                <Input
                  label="Header name"
                  value={p.auth.headerName ?? ''}
                  onChange={(headerName) => authChange({ headerName })}
                />
              )}
              <Select
                label="Credential source"
                value={p.auth.source ?? 'env-var'}
                options={[
                  { value: 'env-var', label: 'Environment Variable' },
                  { value: 'orca-secret', label: 'Orca Encrypted Secret' }
                ]}
                onChange={(source) => authChange({ source: source as AuthConfig['source'] })}
              />
              {p.auth.source === 'orca-secret' ? (
                <Stack>
                  <Input
                    key={p.id || 'new'}
                    label="Secret"
                    type="password"
                    value={read(secret)}
                    maxLength={3000}
                    onChange={(v) => {
                      revision++
                      set(secret, v)
                    }}
                  />
                  {list?.keyringStatus === 'os-keyring' ? (
                    <Text muted>Keyring available · Save Profile stores the secret</Text>
                  ) : (
                    <Callout tone="warning" title="Keyring unavailable">
                      <Text>Choose Environment Variable to provide this credential.</Text>
                    </Callout>
                  )}
                </Stack>
              ) : (
                <Stack>
                  <Input
                    label="Environment variable"
                    value={p.auth.envVar ?? ''}
                    maxLength={120}
                    onChange={(envVar) => authChange({ envVar })}
                  />
                  {list?.credentialEnvPresent?.[p.id] !== undefined &&
                    list?.profiles.find((item) => item.id === p.id)?.auth.envVar ===
                      p.auth.envVar &&
                    (list.credentialEnvPresent[p.id] ? (
                      <Text muted>Available on the Orca host</Text>
                    ) : (
                      <Callout
                        tone="warning"
                        title={'Set ' + p.auth.envVar + ' in the Orca host environment'}
                      />
                    ))}
                </Stack>
              )}
            </Stack>
          )}
          {p.kind === 'local' && (
            <Stack>
              <Input
                label="Start command"
                value={p.local?.command ?? ''}
                placeholder="uv run dagster dev"
                maxLength={1000}
                onChange={(command) => change({ local: { ...p.local, command } })}
              />
              <Input
                label="Working directory"
                value={p.local?.cwd ?? ''}
                onChange={(cwd) => change({ local: { ...p.local, cwd } })}
              />
              <Input label="Port" value={read(port)} maxLength={5} onChange={(v) => set(port, v)} />
            </Stack>
          )}
          <Textarea
            label="Path prefix mappings"
            value={read(mapping)}
            maxLength={2000}
            onChange={(v) => set(mapping, v)}
          />
          <Checkbox
            label="Pin as default for this workspace"
            checked={read(pinned)}
            disabled={!list?.workspaceId}
            onChange={(v) => set(pinned, v)}
          />
          {feedback && <Callout tone={feedback.ok ? 'success' : 'danger'} title={feedback.text} />}
          <Row wrap>
            {p.id && (
              <Button icon="copy" disabled={disabled} onPress={() => duplicate(p)}>
                Duplicate
              </Button>
            )}
            <Button disabled={disabled} onPress={() => submit(true)}>
              Test Connection
            </Button>
            <Button
              disabled={
                disabled ||
                (p.auth.mode !== 'none' &&
                  p.auth.source === 'orca-secret' &&
                  list?.keyringStatus !== 'os-keyring')
              }
              onPress={() => submit(false)}
            >
              Save Profile
            </Button>
            {p.id && p.origin !== 'repo' && (
              <Button
                tone="danger"
                disabled={disabled}
                onPress={() =>
                  s.action(async () => {
                    const workspace = s.scope().workspaceId
                    await plugin.request(
                      'profiles.delete',
                      { ...s.scope(), profileId: p.id },
                      s.requestOptions()
                    )
                    if (workspace === s.scope().workspaceId) {
                      clearDraft()
                      await s.reloadProfiles()
                    }
                  }, false)
                }
              >
                Delete
              </Button>
            )}
          </Row>
        </Stack>
      )}
    </Stack>
  )
}

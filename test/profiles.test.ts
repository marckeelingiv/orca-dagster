import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { assert, mockHost, mountWorker, test, type MockHost } from 'orca/testing'
import type { OrcaWorkerApi } from 'orca/worker'
import { ProfileStore } from '../src/profiles/store'
import { keyFor } from '../src/profiles/credentials'
import type { ConnectionProfile } from '../src/types'

const repo: ConnectionProfile = {
  id: 'remote-dev-proxy',
  name: 'Repo default',
  kind: 'remote-oss',
  url: 'https://example.org',
  auth: { mode: 'none' }
}
const other = { ...repo, id: 'cloud_prod-2', name: 'Production' }
const workspace = { id: 'workspace:/home/developer/demo:main', path: null }
const keyring = { 'secrets.protection': { status: 'os-keyring' } }
const encrypted: ConnectionProfile = {
  ...other,
  auth: { mode: 'bearer', source: 'orca-secret' }
}
test('keyring profile copies retain independent credentials through rotation and deletion', async () => {
  const { store, worker, host } = await fixture()
  try {
    const original = (await store.save(workspace, encrypted, undefined, 'original-value')).profile
    const copy = (
      await store.save(
        workspace,
        { ...original, id: 'copy', name: 'Copy' },
        undefined,
        undefined,
        original.id
      )
    ).profile
    assert.equal(copy.auth.secretKey, 'profile:copy:token')
    assert.equal(host.data().secrets['profile:copy:token'], 'original-value')
    await store.save(workspace, copy, undefined, 'rotated-value')
    await store.delete(workspace, original.id)
    assert.equal(host.data().secrets['profile:copy:token'], 'rotated-value')
    await store.delete(workspace, copy.id)
    assert.deepEqual(host.data().secrets, {})
  } finally {
    await worker.deactivate()
  }
})
async function fixture(
  host: MockHost = mockHost({ results: keyring }),
  decorate?: (orca: OrcaWorkerApi) => ConstructorParameters<typeof ProfileStore>[0]
) {
  let store!: ProfileStore
  const worker = await mountWorker(
    {
      default(orca) {
        store = new ProfileStore(decorate ? decorate(orca) : orca)
      }
    },
    { host }
  )
  return { store, worker, host }
}
test('representative IDs, longest allowed IDs and explicit key references work with Orca host validation', async () => {
  const { store, worker, host } = await fixture()
  try {
    for (const id of [encrypted.id, 'remote-dev-proxy', 'a'.repeat(64)]) {
      const { profile } = await store.save(workspace, { ...encrypted, id }, true, 'first')
      const key = keyFor(profile)
      assert.equal(host.data().secrets[key], 'first')
      await store.save(workspace, profile, true, 'second')
      assert.equal(host.data().secrets[key], 'second')
      await store.delete(workspace, id)
      assert.equal(host.data().secrets[key], undefined)
    }
    assert.deepEqual(host.refusals, [])
  } finally {
    await worker.deactivate()
  }
})
test('repo defaults survive user overrides and deletion, pins are workspace-specific, selection and unpin are immediate', async () => {
  const { store, worker } = await fixture()
  const path = await mkdtemp(join(tmpdir(), 'dagster-profile-'))
  const first = { ...workspace, path },
    second = { id: 'other-folder', path }
  try {
    await writeFile(
      join(path, '.orca-dagster.json'),
      JSON.stringify({ defaultProfile: repo.id, profiles: [repo, other] })
    )
    assert.equal((await store.list(first)).activeProfileId, repo.id)
    await store.save(first, { ...repo, name: 'My override' })
    assert.equal((await store.list(first)).profiles[0].origin, 'user')
    await store.select(first, other.id, true)
    store.workspaceChanged()
    assert.equal((await store.list(second)).activeProfileId, repo.id)
    assert.equal((await store.list(first)).activeProfileId, other.id)
    await store.select(first, repo.id, false)
    assert.equal((await store.list(first)).pinnedProfileId, null)
    await store.delete(first, repo.id)
    assert.deepEqual((await store.list(first)).profiles[0], {
      ...repo,
      origin: 'repo'
    })
    await assert.rejects(store.delete(first, repo.id), /repository profile/)
  } finally {
    await worker.deactivate()
    await rm(path, { recursive: true, force: true })
  }
})
test('malformed and secret-bearing repo files are rejected without credential disclosure', async () => {
  const { store, worker, host } = await fixture()
  const path = await mkdtemp(join(tmpdir(), 'dagster-invalid-profile-'))
  const token = 'must-stay-private'
  try {
    await store.save(workspace, other)
    for (const content of [
      '{' + token,
      JSON.stringify({
        profiles: [{ ...repo, auth: { mode: 'bearer', token } }]
      }),
      JSON.stringify({
        profiles: [{ ...repo, url: 'https://user:' + token + '@example.org' }]
      }),
      JSON.stringify({ profiles: [repo, repo] })
    ]) {
      await writeFile(join(path, '.orca-dagster.json'), content)
      const result = await store.list({ ...workspace, path })
      assert.ok(result.warning)
      assert.deepEqual(
        result.profiles.map((p) => p.id),
        [other.id]
      )
      assert.ok(!JSON.stringify([result, worker.logs, host.data()]).includes(token))
    }
  } finally {
    await worker.deactivate()
    await rm(path, { recursive: true, force: true })
  }
})
test('rejects secret input for unsupported modes and unavailable keyring before any persistence', async () => {
  const { store, worker, host } = await fixture(
    mockHost({ results: { 'secrets.protection': { status: 'unavailable' } } })
  )
  const token = 'private-fixture-token'
  try {
    for (const profile of [
      repo,
      {
        ...other,
        auth: {
          mode: 'bearer' as const,
          source: 'env-var' as const,
          envVar: 'TOKEN'
        }
      },
      encrypted
    ]) {
      await assert.rejects(
        store.save(workspace, profile, true, token),
        (e) => e instanceof Error && !e.message.includes(token)
      )
    }
    assert.deepEqual(host.data().storage, {})
    assert.deepEqual(host.data().secrets, {})
    assert.ok(!JSON.stringify([worker.logs, host.callsOf('storage.set')]).includes(token))
  } finally {
    await worker.deactivate()
  }
})
test('concurrent edits preserve independent profiles and serialize selection and secret cleanup', async () => {
  const { store, worker, host } = await fixture()
  try {
    await Promise.all([
      store.save(workspace, encrypted, false, 'old'),
      store.save(workspace, repo),
      store.save(workspace, encrypted, true, 'rotated')
    ])
    const result = await store.list(workspace)
    assert.deepEqual(result.profiles.map((p) => p.id).sort(), [encrypted.id, repo.id].sort())
    assert.equal(result.pinnedProfileId, encrypted.id)
    await store.save(workspace, {
      ...encrypted,
      auth: { mode: 'bearer', source: 'env-var', envVar: 'TOKEN' }
    })
    assert.deepEqual(host.data().secrets, {})
  } finally {
    await worker.deactivate()
  }
})

test('failed save, rotation and removal preserve both stores and the active selection, then retry cleanly', async () => {
  const host = mockHost({ results: keyring })
  let fault = ''
  const { worker, store } = await fixture(host, (orca) => {
    const interrupt = (point: string) => {
      if (fault === point) {
        fault = ''
        throw Error('host write failed')
      }
    }
    return {
      host: orca.host,
      storage: {
        ...orca.storage,
        set: async (key, value) => {
          await orca.storage.set(key, value)
          interrupt(key)
        }
      },
      secrets: {
        ...orca.secrets,
        delete: async (key) => {
          await orca.secrets.delete(key)
          interrupt('keyring-delete')
        }
      }
    }
  })
  try {
    await store.save(workspace, repo, true)
    const before = host.data()
    fault = 'profiles.active'
    await assert.rejects(store.save(workspace, encrypted, true, 'original'), /failed/)
    assert.deepEqual(host.data(), before)
    assert.equal((await store.list(workspace)).activeProfileId, repo.id)
    await store.save(workspace, encrypted, true, 'original')
    const saved = host.data()
    fault = 'profiles.user'
    await assert.rejects(
      store.save(workspace, { ...encrypted, name: 'Renamed' }, true, 'replacement'),
      /failed/
    )
    assert.deepEqual(host.data(), saved)
    fault = 'keyring-delete'
    await assert.rejects(store.delete(workspace, encrypted.id), /failed/)
    assert.deepEqual(host.data(), saved)
    await store.delete(workspace, encrypted.id)
    assert.deepEqual(host.data().secrets, {})
    assert.equal((await store.list(workspace)).activeProfileId, repo.id)
  } finally {
    await worker.deactivate()
  }
})

test('explicit key references respect host limits and remain available when a user profile is removed', async () => {
  const reference = 'services/dagster:team.release-2'
  const host = mockHost({
    results: keyring,
    secrets: { [reference]: 'shared-value' }
  })
  const { store, worker } = await fixture(host)
  try {
    const profile = {
      ...encrypted,
      auth: { ...encrypted.auth, secretKey: reference }
    }
    await store.save(workspace, profile)
    assert.equal(keyFor((await store.list(workspace)).profiles[0]), reference)
    for (const secretKey of ['constructor', '__proto__', 'prototype', 'x'.repeat(257)]) {
      await assert.rejects(
        store.save(workspace, {
          ...profile,
          auth: { ...profile.auth, secretKey }
        }),
        /secret key/i
      )
    }
    await store.delete(workspace, profile.id)
    assert.equal(host.data().secrets[reference], 'shared-value')
    assert.deepEqual(host.refusals, [])
  } finally {
    await worker.deactivate()
  }
})

test('keyring failures redact the host error and an interrupted rollback resumes before retry', async () => {
  const host = mockHost({ results: keyring })
  let failWrite = true,
    failCleanup = true
  const token = 'never-include-in-errors'
  const { store, worker } = await fixture(host, (orca) => ({
    host: orca.host,
    storage: orca.storage,
    secrets: {
      ...orca.secrets,
      set: async (key, value) => {
        await orca.secrets.set(key, value)
        if (failWrite) {
          failWrite = false
          throw Error('host rejected ' + value)
        }
      },
      delete: async (key) => {
        if (failCleanup) {
          failCleanup = false
          throw Error('temporary lock')
        }
        await orca.secrets.delete(key)
      }
    }
  }))
  try {
    await assert.rejects(
      store.save(workspace, encrypted, true, token),
      (e) => e instanceof Error && /retry/i.test(e.message) && !e.message.includes(token)
    )
    assert.deepEqual((await store.list(workspace)).profiles, [])
    assert.deepEqual(host.data().secrets, {})
    assert.ok(
      !JSON.stringify([host.data().storage, worker.logs, host.callsOf('storage.set')]).includes(
        token
      )
    )
    await store.save(workspace, encrypted, true, token)
    assert.equal(host.data().secrets[keyFor(encrypted)], token)
    assert.ok(!JSON.stringify(host.data().storage).includes(token))
  } finally {
    await worker.deactivate()
  }
})

test('Orca mock host permits punctuation and rejects oversized and reserved explicit keyring keys', async () => {
  const host = mockHost({ results: keyring })
  let secrets!: OrcaWorkerApi['secrets']
  const worker = await mountWorker(
    {
      default(orca) {
        secrets = orca.secrets
      }
    },
    { host }
  )
  try {
    for (const key of ['profile:remote-dev-proxy:token', 'x'.repeat(256), 'org/team:production']) {
      await secrets.set(key, 'fixture')
      assert.equal(await secrets.get(key), 'fixture')
      await secrets.delete(key)
    }
    for (const key of ['', 'constructor', '__proto__', 'prototype', 'x'.repeat(257)]) {
      await assert.rejects(secrets.get(key))
    }
    assert.equal(host.refusals.filter((r) => r.code === 'invalid_params').length, 5)
  } finally {
    await worker.deactivate()
  }
})

test('one malformed stored profile does not hide valid user profiles or their pin', async () => {
  const { store, worker } = await fixture(
    mockHost({
      results: keyring,
      storage: {
        'profiles.user': [repo, { id: 'bad', name: 'bad', auth: null }, other],
        'profiles.active': other.id
      }
    })
  )
  try {
    const list = await store.list(workspace)
    assert.deepEqual(
      list.profiles.map((p) => p.id),
      [repo.id, other.id]
    )
    assert.equal(list.activeProfileId, other.id)
  } finally {
    await worker.deactivate()
  }
})

test('saving and copying a repository profile retain launch confirmation for its inherited command', async () => {
  const path = await mkdtemp(join(tmpdir(), 'repo-command-'))
  const scoped = { ...workspace, path }
  const p = { ...repo, kind: 'local' as const, local: { command: 'uv run dagster dev' } }
  const { store, worker } = await fixture()
  try {
    await writeFile(join(path, '.orca-dagster.json'), JSON.stringify({ profiles: [p] }))
    const copy = (await store.save(scoped, { ...p, id: 'copy' }, undefined, undefined, p.id))
      .profile
    assert.equal(copy.repositoryCommand, true)
    const override = (await store.save(scoped, { ...p, name: 'Label edit' })).profile
    assert.equal(override.repositoryCommand, true)
    const elsewhere = await store.list({ id: 'different', path: null })
    assert.ok(elsewhere.profiles.every((p) => p.repositoryCommand))
    const edited = (await store.save(scoped, { ...override, local: { command: 'my-own-start' } }))
      .profile
    assert.equal(edited.repositoryCommand, undefined)
  } finally {
    await worker.deactivate()
    await rm(path, { recursive: true, force: true })
  }
})

import { assert, test } from 'orca/testing'
import {
  graphqlEndpoint,
  interpolateEnv,
  MissingEnvVarError,
  normalizeGraphqlUrl,
  resolveAuthHeaders
} from '../src/profiles/credentials'
import type { AuthConfig, ConnectionProfile } from '../src/types'

const base: ConnectionProfile = {
  id: 'remote-dev-proxy',
  name: 'Remote',
  kind: 'remote-oss',
  url: 'https://example.org',
  auth: { mode: 'none' }
}
const env = { TOKEN: 'sample-token', USER: 'dév', HEADER: 'X-Dagster-Auth' }
test('resolves all five auth modes from references', async () => {
  const headers = (auth: AuthConfig) =>
    resolveAuthHeaders(
      { ...base, auth },
      {
        env,
        secrets: {
          get: async () => {
            throw Error('unexpected secret lookup')
          }
        }
      }
    )
  const source = { source: 'env-var' as const, envVar: 'TOKEN' }
  assert.deepEqual(await headers({ mode: 'none' }), {})
  assert.deepEqual(await headers({ mode: 'basic', ...source, username: '${USER}' }), {
    Authorization: 'Basic ' + Buffer.from('dév:sample-token').toString('base64')
  })
  assert.deepEqual(await headers({ mode: 'bearer', ...source }), {
    Authorization: 'Bearer sample-token'
  })
  assert.deepEqual(await headers({ mode: 'dagster-plus-token', ...source }), {
    'Dagster-Cloud-Api-Token': 'sample-token'
  })
  assert.deepEqual(
    await headers({
      mode: 'custom-header',
      ...source,
      headerName: '${HEADER}'
    }),
    { 'X-Dagster-Auth': 'sample-token' }
  )
})
test('keyring preview overrides the stored secret only in keyring mode', async () => {
  const calls: string[] = []
  const secrets = {
    get: async (key: string) => {
      calls.push(key)
      return 'saved'
    }
  }
  const profile: ConnectionProfile = {
    ...base,
    auth: { mode: 'bearer', source: 'orca-secret', secretKey: 'team.shared' }
  }
  assert.deepEqual(
    await resolveAuthHeaders(profile, {
      env,
      secrets,
      secretOverride: 'preview'
    }),
    { Authorization: 'Bearer preview' }
  )
  assert.deepEqual(await resolveAuthHeaders(profile, { env, secrets }), {
    Authorization: 'Bearer saved'
  })
  assert.deepEqual(
    await resolveAuthHeaders(
      { ...base, auth: { mode: 'bearer', source: 'env-var', envVar: 'TOKEN' } },
      { env, secrets, secretOverride: 'preview' }
    ),
    { Authorization: 'Bearer sample-token' }
  )
  assert.deepEqual(calls, ['team.shared'])
})
test('expands URL templates, normalizes GraphQL paths, and redacts invalid input', async () => {
  assert.equal(
    graphqlEndpoint(
      { ...base, url: '${ORIGIN}/${DEPLOY}/graphql/' },
      { ORIGIN: 'https://example.org', DEPLOY: 'prod' }
    ),
    'https://example.org/prod/graphql'
  )
  assert.equal(
    graphqlEndpoint(
      {
        ...base,
        kind: 'local',
        url: 'http://localhost:3000',
        local: { port: 4321 }
      },
      {}
    ),
    'http://localhost:4321/graphql'
  )
  assert.throws(
    () => interpolateEnv('${MISSING}', {}),
    (e) => e instanceof MissingEnvVarError && e.varName === 'MISSING'
  )
  const token = 'do-not-echo'
  const redacted = (e: unknown) => e instanceof Error && !e.message.includes(token)
  for (const url of [
    'https://user:' + token + '@example.org',
    'https://example.org?token=' + token,
    'bad ' + token
  ])
    assert.throws(() => normalizeGraphqlUrl(url), redacted)
  for (const auth of [
    { mode: 'bearer', source: 'env-var', envVar: 'TOKEN' },
    {
      mode: 'custom-header',
      source: 'env-var',
      envVar: 'TOKEN',
      headerName: 'bad header ' + token
    },
    {
      mode: 'basic',
      source: 'env-var',
      envVar: 'TOKEN',
      username: 'bad:' + token
    }
  ] as AuthConfig[]) {
    await assert.rejects(
      resolveAuthHeaders(
        { ...base, auth },
        {
          env: { TOKEN: auth.mode === 'bearer' ? token + '\n' : token },
          secrets: { get: async () => null }
        }
      ),
      redacted
    )
  }
})

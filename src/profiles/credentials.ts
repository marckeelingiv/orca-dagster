import { Buffer } from 'node:buffer'
import { localPort } from '../local/port'
import type { ConnectionProfile } from '../types'

type Environment = Record<string, string | undefined>
export class CredentialError extends Error {}

export class MissingEnvVarError extends CredentialError {
  constructor(readonly varName: string) {
    super('Set the ' + varName + ' environment variable in the Orca host environment.')
    this.name = 'MissingEnvVarError'
  }
}

export function interpolateEnv(input: string, env: Environment): string {
  return input.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g, (_, name: string) => {
    if (!Object.hasOwn(env, name) || !env[name]) throw new MissingEnvVarError(name)
    return env[name]!
  })
}

function parseUrl(value: string): URL {
  try {
    const url = new URL(value)
    if (
      ['http:', 'https:'].includes(url.protocol) &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash
    )
      return url
  } catch {
    /* Do not echo the input, which may contain embedded credentials. */
  }
  throw new Error('Enter an HTTP or HTTPS Dagster URL without credentials, a query, or a fragment.')
}

export function normalizeGraphqlUrl(rawUrl: string): string {
  const url = parseUrl(rawUrl)
  const path = url.pathname.replace(/\/+$/, '')
  url.pathname = path.endsWith('/graphql') ? path : path + '/graphql'
  return url.href
}

export function graphqlEndpoint(profile: ConnectionProfile, env: Environment): string {
  const url = parseUrl(interpolateEnv(profile.url, env))
  if (profile.kind === 'local') url.port = String(localPort(profile, url.href))
  return normalizeGraphqlUrl(url.href)
}

export function keyFor(profile: ConnectionProfile): string {
  const key = profile.auth.secretKey ?? 'profile:' + profile.id + ':token'
  if (!key.length || key.length > 256 || ['constructor', 'prototype', '__proto__'].includes(key)) {
    throw new CredentialError(
      'Enter a valid Orca secret key (1–256 characters, excluding reserved names).'
    )
  }
  return key
}

export async function resolveAuthHeaders(
  profile: ConnectionProfile,
  options: {
    secrets: { get(key: string): Promise<string | null> }
    env: Environment
    secretOverride?: string
  }
): Promise<Record<string, string>> {
  const { auth } = profile
  if (auth.mode === 'none') return {}
  let secret: string | null | undefined
  if (auth.source === 'env-var') {
    if (!auth.envVar || !/^[A-Z_][A-Z_0-9]*$/.test(auth.envVar)) {
      throw new CredentialError('Enter a credential environment variable name in the profile.')
    }
    secret = Object.hasOwn(options.env, auth.envVar) ? options.env[auth.envVar] : undefined
    if (!secret) throw new MissingEnvVarError(auth.envVar)
  } else if (auth.source === 'orca-secret') {
    try {
      secret = options.secretOverride || (await options.secrets.get(keyFor(profile)))
    } catch {
      throw new CredentialError(
        'The Orca keyring is unavailable. Unlock it or select an environment variable.'
      )
    }
    if (!secret) throw new CredentialError('Set the encrypted secret in the profile editor.')
  } else {
    throw new CredentialError('Choose a credential source in the profile editor.')
  }
  if (auth.mode === 'basic') {
    const username = interpolateEnv(auth.username ?? '', options.env)
    if (!username || username.includes(':'))
      throw new CredentialError('Enter a Basic auth username without a colon.')
    return {
      Authorization: 'Basic ' + Buffer.from(username + ':' + secret, 'utf8').toString('base64')
    }
  }
  // Reject invalid header values before fetch can include them in an exception.
  if (/[^\x20-\x7e\x80-\xff]/.test(secret))
    throw new CredentialError('The credential contains invalid header characters.')
  switch (auth.mode) {
    case 'bearer':
      return { Authorization: 'Bearer ' + secret }
    case 'dagster-plus-token':
      return { 'Dagster-Cloud-Api-Token': secret }
    case 'custom-header': {
      const name = interpolateEnv(auth.headerName ?? '', options.env)
      if (!/^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/.test(name))
        throw new CredentialError('Enter a valid custom HTTP header name.')
      return { [name]: secret }
    }
    default:
      throw new CredentialError('Choose a supported auth mode.')
  }
}

import type { ConnectionProfile } from '../types'

/** A local port setting overrides an explicit URL port. An unqualified local URL uses Dagster's default. */
export function localPort(profile: ConnectionProfile, resolvedUrl = profile.url): number {
  if (profile.local?.port !== undefined) return profile.local.port
  try {
    return Number(new URL(resolvedUrl).port) || 3000
  } catch {
    return 3000
  } // A draft or unexpanded URL in the editor.
}

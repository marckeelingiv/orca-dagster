import { defineCheck, type CheckScenario } from 'orca/testing'
import { repo, requests, list, data, run, profile } from './panel/fixtures'
function scenario(tab: string, extra: Record<string, string | boolean> = {}): CheckScenario {
  return {
    requests,
    persisted: {
      workspace: {
        'dagster.scope': JSON.stringify(['sample', 'local']),
        'dagster.tab': tab,
        ...extra
      }
    }
  }
}
export default defineCheck({
  empty: {
    requests: {
      'profiles.list': { ...list, profiles: [], activeProfileId: null }
    }
  },
  'local-stopped': {
    requests: {
      ...requests,
      'dagster.snapshot': {
        ...data,
        status: 'stopped',
        localState: { status: 'stopped' }
      }
    }
  },
  dag: scenario('dag'),
  'connected-assets': scenario('assets', {
    'dagster.asset': JSON.stringify([repo.locationName, repo.repositoryName, ['daily_revenue']])
  }),
  automation: scenario('automation'),
  'runs-list': scenario('runs'),
  'run-detail-failed': scenario('runs', { 'dagster.run': run.runId }),
  'profile-editor': scenario('assets', { 'dagster.editor': true }),
  'auth-error': {
    requests: {
      ...requests,
      'profiles.list': {
        ...list,
        profiles: [
          {
            ...profile,
            auth: { mode: 'bearer', source: 'env-var', envVar: 'DAGSTER_TOKEN' }
          }
        ]
      },
      'dagster.snapshot': { ...data, status: 'auth-error', error: 'HTTP 401' }
    }
  }
})

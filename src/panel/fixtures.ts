import { type CheckScenario } from 'orca/testing'
import type { ConnectionProfile, ProfileList, RunDetail, Snapshot } from '../types'

export const profile: ConnectionProfile = {
  id: 'local',
  name: 'Local (Docker)',
  kind: 'local',
  url: 'http://localhost:3000',
  auth: { mode: 'none' },
  origin: 'repo'
}
export const list: ProfileList = {
  profiles: [profile],
  activeProfileId: 'local',
  workspaceId: 'sample',
  keyringStatus: 'os-keyring'
}
export const repo = {
  locationName: 'sample_project',
  repositoryName: '__repository__'
}
export const run: RunDetail = {
  runId: 'a1b2c3d4',
  pipelineName: 'orders_pipeline_job',
  status: 'FAILURE',
  startTime: 1700000000000,
  endTime: 1700000001400,
  assetKeys: [['daily_revenue']],
  steps: [{ key: 'daily_revenue', status: 'FAILURE' }],
  logs: [
    {
      timestamp: 1700000001400,
      level: 'ERROR',
      message: 'RuntimeError: sample enrichment failed'
    }
  ]
}
export const data: Snapshot = {
  uiUrl: profile.url,
  status: 'connected',
  syncedAt: 1700000001500,
  localState: { status: 'attached' },
  locations: [{ name: repo.locationName, loadStatus: 'LOADED' }],
  assets: [
    {
      ...repo,
      assetKey: ['daily_revenue'],
      groupName: 'commerce',
      status: 'success',
      lastMaterializedAt: 1700000001500 - 120000,
      isPartitioned: true,
      codeReferences: [],
      jobNames: [run.pipelineName]
    }
  ],
  jobs: [{ ...repo, name: run.pipelineName }],
  schedules: [
    {
      ...repo,
      name: 'daily_orders_schedule',
      status: 'RUNNING',
      cronSchedule: '0 6 * * *',
      tick: { status: 'SUCCESS', timestamp: 1700000001500 - 240000 }
    }
  ],
  sensors: [
    {
      ...repo,
      name: 'orders_sensor',
      status: 'STOPPED',
      minIntervalSeconds: 30
    }
  ],
  runs: [run]
}
export const requests: CheckScenario['requests'] = {
  'profiles.list': list,
  'dagster.snapshot': data,
  'dagster.graph': {
    token: 'fixture',
    nodes: data.assets.map((a) => ({
      ...a,
      keyString: JSON.stringify([a.locationName, a.repositoryName, a.assetKey]),
      dependencyKeys: []
    }))
  },
  'source.index': { changed: [], files: [] },
  'dagster.health': [],
  'dagster.runDetail': { run },
  'dagster.runs': { runs: [run], hasMore: false },
  'agent.listTargets': { defaultAgent: 'claude', terminals: [] },
  'dagster.assetPartitions': {
    kind: 'single',
    keys: ['2026-11-24', '2026-11-23']
  }
}

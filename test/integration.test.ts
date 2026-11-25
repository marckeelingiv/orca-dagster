import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { mockHost, mountWorker } from 'orca/testing'
import type { TestWorker } from 'orca/testing'
import * as main from '../src/worker'
import { AuthError, DagsterClient } from '../src/dagster/client'
import {
  graphqlEndpoint,
  MissingEnvVarError,
  resolveAuthHeaders
} from '../src/profiles/credentials'
import type { ConnectionProfile, InstigationSummary } from '../src/types'

const live = { skip: process.env.DAGSTER_E2E !== '1' }
const root = resolve('.')
const localUrl = 'http://localhost:3000'
const remoteUrl = 'http://localhost:3001'
const workspaceId = '7270eaf1-6439-450b-be07-a1aac71785ce::' + root
const scope = { workspaceId, profileId: 'local-docker' }

test(
  'live proxy authenticates both header schemes and suppresses credentials in failures',
  live,
  async () => {
    const token = process.env.DAGSTER_DEV_REMOTE_TOKEN
    assert.ok(token, 'Set DAGSTER_DEV_REMOTE_TOKEN to the Compose fixture token.')
    const repoConfig = await readFile(resolve(root, '.orca-dagster.json'), 'utf8')
    const remote: ConnectionProfile = JSON.parse(repoConfig).profiles.find(
      (p: ConnectionProfile) => p.id === 'remote-dev-proxy'
    )
    assert.equal(remote.url, remoteUrl)
    assert.equal(repoConfig.includes(token), false)
    if (process.env.DAGSTER_E2E_REMOTE_URL) remote.url = process.env.DAGSTER_E2E_REMOTE_URL
    const endpoint = graphqlEndpoint(remote, process.env)
    const secrets = { get: async () => null }
    await assert.rejects(resolveAuthHeaders(remote, { env: {}, secrets }), MissingEnvVarError)
    const wrong = randomUUID()
    const bad = await resolveAuthHeaders(remote, {
      env: { DAGSTER_DEV_REMOTE_TOKEN: wrong },
      secrets
    })
    for (const headers of [{}, bad]) {
      await assert.rejects(
        new DagsterClient(endpoint, headers).ping(),
        (error: unknown) =>
          error instanceof AuthError &&
          error.status === 401 &&
          error.code === 'UNAUTHORIZED' &&
          !error.message.includes(wrong) &&
          !error.message.includes(token)
      )
    }
    for (const mode of ['bearer', 'dagster-plus-token'] as const) {
      const headers = await resolveAuthHeaders(
        { ...remote, auth: { ...remote.auth, mode } },
        { env: process.env, secrets }
      )
      const client = new DagsterClient(endpoint, headers)
      assert.ok(await client.ping())
      assert.ok(
        (await client.fetchSnapshot()).assets.some(
          (asset) => asset.assetKey.join('/') === 'raw_orders'
        )
      )
      assert.equal(client.redact('credential=' + token), 'credential=[redacted]')
    }
  }
)

async function waitForRun(
  worker: TestWorker,
  runId: string,
  expected: 'SUCCESS' | 'FAILURE' | 'CANCELED'
) {
  const deadline = Date.now() + 180_000
  let status = ''
  do {
    const { run } = await worker.request('dagster.runDetail', { ...scope, runId })
    status = run.status
    if (['SUCCESS', 'FAILURE', 'CANCELED'].includes(status)) {
      assert.equal(status, expected, 'Run ' + runId + ': ' + (run.error || status))
      return run
    }
    await delay(750)
  } while (Date.now() < deadline)
  throw new Error('Timed out waiting for run ' + runId + ' (' + status + ').')
}

test(
  'live worker navigates sample definitions, controls fixture runs, and relays the failing Python frame',
  live,
  async () => {
    const token = process.env.DAGSTER_DEV_REMOTE_TOKEN
    assert.ok(token, 'Set DAGSTER_DEV_REMOTE_TOKEN to the Compose fixture token.')
    const host = mockHost({
      settings: { defaultAgent: 'codex' },
      results: {
        'workspace.readContext': {
          workspaceId,
          branch: 'main',
          displayName: 'Dagster E2E',
          terminals: []
        },
        'workspace.list': {
          workspaces: [
            {
              id: workspaceId,
              kind: 'worktree',
              name: 'Dagster E2E',
              path: root,
              branch: 'main',
              hostId: 'local'
            }
          ],
          truncated: false
        },
        'terminal.list': { terminals: [], truncated: false },
        'editor.open': { opened: true },
        'secrets.protection': { status: 'os-keyring' },
        'agent.launch': { terminalId: 'fixture-agent' },
        'statusBar.update': { ok: true }
      }
    })
    const worker = await mountWorker(
      {
        ...main,
        default: (api) =>
          main.default(api, undefined, async () => {
            throw Error('Test host has no desktop CLI')
          })
      },
      { host }
    )
    try {
      const profiles = await worker.request('profiles.list')
      const local = profiles.profiles.find((p) => p.id === scope.profileId)
      const remote = profiles.profiles.find((p) => p.id === 'remote-dev-proxy')
      assert.equal(local?.url, localUrl)
      assert.equal(local.local?.port, 3000)
      assert.equal(remote?.url, remoteUrl)
      assert.equal(profiles.workspaceId, workspaceId)
      for (const [profile, url] of [
        [local, process.env.DAGSTER_E2E_LOCAL_URL],
        [remote, process.env.DAGSTER_E2E_REMOTE_URL]
      ] as const) {
        if (url) {
          profile.url = url
          if (profile.kind === 'local')
            profile.local = { ...profile.local, port: Number(new URL(url).port) }
          await worker.request('profiles.save', { workspaceId, profile })
        }
      }
      const tested = await worker.request('profiles.test', { profile: remote })
      assert.equal(tested.ok, true, tested.error)
      const remoteScope = { workspaceId, profileId: remote.id }
      await worker.request('profiles.select', remoteScope)
      const remoteData = await worker.request('dagster.snapshot', remoteScope)
      assert.equal(remoteData.status, 'connected', remoteData.error)
      assert.ok(remoteData.assets.some((a) => a.assetKey.join('/') === 'raw_orders'))
      await worker.request('profiles.select', scope)
      const invalid = await worker.request('profiles.test', {
        profile: { ...remote, auth: { ...remote.auth, envVar: 'DAGSTER_E2E_ABSENT_CREDENTIAL' } }
      })
      assert.equal(invalid.ok, false)
      assert.match(invalid.error || '', /DAGSTER_E2E_ABSENT_CREDENTIAL/)
      const wrong = randomUUID()
      const rejected = await worker.request('profiles.test', {
        profile: { ...remote, auth: { mode: 'bearer', source: 'orca-secret' } },
        secretValue: wrong
      })
      assert.equal(rejected.ok, false)
      assert.match(rejected.error || '', /401|unauthoriz/i)
      assert.equal(JSON.stringify(rejected).includes(wrong), false)
      const snapshot = await worker.request('dagster.snapshot', scope)
      assert.equal(snapshot.status, 'connected', snapshot.error)
      assert.equal(snapshot.localState?.status, 'attached')
      const assets = new Map(snapshot.assets.map((a) => [a.assetKey.join('/'), a]))
      for (const name of ['raw_orders', 'cleaned_orders', 'daily_revenue', 'flaky_enrichment']) {
        assert.equal(assets.get(name)?.locationName, 'sample_project')
      }
      assert.equal(assets.get('daily_revenue')?.isPartitioned, true)
      const raw = assets.get('raw_orders')!
      const schedule = snapshot.schedules.find(
        (s) => s.name === 'daily_orders_schedule' && s.locationName === 'sample_project'
      )
      const sensor = snapshot.sensors.find(
        (s) => s.name === 'new_orders_sensor' && s.locationName === 'sample_project'
      )
      const job = snapshot.jobs.find(
        (j) => j.name === 'orders_pipeline_job' && j.locationName === 'sample_project'
      )
      assert.ok(schedule && sensor && job)
      // A deliberately unmatchable symbol proves the Tier 1 result came from actual metadata.
      assert.ok(
        raw.codeReferences.some((ref) => 'filePath' in ref && ref.filePath.startsWith('/app/src/'))
      )
      const file = 'docker/sample_project/src/sample_project/assets.py'
      const direct = await worker.request('source.open', {
        ...scope,
        kind: 'asset',
        name: 'metadata_only',
        codeReferences: raw.codeReferences
      })
      assert.equal(direct.resolvedPath, file)
      assert.ok(direct.opened && direct.lineNumber)
      const scanned = await worker.request('source.open', {
        ...scope,
        kind: 'schedule',
        name: schedule.name
      })
      assert.equal(scanned.resolvedPath, 'docker/sample_project/src/sample_project/schedules.py')
      assert.equal(scanned.opened, true)
      assert.deepEqual(host.callsOf('editor.open'), [
        { workspaceId, path: file },
        { workspaceId, path: scanned.resolvedPath }
      ])

      const reloaded = await worker.request('dagster.reload', {
        ...scope,
        locationNames: ['sample_project']
      })
      assert.ok(
        reloaded.results.length === 1 && reloaded.results[0].ok,
        JSON.stringify(reloaded.results)
      )
      for (const automation of ['schedule', 'sensor'] as const) {
        const item: InstigationSummary = automation === 'schedule' ? schedule : sensor
        const initial: boolean = item.status === 'RUNNING'
        const toggle = (running: boolean): Promise<{ status: string }> =>
          automation === 'schedule'
            ? worker.request('dagster.toggleSchedule', {
                ...scope,
                locationName: item.locationName,
                repositoryName: item.repositoryName,
                scheduleName: item.name,
                running
              })
            : worker.request('dagster.toggleSensor', {
                ...scope,
                locationName: item.locationName,
                repositoryName: item.repositoryName,
                sensorName: item.name,
                running
              })
        try {
          assert.equal((await toggle(!initial)).status, initial ? 'STOPPED' : 'RUNNING')
        } finally {
          assert.equal((await toggle(initial)).status, item.status)
        }
      }
      assert.equal(
        (await worker.request('dagster.assetPartitions', { ...scope, assetKey: raw.assetKey }))
          .kind,
        'none'
      )
      const parts = await worker.request('dagster.assetPartitions', {
        ...scope,
        assetKey: ['daily_revenue'],
        limit: 2
      })
      assert.equal(parts.kind, 'single')
      assert.ok(parts.keys.length > 0 && parts.keys.length <= 2)
      const tags = { 'orca/e2e': randomUUID() }
      const materialize = (name: string) =>
        worker.request('dagster.materialize', { ...scope, assetKeys: [[name]], tags })
      const { runId: rawId } = await materialize('raw_orders')
      await waitForRun(worker, rawId, 'SUCCESS')
      let previousSuccess = ''
      for (const runConfigYaml of [
        '{"ops":{"flaky_enrichment":{"config":{"should_fail":false}}}}',
        'ops:\n  flaky_enrichment:\n    config:\n      should_fail: false'
      ]) {
        const launch = await worker.request('dagster.launchJob', {
          ...scope,
          locationName: job.locationName,
          repositoryName: job.repositoryName,
          jobName: 'flaky_debug_job',
          runConfigYaml,
          tags
        })
        await waitForRun(worker, launch.runId, 'SUCCESS')
        previousSuccess = launch.runId
      }
      const { runId } = await materialize('flaky_enrichment')
      const failed = await waitForRun(worker, runId, 'FAILURE')
      assert.ok(
        failed.steps.some((step) => step.key === 'flaky_enrichment' && step.status === 'FAILURE')
      )
      assert.match(failed.error || '', /Simulated enrichment failure/)
      assert.match(failed.error || '', /assets\.py/)
      const errors = await worker.request('dagster.runDetail', {
        ...scope,
        runId,
        errorsOnly: true
      })
      assert.ok(errors.run.logs.some((log) => log.stepKey === 'flaky_enrichment' && log.error))
      const { comparison } = await worker.request('dagster.compareFailure', {
        ...scope,
        runId,
        assetKey: ['flaky_enrichment']
      })
      assert.ok(comparison.includes(previousSuccess), comparison)
      assert.match(comparison, /Changed configuration keys: ops/)
      const sent = await worker.request('agent.sendRunError', { ...scope, runId, mode: 'launch' })
      assert.equal(sent.terminalId, 'fixture-agent')
      const [launch] = host.callsOf('agent.launch') as { prompt: string; workspaceId: string }[]
      assert.equal(launch.workspaceId, workspaceId)
      for (const context of [runId, 'flaky_enrichment', file, 'Simulated enrichment failure'])
        assert.ok(launch.prompt.includes(context), context)
      assert.match(launch.prompt, /File ".+\.py", line \d+/)
      assert.equal(launch.prompt.includes(token), false)
      const retry = await worker.request('dagster.reexecuteRun', {
        ...scope,
        runId,
        strategy: 'FROM_FAILURE'
      })
      assert.notEqual(retry.runId, runId)
      assert.equal((await waitForRun(worker, retry.runId, 'FAILURE')).parentRunId, runId)
      const orderJob = {
        ...scope,
        locationName: job.locationName,
        repositoryName: job.repositoryName,
        jobName: job.name,
        tags
      }
      const orders = await worker.request('dagster.launchJob', orderJob)
      await waitForRun(worker, orders.runId, 'SUCCESS')
      const partition = await worker.request('dagster.materialize', {
        ...scope,
        assetKeys: [['daily_revenue']],
        partitionKey: parts.keys[0],
        tags
      })
      await waitForRun(worker, partition.runId, 'SUCCESS')
      const replay = await worker.request('dagster.reexecuteRun', {
        ...scope,
        runId: orders.runId,
        strategy: 'ALL_STEPS'
      })
      await waitForRun(worker, replay.runId, 'SUCCESS')
      const cancel = await worker.request('dagster.launchJob', orderJob)
      const terminated = await worker.request('dagster.terminateRun', {
        ...scope,
        runId: cancel.runId
      })
      assert.ok(terminated.ok, terminated.message)
      await waitForRun(worker, cancel.runId, 'CANCELED')
      const first = await worker.request('dagster.runs', { ...scope, limit: 1, filter: 'Failure' })
      assert.equal(first.hasMore, true)
      const second = await worker.request('dagster.runs', {
        ...scope,
        limit: 1,
        cursor: first.runs[0].runId,
        filter: 'Failure'
      })
      assert.equal(second.runs[0].status, 'FAILURE')
      assert.notEqual(first.runs[0].runId, second.runs[0].runId)
      assert.equal(host.callsOf('terminal.create').length, 0)
      assert.deepEqual(worker.refusals, [])
      const surfaced = JSON.stringify([
        worker.logs,
        invalid,
        rejected,
        worker.posts,
        host.data().storage
      ])
      for (const credential of [token, wrong]) assert.equal(surfaced.includes(credential), false)
    } finally {
      await worker.deactivate()
    }
  }
)

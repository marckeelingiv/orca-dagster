import { resolveSource } from './source/resolver'
import type { AssetSummary, RunDetail, SourceInput } from './types'
import type { Connection } from './session'

/** For an implicit asset job, the failing step identifies the Python file more precisely than the job name. */
export async function resolveRunSource(
  connection: Connection,
  run: RunDetail,
  catalog: AssetSummary[],
  uiUrl: string
) {
  const assets = catalog.filter(
    (a) =>
      (!run.locationName || a.locationName === run.locationName) &&
      (!run.repositoryName || a.repositoryName === run.repositoryName)
  )
  const inputs: SourceInput[] = []
  const asAsset = (key: string[], a?: AssetSummary): SourceInput => ({
    kind: 'asset',
    name: key.at(-1) ?? '',
    assetKey: key,
    dbtUniqueId: a?.dbtUniqueId,
    opNames: a?.opNames,
    codeReferences: a?.codeReferences,
    locationName: a?.locationName ?? run.locationName,
    repositoryName: a?.repositoryName ?? run.repositoryName
  })
  const steps = run.steps
    .filter((s) => s.status === 'FAILURE' || s.status === 'FAILED')
    .map((s) => s.key)
  if (!steps.length)
    steps.push(...new Set(run.logs.filter((l) => l.error && l.stepKey).map((l) => l.stepKey!)))
  for (const step of steps) {
    const name = step.split('.').at(-1)!.split('[')[0]
    const match = assets.find((a) => a.assetKey.join('__') === name)
    inputs.push(asAsset(match?.assetKey ?? [name], match))
  }
  const job: SourceInput = {
    kind: 'job',
    name: run.pipelineName,
    locationName: run.locationName,
    repositoryName: run.repositoryName
  }
  if (!run.pipelineName.startsWith('__ASSET_JOB')) inputs.push(job)
  for (const key of run.assetKeys.slice(0, 10)) {
    const match = assets.find((a) => JSON.stringify(a.assetKey) === JSON.stringify(key))
    inputs.push(asAsset(key, match))
  }
  if (!inputs.length) inputs.push(job)
  let fallback
  const checked = new Set<string>()
  for (const input of inputs) {
    const key = JSON.stringify([input.kind, input.assetKey ?? input.name])
    if (checked.has(key)) continue
    checked.add(key)
    const found = await resolveSource(connection.workspace.path, connection.profile, input, uiUrl)
    if (found.resolvedPath) return found
    fallback ??= found
  }
  return fallback
}

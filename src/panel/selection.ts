import { atom, read, set } from 'orca'
import type { AssetSummary, Partitions } from '../types'
export const keyOf = (a: AssetSummary) =>
  JSON.stringify([a.locationName, a.repositoryName, a.assetKey])

type Entry = { id: string; parts: Partitions | null }
export const basket = atom<Entry[]>([])
export function toggleAsset(asset: AssetSummary, parts: Partitions | null) {
  const id = keyOf(asset),
    saved = read(basket)
  set(
    basket,
    saved.some((e) => e.id === id) ? saved.filter((e) => e.id !== id) : [...saved, { id, parts }]
  )
}
export function selectionFor(
  assets: AssetSummary[],
  focus: AssetSummary,
  parts: Partitions | null
) {
  const saved = read(basket)
  const members = saved.map((e) => assets.find((a) => keyOf(a) === e.id))
  const selected = members.filter((a): a is AssetSummary => !!a)
  const focused = keyOf(focus)
  if (!saved.some((e) => e.id === focused)) selected.push(focus)
  const definitions = selected.map((a) =>
    keyOf(a) === focused ? parts : saved.find((e) => e.id === keyOf(a))?.parts
  )
  let reason = ''
  if (members.some((a) => !a))
    reason = 'A selected asset is no longer available. Clear the selection.'
  else if (
    selected.some(
      (a) => a.locationName !== focus.locationName || a.repositoryName !== focus.repositoryName
    )
  )
    reason = 'Select assets from the same repository and code location.'
  else if (
    selected.length > 1 &&
    !focus.jobNames.some(
      (job) => job.startsWith('__ASSET_JOB') && selected.every((a) => a.jobNames.includes(job))
    )
  )
    reason = 'Select assets that share an implicit asset job.'
  else if (selected.some((a) => a.isPartitioned !== focus.isPartitioned))
    reason = 'Select assets with compatible partition definitions.'
  else if (focus.isPartitioned && definitions.some((d) => !d))
    reason = 'Waiting for partition definitions.'
  else if (focus.isPartitioned && definitions.some((d) => d?.kind === 'multi'))
    reason = 'Choose multidimensional partitions in Launchpad.'
  else if (
    focus.isPartitioned &&
    definitions.some((d) => d?.kind !== parts?.kind || d?.dimensionName !== parts?.dimensionName)
  )
    reason = 'Selected assets use different partition dimensions.'
  const keys = (parts?.keys ?? [])
    .filter((k) => k.length <= 180 && definitions.every((d) => d?.keys.includes(k)))
    .slice(0, 100)
  if (!reason && parts?.kind === 'single' && !keys.length)
    reason = 'Selected assets have no common partition in the loaded keys.'
  return { selected, keys, reason }
}

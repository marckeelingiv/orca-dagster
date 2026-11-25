import type { SourceInput } from './types'

type Target =
  | SourceInput
  | { kind: 'lineage'; expression: string }
  | { kind: 'partitions'; assetKey: string[] }
  | { kind: 'run'; runId: string }
  | {
      kind: 'launchpad'
      locationName: string
      repositoryName: string
      jobName: string
      assetKeys?: string[][]
    }

/** Keep deployment path prefixes (including Dagster+) when building UI routes. */
export function dagsterUrl(base: string, target: Target): string {
  const url = new URL(base)
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
    throw new Error('Enter an HTTP or HTTPS Dagster URL without credentials.')
  }
  url.search = ''
  url.hash = ''
  const path = url.pathname.replace(/\/+$/, '').replace(/\/graphql$/, '')
  let route: string
  if (target.kind === 'lineage') {
    const expression = target.expression.trim()
    route = '/asset-groups' + (expression ? '~' + encodeURIComponent(expression) : '') + '/'
  } else if (target.kind === 'partitions') {
    route = '/assets/' + target.assetKey.map(encodeURIComponent).join('/')
    url.searchParams.set('view', 'partitions')
  } else if (target.kind === 'asset') {
    route = '/assets/' + (target.assetKey ?? [target.name]).map(encodeURIComponent).join('/')
  } else if (target.kind === 'run') {
    route = '/runs/' + encodeURIComponent(target.runId)
  } else if (target.locationName) {
    const location = encodeURIComponent(target.locationName)
    const repo = target.repositoryName
    const address =
      !repo || repo === '__repository__' ? location : encodeURIComponent(repo) + '@' + location
    const root = '/locations/' + address
    if (target.kind === 'launchpad') {
      route = root + '/jobs/' + encodeURIComponent(target.jobName) + '/playground'
      if (target.assetKeys?.length) {
        route += '/setup'
        target.assetKeys.forEach((key, i) =>
          key.forEach((part, j) => {
            url.searchParams.append('assetSelection[' + i + '][path][' + j + ']', part)
          })
        )
      }
    } else {
      route = root + '/' + target.kind + 's/' + encodeURIComponent(target.name)
    }
  } else if (target.kind === 'job') {
    route = '/guess/' + encodeURIComponent(target.name)
  } else {
    route = '/locations'
  }
  url.pathname = path + route
  return url.href
}

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { runInNewContext } from 'node:vm'
import { setImmediate as tick } from 'node:timers/promises'
import { buildSync } from 'esbuild'
import { parseHTML } from 'linkedom'

// Exercise the shipped Preact adapters with the same JSX transform as the HTML build.
const bundle = buildSync({
  stdin: {
    contents: 'export * from "./src/frame/controls"; export {h,render} from "preact"',
    resolveDir: process.cwd()
  },
  bundle: true,
  write: false,
  format: 'iife',
  globalName: 'frame',
  jsx: 'automatic',
  jsxImportSource: 'preact'
}).outputFiles[0].text
function frame(orca = {}) {
  const { document, Event } = parseHTML('<html><body></body></html>')
  const window = Object.assign(new EventTarget(), { orca })
  const api = runInNewContext(bundle + '\nframe', {
    document,
    window,
    AbortController,
    queueMicrotask
  })
  return { api, document, window, Event }
}

test('workspace restores finish before persistence resumes, including rapid switches and missing keys', async () => {
  const stores = new Map([
    ['sidebar.a', { tab: 'runs', search: 'alpha' }],
    ['sidebar.b', { tab: 'dag' }]
  ])
  const listeners = new Set()
  let finishSlow
  const { api, window } = frame({
    plugin: {
      on: (_, fn) => {
        listeners.add(fn)
        return { dispose: () => listeners.delete(fn) }
      }
    },
    host: { call: async () => ({ workspaceId: 'a' }) },
    storage: {
      get: async (k) =>
        k === 'sidebar.slow'
          ? new Promise((resolve) => {
              finishSlow = resolve
            })
          : stores.get(k),
      set: async (k, v) => stores.set(k, JSON.parse(JSON.stringify(v)))
    }
  })
  const tab = api.atom('tab', 'assets'),
    search = api.atom('search', '')
  let ready
  await api.mountFrame(
    () => {},
    (_, hydration) => {
      ready = hydration
    }
  )
  assert.equal(api.read(tab), 'runs')
  function change(workspaceId) {
    for (const fn of listeners) fn({ workspaceId })
  }
  api.set(search, 'edited')
  change('slow')
  await tick()
  change('b')
  api.set(tab, 'late write while switching')
  await ready()
  assert.equal(api.read(tab), 'dag')
  assert.equal(api.read(search), '')
  assert.deepEqual(stores.get('sidebar.b'), { tab: 'dag' })
  finishSlow({ tab: 'stale' })
  await tick()
  assert.equal(api.read(tab), 'dag')
  api.set(search, 'beta')
  change('a')
  await ready()
  assert.equal(api.read(search), 'edited')
  assert.deepEqual(stores.get('sidebar.b'), { tab: 'dag', search: 'beta' })
  change('new')
  await ready()
  assert.equal(api.read(tab), 'assets')
  assert.equal(api.read(search), '')
  window.dispatchEvent(new Event('pagehide'))
  assert.equal(listeners.size, 0)
})

test('frame controls render labels, icons, button styles, current rows and compact times into the DOM', () => {
  const { api, document } = frame()
  const { h, render } = api
  render(
    h('div', {}, [
      h(api.Button, { icon: 'play', tone: 'accent', variant: 'solid' }, 'Materialize'),
      h(api.Input, {
        type: 'password',
        label: 'Credential',
        labelHidden: true,
        value: 'fixture',
        placeholder: 'Enter secret'
      }),
      h(api.Select, {
        label: 'Profile',
        labelHidden: true,
        placeholder: 'Choose profile',
        options: [{ value: 'local', label: 'Local' }]
      }),
      h(api.ListItem, { title: 'Job', current: true }),
      h(api.Tree, {
        label: 'Assets',
        maxHeight: 'lg',
        items: [{ key: 'item', label: '<asset>', icon: 'check' }]
      }),
      h(api.RelativeTime, { at: Date.now() - 120100 }),
      h(api.RelativeTime, { at: Date.now() }),
      h(api.RelativeTime, { at: NaN })
    ]),
    document.body
  )
  assert.equal(
    document.querySelector('button[data-tone="accent"][data-variant="solid"] [data-icon="play"]')
      ?.tagName,
    'SVG'
  )
  assert.equal(document.querySelector('input').value, 'fixture')
  assert.equal(document.querySelector('input').placeholder, 'Enter secret')
  assert.deepEqual(
    [...document.querySelectorAll('label .sr-only')].map((e) => e.textContent),
    ['Credential', 'Profile']
  )
  assert.equal(document.querySelector('select option[disabled]').textContent, 'Choose profile')
  assert.ok(document.querySelector('.item[aria-current="true"]'))
  assert.ok(document.querySelector('.tree[data-max-height="lg"] .tree-row [data-icon="check"]'))
  assert.equal(document.querySelector('asset'), null)
  assert.deepEqual(
    [...document.querySelectorAll('time')].map((t) => t.textContent),
    ['2m ago', 'just now']
  )
  assert.equal(document.body.lastElementChild.lastElementChild.textContent, '—')
})

test('frame table supports row selection, double-click and keyboard activation, and pagination', () => {
  const { api, document, Event } = frame()
  const calls = []
  const props = {
    label: 'Runs',
    columns: [{ key: 'id', label: 'Run' }],
    rows: [{ key: 'r1', cells: { id: 'first' } }],
    hasMore: true,
    onLoadMore: () => calls.push('more'),
    onRowAction: ({ key }) => calls.push('open ' + key),
    onSelectionChange: ({ keys }) => calls.push('select ' + keys[0])
  }
  api.render(api.h(api.Table, props), document.body)
  const row = document.querySelector('tbody tr')
  row.dispatchEvent(new Event('click'))
  row.dispatchEvent(new Event('dblclick'))
  row.dispatchEvent(Object.assign(new Event('keydown'), { key: 'Enter' }))
  document.querySelector('button').click()
  assert.deepEqual(calls, ['select r1', 'open r1', 'open r1', 'more'])
  api.render(api.h(api.Table, { ...props, hasMore: false }), document.body)
  assert.equal(document.querySelector('button'), null)
})

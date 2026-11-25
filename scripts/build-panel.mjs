import { build } from 'esbuild'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'

const result = await build({
  entryPoints: ['src/frame/main.tsx'],
  bundle: true,
  alias: { 'orca/jsx-runtime': 'preact/jsx-runtime' },
  write: false,
  format: 'iife',
  jsx: 'automatic',
  jsxImportSource: 'preact',
  target: 'es2022',
  plugins: [
    {
      name: 'frame-controls',
      setup(b) {
        b.onResolve({ filter: /^orca$/ }, () => ({ path: resolve('src/frame/controls.tsx') }))
        b.onResolve({ filter: /^\.\/graph-surface$/ }, () => ({
          path: resolve('src/frame/graph.tsx')
        }))
      }
    }
  ]
})
const css = await readFile('src/frame/frame.css', 'utf8')
await mkdir('dist', { recursive: true })
await writeFile(
  'dist/sidebar.html',
  '<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><style>' +
    css +
    '</style></head><body><script>' +
    result.outputFiles[0].text.replace(/<\/script/gi, '<\\/script') +
    '</script></body></html>'
)

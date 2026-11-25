/** @jsxImportSource preact */
import { h, render, type ComponentType } from 'preact'
import Panel, { setup } from '../panel'
import { mountFrame } from './bridge'
void mountFrame(() => render(h(Panel as ComponentType, {}), document.body), setup).catch((e) => {
  document.body.textContent = String(e)
})

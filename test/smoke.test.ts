import { assert, mount, mountWorker, test } from 'orca/testing'
import * as panel from '../src/panel'
import * as main from '../src/worker'

test('mounts the worker and the sidebar without a workspace', async () => {
  const worker = await mountWorker(main)
  const profiles = await worker.request('profiles.list')
  assert.equal(profiles.activeProfileId, null)
  const view = await mount(panel, { host: worker.host })
  assert.deepEqual(view.errors, [])
  await view.unmount()
  await worker.deactivate()
})

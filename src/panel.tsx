import { Graph } from './panel/graph'
import * as explorer from './panel/explorer-state'
import {
  Badge,
  Button,
  Callout,
  CodeView,
  Collapsible,
  EmptyState,
  IconButton,
  RelativeTime,
  Select,
  Skeleton,
  Stack,
  Tab,
  Tabs,
  Text,
  Toolbar,
  plugin,
  read,
  set,
  type PanelContext
} from 'orca'
import { localPort } from './local/port'
import { Assets, Automation, Runs } from './panel/views'
import { Profiles, clearDraft, edit, ensureEditor } from './panel/profiles'
import * as s from './panel/state'

export function setup(ctx: PanelContext, workspaceReady?: () => Promise<void>) {
  ctx.subscriptions.push(plugin.on('dagster.workspaceChanged', clearDraft))
  s.setup(ctx, ensureEditor, workspaceReady)
}
export default function Panel() {
  const list = read(s.profiles),
    data = read(s.snapshot),
    profile = s.active()
  const status = data?.status ?? (profile ? 'connecting' : 'unconfigured')
  const local = data?.localState,
    managed = local?.status === 'managed-running'
  const disabled = read(s.busy) || read(s.switching)
  const localAction = s.runLocal
  const confirmation = read(s.localConfirmation)
  const reload = () =>
    s.action(async () => {
      const r = await plugin.request('dagster.reload', s.scope(), s.requestOptions())
      const failed = r.results.find((l) => !l.ok)
      if (failed) throw Error(failed.error ?? 'Reload failed for ' + failed.locationName)
    })
  const label = {
    connected: 'Connected',
    connecting: 'Connecting',
    degraded: 'Degraded',
    stopped: 'Stopped',
    'auth-error': 'Auth failed',
    error: 'Error',
    unconfigured: 'No connection'
  }[status]
  const partitionsOnly = read(s.tab) === 'partitions'
  const selected = partitionsOnly ? 'assets' : read(s.tab)
  return (
    <Stack>
      <Toolbar label="Connection">
        <Select
          label="Connection profile"
          labelHidden
          value={profile?.id}
          placeholder="Connection profile"
          disabled={read(s.switching)}
          options={(list?.profiles ?? [])
            .slice(0, 100)
            .map((p) => ({ value: p.id, label: s.clip(p.name) }))}
          onChange={(v) => {
            clearDraft()
            void s.switchProfile(v)
          }}
        />
        <Badge
          tone={
            status === 'connected'
              ? 'success'
              : ['error', 'auth-error'].includes(status)
                ? 'danger'
                : ['connecting', 'degraded'].includes(status)
                  ? 'warning'
                  : 'neutral'
          }
        >
          {label}
        </Badge>
        <IconButton
          icon="refresh"
          label="Refresh now"
          disabled={disabled}
          onPress={() => s.reloadProfiles()}
        />
        <Button
          icon="settings"
          variant="ghost"
          tooltip="Manage profiles"
          onPress={() => {
            if (read(s.editor)) {
              clearDraft()
              set(s.editor, false)
            } else edit(profile)
          }}
        >
          Profiles
        </Button>
      </Toolbar>
      {profile && (
        <Toolbar label={profile.kind === 'local' ? 'Local controls' : 'Connection controls'}>
          {profile.kind === 'local' && (
            <>
              <Button
                icon="play"
                disabled={
                  disabled ||
                  managed ||
                  local?.status === 'attached' ||
                  local?.status === 'starting' ||
                  local?.status === 'crashed'
                }
                onPress={() => localAction('local.start')}
              >
                Start
              </Button>
              <Button
                icon="stop"
                disabled={disabled || !local?.terminalId}
                onPress={() => localAction('local.stop')}
              >
                Stop
              </Button>
              <Button
                icon="refresh"
                disabled={disabled || (!managed && local?.status !== 'crashed')}
                onPress={() => localAction('local.restart')}
              >
                Restart
              </Button>
            </>
          )}
          <Button disabled={disabled || status === 'stopped'} onPress={reload}>
            Reload
          </Button>
          {profile.kind === 'local' && (
            <Button
              icon="terminal"
              disabled={!local?.terminalId || disabled}
              onPress={() => localAction('local.focusLogs')}
            >
              Logs
            </Button>
          )}
          <Text muted size="xs">
            {profile.kind === 'local' && (
              <Text>{':' + localPort(profile, data?.uiUrl) + ' · '}</Text>
            )}
            {data?.syncedAt ? (
              <>
                Sync <RelativeTime at={data.syncedAt} />
              </>
            ) : (
              label
            )}
          </Text>
        </Toolbar>
      )}
      {confirmation && (
        <Callout title="Run this repository command?" tone="warning">
          <Text>Working directory</Text>
          <CodeView code={confirmation.plan.cwd} wrap />
          <Text>Command</Text>
          <CodeView code={confirmation.plan.command} wrap />
          <Toolbar label="Confirm repository command">
            <Button onPress={() => set(s.localConfirmation, null)}>Cancel</Button>
            <Button
              tone="accent"
              variant="solid"
              disabled={disabled}
              onPress={() => localAction(confirmation.action, confirmation.plan)}
            >
              Run command
            </Button>
          </Toolbar>
        </Callout>
      )}
      {list?.warning && (
        <Callout tone="warning">
          <Text>{s.clip(list.warning, 500)}</Text>
        </Callout>
      )}
      {read(s.launchedRun) && (
        <Callout tone="success" title="Run launched">
          <Button
            onPress={() => {
              set(s.editor, false)
              set(s.tab, 'runs')
              return s.showRun(read(s.launchedRun))
            }}
          >
            {'View run ' + s.clip(read(s.launchedRun), 80)}
          </Button>
        </Callout>
      )}
      {read(s.message) && (
        <Callout tone="danger">
          <Text>{read(s.message)}</Text>
        </Callout>
      )}
      {read(s.editor) ? (
        <Profiles />
      ) : !list || read(s.switching) ? (
        <Skeleton lines={6} />
      ) : !profile ? (
        <EmptyState title="No Dagster connection" icon="database">
          <Button onPress={() => edit()}>Add Profile</Button>
          <Button
            disabled={disabled || !list.workspaceId}
            onPress={() =>
              s.action(async () => {
                const workspaceId = s.scope().workspaceId
                const { profile: saved } = await plugin.request(
                  'profiles.save',
                  {
                    workspaceId,
                    profile: {
                      id: 'local',
                      name: 'Local Dagster',
                      kind: 'local',
                      url: 'http://localhost:3000',
                      auth: { mode: 'none' }
                    }
                  },
                  s.requestOptions()
                )
                try {
                  await plugin.request(
                    'local.start',
                    { workspaceId, profileId: saved.id },
                    s.requestOptions()
                  )
                } catch (error) {
                  await s.reloadProfiles()
                  if (s.scope().workspaceId === workspaceId) s.fail(error)
                  return
                }
                await s.reloadProfiles()
              }, false)
            }
          >
            Start Local Dagster
          </Button>
        </EmptyState>
      ) : !data ? (
        <Skeleton lines={6} />
      ) : status === 'auth-error' ? (
        <Callout tone="danger" title="Authentication failed">
          <Text>
            {s.clip(
              [profile.auth.mode, profile.auth.envVar ?? profile.auth.secretKey, data.error]
                .filter(Boolean)
                .join(' · '),
              600
            )}
          </Text>
          <Button onPress={() => edit(profile)}>Edit Profile</Button>
        </Callout>
      ) : status === 'stopped' ? (
        <EmptyState
          title="Local Dagster is stopped"
          icon="play"
          description={s.clip(local?.diagnostics, 200)}
        >
          <Button disabled={disabled} onPress={() => localAction('local.start')}>
            Start Local Dagster
          </Button>
        </EmptyState>
      ) : (
        <Stack>
          {data.error && (
            <Callout tone="warning">
              <Text>{s.clip(data.error, 700)}</Text>
            </Callout>
          )}
          {data.locations
            .filter((l) => l.error)
            .slice(0, 6)
            .map((l) => (
              <Callout key={l.name} tone="warning" title="Code location failed to load">
                <Collapsible title={s.clip(l.name)}>
                  <CodeView language="text" wrap code={s.clip(l.error, 2500)} />
                </Collapsible>
                <Button
                  disabled={disabled}
                  onPress={() =>
                    s.action(
                      () =>
                        plugin.request(
                          'agent.loadError',
                          { ...s.scope(), locationName: l.name },
                          s.requestOptions()
                        ),
                      false
                    )
                  }
                >
                  Fix Load Error with Agent
                </Button>
                <Button disabled={disabled} onPress={reload}>
                  Reload
                </Button>
              </Callout>
            ))}
          {read(explorer.backfill) && (
            <Text>{'Backfill ' + read(explorer.backfill) + ' · ' + read(explorer.progress)}</Text>
          )}
          <Tabs
            label="Dagster views"
            value={selected}
            onChange={(v) => {
              set(s.tab, v)
              if (v === 'dag' && !read(explorer.index))
                void explorer.ensureGraphIndex().catch(s.fail)
              if (v === 'runs') {
                if (read(s.runId)) void s.showRun(read(s.runId))
                else void s.loadPage()
              }
            }}
          >
            <Tab value="dag" label="DAG" icon="branch">
              {selected === 'dag' && <Graph data={data} />}
            </Tab>
            <Tab
              value="assets"
              label={'Assets (' + (read(explorer.graph).length || data.assets.length) + ')'}
              icon="layers"
            >
              {selected === 'assets' && (
                <Stack>
                  <Button onPress={() => set(s.tab, partitionsOnly ? 'assets' : 'partitions')}>
                    {partitionsOnly ? 'All assets' : 'Partitioned assets'}
                  </Button>
                  <Assets data={data} partitionsOnly={partitionsOnly} />
                </Stack>
              )}
            </Tab>
            <Tab value="automation" label="Automation" icon="zap">
              {selected === 'automation' && <Automation data={data} />}
            </Tab>
            <Tab value="runs" label="Runs" icon="activity">
              {selected === 'runs' && <Runs data={data} />}
            </Tab>
          </Tabs>
        </Stack>
      )}
    </Stack>
  )
}

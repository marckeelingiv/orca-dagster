/** @jsxImportSource preact */
import { toChildArray, isValidElement, type VNode, type ComponentChildren } from 'preact'
import { useState } from 'preact/hooks'
import type * as UI from 'orca'
export * from './bridge'
type Props<T> = Omit<T, 'children'> & { children?: ComponentChildren }
type Box = {
  children?: ComponentChildren
  label?: string
  title?: string
  description?: string
  tone?: string
}
export const Stack = ({ children }: Box) => <div class="stack">{children}</div>
export const Row = ({ children, wrap, justify }: Props<UI.RowProps>) => (
  <div
    class={'row' + (wrap ? ' wrap' : '')}
    style={{ justifyContent: justify === 'between' ? 'space-between' : justify }}
  >
    {children}
  </div>
)
export const Toolbar = ({ children, label }: Box) => (
  <div class="toolbar" role="toolbar" aria-label={label}>
    {children}
  </div>
)
export const Text = ({ children, tone, muted, size }: Props<UI.TextProps>) => (
  <span class={[tone, muted && 'muted', size].filter(Boolean).join(' ')}>{children}</span>
)
export const Badge = ({ children, tone }: Box) => (
  <small class={'badge ' + (tone ?? '')}>{children}</small>
)
export const Card = ({ children, title }: Box) => (
  <section>
    {title && <h3>{title}</h3>}
    {children}
  </section>
)
export const Heading = ({ children }: Box) => <h3>{children}</h3>
export const Callout = ({ children, title, tone }: Box) => (
  <section class={'callout ' + (tone ?? '')} role="status">
    {title && <strong>{title}</strong>}
    {children}
  </section>
)
export const EmptyState = ({ children, title, description }: Box) => (
  <section>
    <h3>{title}</h3>
    {description && <p>{description}</p>}
    {children}
  </section>
)
export const Skeleton = () => <p role="status">Loading…</p>
const iconPaths: Partial<Record<UI.IconName, string>> = {
  refresh: 'M13 5a5.5 5.5 0 1 0 .3 5M13 2v3.5H9.5',
  settings: 'M2 4h5m3 0h4M2 8h8m3 0h1M2 12h2m3 0h7M7 2.5v3M10 6.5v3M4 10.5v3',
  play: 'M4.5 2.8 13 8l-8.5 5.2z',
  stop: 'M4 4h8v8H4z',
  terminal: 'M2 3h12v10H2zM4 6l2 2-2 2m4 0h3',
  check: 'm2.5 8 3.5 3.5 7.5-8',
  error: 'M8 2a6 6 0 1 0 0 12A6 6 0 0 0 8 2zm0 3v3m0 2v1',
  warning: 'm8 2 6.5 12h-13zM8 6v3m0 1.5v1',
  layers: 'm8 2 6 3-6 3-6-3zM2 8l6 3 6-3M2 11l6 3 6-3',
  'open-external': 'M8 3h5v5m0-5-7 7m-1-6H2v10h10v-3',
  copy: 'M6 2h8v10H6zM3 5H2v9h8v-1',
  add: 'M8 2v12M2 8h12',
  collapse: 'm10 3-5 5 5 5',
  bot: 'M2 5h12v8H2zM8 2v3M5 8v1m6-1v1',
  branch: 'M4 2v12m0-6c6 0 8-1 8-6',
  filter: 'M2 3h12L9 9v4l-2 1V9z'
}
function Icon({ name }: { name?: UI.IconName }) {
  if (!name) return null
  return (
    <svg class="icon" data-icon={name} aria-hidden="true" viewBox="0 0 16 16">
      <path d={iconPaths[name] ?? 'M8 3a5 5 0 1 0 0 10A5 5 0 0 0 8 3z'} />
    </svg>
  )
}
export const Button = ({
  children,
  icon,
  tone,
  variant,
  onPress,
  disabled,
  tooltip
}: Props<UI.ButtonProps>) => (
  <button
    data-tone={tone}
    data-variant={variant}
    disabled={disabled}
    title={tooltip}
    onClick={onPress}
  >
    <Icon name={icon} />
    {children}
  </button>
)
export const IconButton = ({
  label,
  tooltip,
  icon,
  tone,
  variant,
  onPress,
  disabled
}: UI.IconButtonProps) => (
  <button
    class="icon-btn"
    aria-label={label}
    title={tooltip ?? label}
    data-tone={tone}
    data-variant={variant}
    disabled={disabled}
    onClick={onPress}
  >
    <Icon name={icon} />
  </button>
)
export const Link = ({ children, onPress }: Props<UI.LinkProps>) => (
  <button class="link" onClick={onPress}>
    {children}
  </button>
)
export function Input(p: UI.InputProps) {
  const attrs = {
    value: p.value,
    'aria-invalid': p.invalid,
    placeholder: p.placeholder,
    maxLength: p.maxLength,
    disabled: p.disabled,
    onInput: (e: { currentTarget: HTMLInputElement }) => p.onChange?.(e.currentTarget.value),
    onKeyDown: (e: { key: string; currentTarget: HTMLInputElement }) => {
      if (e.key === 'Enter') p.onSubmit?.(e.currentTarget.value)
    }
  }
  return (
    <label class="field">
      <span class={p.labelHidden ? 'sr-only' : undefined}>{p.label}</span>
      {p.type === 'password' ? (
        <input type="password" {...attrs} />
      ) : p.type === 'search' ? (
        <input type="search" {...attrs} />
      ) : (
        <input type="text" {...attrs} />
      )}
    </label>
  )
}
export const Textarea = (p: UI.TextareaProps) => (
  <label class="field">
    <span class={p.labelHidden ? 'sr-only' : undefined}>{p.label}</span>
    <textarea
      value={p.value}
      placeholder={p.placeholder}
      maxLength={p.maxLength}
      disabled={p.disabled}
      onInput={(e) => p.onChange?.(e.currentTarget.value)}
    />
  </label>
)
export const Select = (p: UI.SelectProps) => (
  <label class="field">
    <span class={p.labelHidden ? 'sr-only' : undefined}>{p.label}</span>
    <select
      value={p.value ?? p.defaultValue ?? (p.placeholder ? '' : undefined)}
      disabled={p.disabled}
      onChange={(e) => p.onChange?.(e.currentTarget.value)}
    >
      {p.placeholder && (
        <option disabled value="">
          {p.placeholder}
        </option>
      )}
      {p.options.map((o) => (
        <option key={o.value} value={o.value} disabled={o.disabled}>
          {o.label}
        </option>
      ))}
    </select>
  </label>
)
export const Checkbox = (p: UI.CheckboxProps) => (
  <label class="choice">
    <input
      type="checkbox"
      checked={p.checked}
      disabled={p.disabled}
      onChange={(e) => p.onChange?.(e.currentTarget.checked)}
    />
    {p.label}
  </label>
)
export const Switch = Checkbox
export function Collapsible({ children, title, defaultOpen }: Props<UI.CollapsibleProps>) {
  const [expanded, setExpanded] = useState(!!defaultOpen)
  return (
    <details open={defaultOpen} onToggle={(event) => setExpanded(event.currentTarget.open)}>
      <summary>{title}</summary>
      {expanded && children}
    </details>
  )
}
export const CodeView = (p: UI.CodeViewProps) => <pre data-max-height={p.maxHeight}>{p.code}</pre>
export function RelativeTime({ at, format = 'short' }: UI.RelativeTimeProps) {
  const date = new Date(at)
  if (!Number.isFinite(date.getTime())) return <span>—</span>
  const minutes = Math.floor(Math.abs(Date.now() - at) / 60000)
  const unit = minutes < 60 ? 'minute' : minutes < 1440 ? 'hour' : 'day'
  const value = Math.floor(minutes / { minute: 1, hour: 60, day: 1440 }[unit])
  const age = format === 'long' ? value + ' ' + unit + (value === 1 ? '' : 's') : value + unit[0]
  return (
    <time dateTime={date.toISOString()} title={date.toLocaleString()}>
      {format === 'duration'
        ? age
        : !minutes
          ? 'just now'
          : at > Date.now()
            ? 'in ' + age
            : age + ' ago'}
    </time>
  )
}
export const KeyValue = (p: UI.KeyValueProps) => (
  <dl>
    {p.items.map((item) => (
      <div key={item.label}>
        <dt>{item.label}</dt>
        <dd>{String(item.value ?? '—')}</dd>
      </div>
    ))}
  </dl>
)
export const List = Stack
export const ListItem = ({
  children,
  title,
  description,
  current,
  onPress
}: Props<UI.ListItemProps>) => (
  <div class="item" aria-current={current || undefined}>
    {title && (onPress ? <button onClick={onPress}>{title}</button> : <strong>{title}</strong>)}
    {description && <small>{description}</small>}
    {children}
  </div>
)
export const Tab = ({ children, value }: Props<UI.TabProps>) => (
  <div class={'tab-panel tab-' + value}>{children}</div>
)
export function Tabs({ children, value, onChange, label }: Props<UI.TabsProps>) {
  const tabs = toChildArray(children).filter(isValidElement) as VNode<Props<UI.TabProps>>[]
  return (
    <div class="tabs">
      <nav aria-label={label}>
        {tabs.map((t) => (
          <button
            key={t.props.value}
            aria-current={t.props.value === value ? 'page' : undefined}
            onClick={() => onChange?.(t.props.value)}
          >
            {t.props.label}
          </button>
        ))}
      </nav>
      {tabs.find((t) => t.props.value === value)}
    </div>
  )
}
export function Tree(p: UI.TreeProps) {
  function item(n: UI.TreeItem): ComponentChildren {
    return n.children ? (
      <details key={n.key} open={p.defaultExpanded?.includes(n.key)}>
        <summary>
          <Icon name={n.icon} />
          {n.label}
        </summary>
        {n.children.map(item)}
      </details>
    ) : (
      <button
        key={n.key}
        class="tree-row"
        disabled={n.disabled}
        aria-pressed={p.selected === n.key}
        onClick={() => p.onSelect?.({ key: n.key })}
      >
        <Icon name={n.icon} />
        {n.label}
        <small>{n.description}</small>
      </button>
    )
  }
  return (
    <div class="tree" data-max-height={p.maxHeight} aria-label={p.label}>
      {p.items.map(item)}
    </div>
  )
}
export function Table(p: UI.TableProps) {
  return (
    <div class="scroll" data-max-height={p.maxHeight}>
      <table aria-label={p.label}>
        <thead>
          <tr>
            {p.columns.map((c) => (
              <th key={c.key}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {p.rows.map((r) => {
            const select = () => p.onSelectionChange?.({ keys: [r.key] })
            const activate = () => p.onRowAction?.({ key: r.key })
            return (
              <tr
                key={r.key}
                tabIndex={r.disabled ? undefined : 0}
                aria-disabled={r.disabled}
                aria-selected={p.selected?.includes(r.key)}
                onClick={() => !r.disabled && select()}
                onDblClick={() => !r.disabled && activate()}
                onKeyDown={(e) => {
                  if (r.disabled || !['Enter', ' '].includes(e.key)) return
                  e.preventDefault()
                  if (e.key === 'Enter' && p.onRowAction) activate()
                  else select()
                }}
              >
                {p.columns.map((c) => {
                  const v = r.cells[c.key]
                  return (
                    <td key={c.key}>
                      {v && typeof v === 'object' ? <RelativeTime {...v} /> : String(v ?? '')}
                    </td>
                  )
                })}
              </tr>
            )
          })}
        </tbody>
      </table>
      {!p.rows.length && <p>{p.emptyText}</p>}
      {p.hasMore && (
        <button disabled={p.loading} onClick={p.onLoadMore}>
          Load more
        </button>
      )}
    </div>
  )
}

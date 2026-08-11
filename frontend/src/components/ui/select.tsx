import { Header, ListBox, Select } from '@heroui/react'
import { motion, useReducedMotion } from 'framer-motion'
import { Search, X } from 'lucide-react'
import * as React from 'react'
import { cn } from '@/lib/utils'

/** React Aria ╨╜╨╡ ╨╗╤О╨▒╨╕╤В ╨┐╤Г╤Б╤В╨╛╨╣ id тАФ ╨╝╨░╨┐╨┐╨╕╨╝ "" тЖФ sentinel */
const EMPTY_KEY = '__wms_empty__'

type OptionItem = {
  value: string
  label: string
  disabled?: boolean
}

type ParsedChild =
  | { kind: 'option'; option: OptionItem }
  | { kind: 'group'; id: string; label: string; options: OptionItem[] }

function textFromNode(node: React.ReactNode): string {
  if (node == null || typeof node === 'boolean') return ''
  if (typeof node === 'string' || typeof node === 'number') return String(node)
  if (Array.isArray(node)) return node.map(textFromNode).filter(Boolean).join(' ').trim()
  if (React.isValidElement<{ children?: React.ReactNode }>(node)) {
    return textFromNode(node.props.children)
  }
  return ''
}

function parseOption(element: React.ReactElement): OptionItem {
  const props = element.props as React.OptionHTMLAttributes<HTMLOptionElement> & {
    children?: React.ReactNode
  }
  return {
    value: props.value != null ? String(props.value) : '',
    label: textFromNode(props.children),
    disabled: Boolean(props.disabled),
  }
}

function parseSelectChildren(children: React.ReactNode): ParsedChild[] {
  const result: ParsedChild[] = []

  React.Children.forEach(children, (child, index) => {
    if (!React.isValidElement(child)) return

    if (child.type === 'option') {
      result.push({ kind: 'option', option: parseOption(child) })
      return
    }

    if (child.type === 'optgroup') {
      const props = child.props as React.OptgroupHTMLAttributes<HTMLOptGroupElement> & {
        children?: React.ReactNode
        label?: string
      }
      const options: OptionItem[] = []
      React.Children.forEach(props.children, (opt) => {
        if (!React.isValidElement(opt) || opt.type !== 'option') return
        options.push(parseOption(opt))
      })
      result.push({
        kind: 'group',
        id: `group-${index}-${props.label ?? ''}`,
        label: props.label ?? '',
        options,
      })
    }
  })

  return result
}

function filterParsed(parsed: ParsedChild[], query: string): ParsedChild[] {
  const normalized = query.trim().toLowerCase()
  if (!normalized) return parsed

  const result: ParsedChild[] = []
  for (const node of parsed) {
    if (node.kind === 'option') {
      if (node.option.label.toLowerCase().includes(normalized)) result.push(node)
      continue
    }
    const options = node.options.filter((option) =>
      option.label.toLowerCase().includes(normalized),
    )
    if (options.length > 0) result.push({ ...node, options })
  }
  return result
}

function toKey(value: string): string {
  return value === '' ? EMPTY_KEY : value
}

function fromKey(key: React.Key | null): string {
  if (key == null) return ''
  const value = String(key)
  return value === EMPTY_KEY ? '' : value
}

function synthesizeChangeEvent(
  value: string,
  name: string | undefined,
): React.ChangeEvent<HTMLSelectElement> {
  const target = {
    value,
    name: name ?? '',
  } as HTMLSelectElement

  return {
    target,
    currentTarget: target,
    type: 'change',
    bubbles: true,
    cancelable: false,
    defaultPrevented: false,
    eventPhase: 0,
    isTrusted: false,
    nativeEvent: new Event('change'),
    preventDefault() {},
    isDefaultPrevented: () => false,
    stopPropagation() {},
    isPropagationStopped: () => false,
    persist() {},
    timeStamp: Date.now(),
  }
}

const springPanel = { type: 'spring' as const, stiffness: 520, damping: 36, mass: 0.85 }
const springItem = { type: 'spring' as const, stiffness: 480, damping: 32, mass: 0.7 }

export type SelectNativeProps = Omit<React.SelectHTMLAttributes<HTMLSelectElement>, 'size'> & {
  placeholder?: string
  searchable?: boolean
  searchPlaceholder?: string
}

/**
 * Drop-in ╨╖╨░╨╝╨╡╨╜╨░ native `<select>` ╨╜╨░ HeroUI Select + ListBox.
 * ╨Ъ╨╗╨╕╨║╨╕ ╤З╨╡╤А╨╡╨╖ React Aria (`isNonModal` тАФ ╤А╨░╨▒╨╛╤В╨░╨╡╤В ╨▓╨╜╤Г╤В╤А╨╕ Dialog).
 * Spring/stagger тАФ ╤В╨╛╨╗╤М╨║╨╛ ╨▓╨╕╨╖╤Г╨░╨╗, ╨▒╨╡╨╖ ╤Б╨▓╨╛╨╡╨│╨╛ portal.
 */
export const SelectNative = React.forwardRef<HTMLSelectElement, SelectNativeProps>(
  (
    {
      className,
      children,
      value,
      defaultValue,
      onChange,
      onBlur,
      disabled,
      name,
      id,
      required,
      placeholder = '╨Т╤Л╨▒╨╡╤А╨╕╤В╨╡',
      searchable = false,
      searchPlaceholder = '╨Я╨╛╨╕╤Б╨║тАж',
      'aria-label': ariaLabel,
      ...rest
    },
    _ref,
  ) => {
    void _ref
    void required
    void rest

    const reduceMotion = useReducedMotion()
    const rootRef = React.useRef<HTMLDivElement>(null)
    const searchRef = React.useRef<HTMLInputElement>(null)
    const [isOpen, setIsOpen] = React.useState(false)
    const [search, setSearch] = React.useState('')

    const parsed = React.useMemo(() => parseSelectChildren(children), [children])
    const visible = React.useMemo(
      () => (searchable ? filterParsed(parsed, search) : parsed),
      [parsed, searchable, search],
    )

    const controlled = value !== undefined
    const selectedKey = controlled ? toKey(String(value)) : undefined
    const defaultSelectedKey =
      !controlled && defaultValue !== undefined ? toKey(String(defaultValue)) : undefined

    // isNonModal ╨▓╨╜╤Г╤В╤А╨╕ Dialog ╤Б╨░╨╝ ╨╜╨╡ ╨╖╨░╨║╤А╤Л╨▓╨░╨╡╤В╤Б╤П ╨┐╨╛ ╨║╨╗╨╕╨║╤Г ╤Б╨╜╨░╤А╤Г╨╢╨╕
    React.useEffect(() => {
      if (!isOpen) return

      const onPointerDown = (event: PointerEvent) => {
        const target = event.target
        if (!(target instanceof Element)) return
        if (target.closest('[data-slot="select-popover"]')) return
        if (rootRef.current?.contains(target)) {
          // Trigger press while open: swallow it, otherwise the popover closes and reopens
          event.preventDefault()
          event.stopPropagation()
          setIsOpen(false)
          return
        }
        setIsOpen(false)
      }

      document.addEventListener('pointerdown', onPointerDown, true)
      return () => document.removeEventListener('pointerdown', onPointerDown, true)
    }, [isOpen])

    React.useEffect(() => {
      if (!isOpen) {
        setSearch('')
        return
      }
      if (!searchable) return
      const frame = requestAnimationFrame(() => searchRef.current?.focus())
      return () => cancelAnimationFrame(frame)
    }, [isOpen, searchable])

    let optionIndex = 0

    const renderOption = (option: OptionItem) => {
      const idKey = toKey(option.value)
      const index = optionIndex++
      const itemTransition = reduceMotion
        ? { duration: 0 }
        : { ...springItem, delay: 0.02 + index * 0.035 }

      return (
        <ListBox.Item key={idKey} id={idKey} textValue={option.label || ' '} isDisabled={option.disabled}>
          <motion.span
            className="flex w-full min-w-0 items-center truncate"
            initial={reduceMotion || !isOpen ? false : { opacity: 0, y: -6 }}
            animate={{ opacity: 1, y: 0 }}
            transition={itemTransition}
          >
            {option.label}
          </motion.span>
          <ListBox.ItemIndicator />
        </ListBox.Item>
      )
    }

    const hasOptions = visible.some((node) =>
      node.kind === 'option' ? true : node.options.length > 0,
    )

    return (
      <div ref={rootRef} className={cn('w-full', className)}>
        <Select
          aria-label={ariaLabel}
          className="w-full"
          defaultSelectedKey={defaultSelectedKey}
          fullWidth
          id={id}
          isDisabled={disabled}
          isOpen={isOpen}
          name={name}
          placeholder={placeholder}
          selectedKey={selectedKey}
          onBlur={onBlur as (() => void) | undefined}
          onOpenChange={setIsOpen}
          onSelectionChange={(key) => {
            onChange?.(synthesizeChangeEvent(fromKey(key), name))
            setIsOpen(false)
          }}
        >
          <Select.Trigger>
            <Select.Value />
            <Select.Indicator />
          </Select.Trigger>
          {/*
            isNonModal: ╨╕╨╜╨░╤З╨╡ Escape/underlay ╨╖╨░╨║╤А╤Л╨▓╨░╤О╤В Dialog ╨╕ ╤Б╤В╨░╨▓╤П╤В inert ╨╜╨░ #root тАФ
            ╨║╨╗╨╕╨║╨╕ ╨┐╨╛ portaled-╤Б╨┐╨╕╤Б╨║╤Г ╨╜╨╡ ╨┤╨╛╤Е╨╛╨┤╤П╤В.
          */}
          <Select.Popover isNonModal className="pointer-events-auto">
            <motion.div
              initial={reduceMotion ? false : { opacity: 0, height: 0 }}
              animate={{ opacity: 1, height: 'auto' }}
              transition={reduceMotion ? { duration: 0 } : springPanel}
              className="overflow-hidden"
            >
              {searchable ? (
                <div
                  className="flex h-10 items-center gap-2 border-b border-border px-2.5"
                  // ╨╜╨╡ ╨┤╨░╤С╨╝ ListBox/Select ╨┐╨╡╤А╨╡╤Е╨▓╨░╤В╨╕╤В╤М ╨▓╨▓╨╛╨┤
                  onPointerDown={(event) => event.stopPropagation()}
                  onKeyDown={(event) => event.stopPropagation()}
                >
                  <Search className="size-3.5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
                  <input
                    ref={searchRef}
                    type="text"
                    value={search}
                    placeholder={searchPlaceholder}
                    aria-label={searchPlaceholder}
                    className="h-full w-full bg-transparent text-sm text-foreground outline-none placeholder:text-muted-foreground"
                    onChange={(event) => setSearch(event.target.value)}
                  />
                  {search ? (
                    <button
                      type="button"
                      aria-label="╨Ю╤З╨╕╤Б╤В╨╕╤В╤М ╨┐╨╛╨╕╤Б╨║"
                      className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                      onClick={() => {
                        setSearch('')
                        searchRef.current?.focus()
                      }}
                    >
                      <X className="size-3.5" strokeWidth={1.75} />
                    </button>
                  ) : null}
                </div>
              ) : null}

              <ListBox
                renderEmptyState={
                  !hasOptions
                    ? () => (
                        <div className="px-3 py-2 text-sm text-muted-foreground">╨Э╨╕╤З╨╡╨│╨╛ ╨╜╨╡ ╨╜╨░╨╣╨┤╨╡╨╜╨╛</div>
                      )
                    : undefined
                }
              >
                {visible.map((node) => {
                  if (node.kind === 'option') return renderOption(node.option)
                  return (
                    <ListBox.Section key={node.id} id={node.id}>
                      <Header>{node.label}</Header>
                      {node.options.map(renderOption)}
                    </ListBox.Section>
                  )
                })}
              </ListBox>
            </motion.div>
          </Select.Popover>
        </Select>
      </div>
    )
  },
)
SelectNative.displayName = 'SelectNative'

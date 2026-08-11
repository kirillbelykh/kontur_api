import { createPortal } from 'react-dom'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import { Check, ChevronDown, Search, X } from 'lucide-react'
import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * Собственный дропдаун вместо React Aria Select.
 * Панель позиционируется как position:fixed от rect триггера — это не зависит
 * от буферизованной математики overlay'ев RAC, которая ломается под CSS `zoom`
 * (масштаб страницы 110% и т.п.). Мягкий фон + тонкая граница — по скрину 3.
 */

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
      result.push({ kind: 'group', id: `group-${index}-${props.label ?? ''}`, label: props.label ?? '', options })
    }
  })
  return result
}

function flattenOptions(parsed: ParsedChild[]): OptionItem[] {
  const flat: OptionItem[] = []
  for (const node of parsed) {
    if (node.kind === 'option') flat.push(node.option)
    else flat.push(...node.options)
  }
  return flat
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
    const options = node.options.filter((option) => option.label.toLowerCase().includes(normalized))
    if (options.length > 0) result.push({ ...node, options })
  }
  return result
}

function synthesizeChangeEvent(value: string, name?: string): React.ChangeEvent<HTMLSelectElement> {
  const target = { value, name: name ?? '' } as HTMLSelectElement
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

type PanelRect = { left: number; top: number; width: number; maxHeight: number; placement: 'bottom' | 'top' }

function computeRect(trigger: HTMLElement): PanelRect {
  const rect = trigger.getBoundingClientRect()
  const gap = 4
  const viewport = window.innerHeight
  const spaceBelow = viewport - rect.bottom - gap
  const spaceAbove = rect.top - gap
  const openUp = spaceBelow < 220 && spaceAbove > spaceBelow
  const maxHeight = Math.max(160, Math.min(320, openUp ? spaceAbove : spaceBelow))
  return {
    left: rect.left,
    top: openUp ? rect.top - gap : rect.bottom + gap,
    width: rect.width,
    maxHeight,
    placement: openUp ? 'top' : 'bottom',
  }
}

export type SelectNativeProps = Omit<React.SelectHTMLAttributes<HTMLSelectElement>, 'size'> & {
  placeholder?: string
  searchable?: boolean
  searchPlaceholder?: string
}

export const SelectNative = React.forwardRef<HTMLSelectElement, SelectNativeProps>(
  (
    {
      className,
      children,
      value,
      defaultValue,
      onChange,
      disabled,
      name,
      id,
      placeholder = 'Выберите',
      searchable = false,
      searchPlaceholder = 'Поиск…',
      'aria-label': ariaLabel,
    },
    _ref,
  ) => {
    void _ref
    const reduceMotion = useReducedMotion()
    const triggerRef = React.useRef<HTMLButtonElement>(null)
    const panelRef = React.useRef<HTMLDivElement>(null)
    const searchRef = React.useRef<HTMLInputElement>(null)
    const [open, setOpen] = React.useState(false)
    const [search, setSearch] = React.useState('')
    const [rect, setRect] = React.useState<PanelRect | null>(null)
    const [activeIndex, setActiveIndex] = React.useState(0)

    const parsed = React.useMemo(() => parseSelectChildren(children), [children])
    const [internalValue, setInternalValue] = React.useState<string>(
      value !== undefined ? String(value) : defaultValue !== undefined ? String(defaultValue) : '',
    )
    const currentValue = value !== undefined ? String(value) : internalValue

    const visible = React.useMemo(
      () => (searchable ? filterParsed(parsed, search) : parsed),
      [parsed, searchable, search],
    )
    const flatVisible = React.useMemo(() => flattenOptions(visible), [visible])
    const selectedLabel = React.useMemo(() => {
      const found = flattenOptions(parsed).find((option) => option.value === currentValue && option.value !== '')
      return found?.label ?? ''
    }, [parsed, currentValue])

    const reposition = React.useCallback(() => {
      if (triggerRef.current) setRect(computeRect(triggerRef.current))
    }, [])

    React.useEffect(() => {
      if (!open) return
      reposition()
      const onScroll = () => reposition()
      window.addEventListener('scroll', onScroll, true)
      window.addEventListener('resize', onScroll)
      return () => {
        window.removeEventListener('scroll', onScroll, true)
        window.removeEventListener('resize', onScroll)
      }
    }, [open, reposition])

    React.useEffect(() => {
      if (!open) return
      const onPointerDown = (event: PointerEvent) => {
        const target = event.target as Node | null
        if (triggerRef.current?.contains(target) || panelRef.current?.contains(target)) return
        setOpen(false)
      }
      document.addEventListener('pointerdown', onPointerDown, true)
      return () => document.removeEventListener('pointerdown', onPointerDown, true)
    }, [open])

    React.useEffect(() => {
      if (open && searchable) {
        const frame = requestAnimationFrame(() => searchRef.current?.focus())
        return () => cancelAnimationFrame(frame)
      }
      if (!open) setSearch('')
    }, [open, searchable])

    const commit = (option: OptionItem) => {
      if (option.disabled) return
      if (value === undefined) setInternalValue(option.value)
      onChange?.(synthesizeChangeEvent(option.value, name))
      setOpen(false)
      triggerRef.current?.focus()
    }

    const onTriggerKeyDown = (event: React.KeyboardEvent) => {
      if (disabled) return
      if (!open && (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ')) {
        event.preventDefault()
        setOpen(true)
        setActiveIndex(Math.max(0, flatVisible.findIndex((o) => o.value === currentValue)))
        return
      }
      if (!open) return
      if (event.key === 'Escape') {
        event.preventDefault()
        setOpen(false)
      } else if (event.key === 'ArrowDown') {
        event.preventDefault()
        setActiveIndex((index) => Math.min(flatVisible.length - 1, index + 1))
      } else if (event.key === 'ArrowUp') {
        event.preventDefault()
        setActiveIndex((index) => Math.max(0, index - 1))
      } else if (event.key === 'Enter') {
        event.preventDefault()
        const option = flatVisible[activeIndex]
        if (option) commit(option)
      }
    }

    let flatCursor = 0
    const renderOption = (option: OptionItem) => {
      const index = flatCursor++
      const selected = option.value === currentValue && option.value !== ''
      const active = index === activeIndex
      return (
        <button
          key={`${option.value}-${index}`}
          type="button"
          role="option"
          aria-selected={selected}
          disabled={option.disabled}
          onMouseEnter={() => setActiveIndex(index)}
          onClick={() => commit(option)}
          className={cn(
            'flex w-full items-center gap-2 rounded-md px-2.5 py-1.5 text-left text-sm transition-colors',
            active ? 'bg-muted text-foreground' : 'text-foreground',
            option.disabled && 'cursor-not-allowed opacity-40',
          )}
        >
          <span className="min-w-0 flex-1 truncate">{option.label || '\u00A0'}</span>
          {selected ? <Check className="h-3.5 w-3.5 shrink-0 text-foreground" strokeWidth={2.25} /> : null}
        </button>
      )
    }

    const hasOptions = flatVisible.length > 0

    return (
      <>
        <button
          ref={triggerRef}
          type="button"
          id={id}
          name={name}
          disabled={disabled}
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-label={ariaLabel}
          onClick={() => !disabled && setOpen((prev) => !prev)}
          onKeyDown={onTriggerKeyDown}
          className={cn(
            'flex h-9 w-full items-center gap-2 rounded-[var(--field-radius,0.5rem)] px-2.5 text-left text-sm transition-colors',
            'border bg-[var(--field-bg)] text-foreground',
            open ? 'border-[var(--field-border-focus)]' : 'border-[var(--field-border)] hover:border-[var(--field-border-hover)]',
            disabled && 'cursor-not-allowed opacity-50',
            className,
          )}
          style={open ? { boxShadow: '0 0 0 3px var(--field-ring)' } : undefined}
        >
          <span className={cn('min-w-0 flex-1 truncate', !selectedLabel && 'text-muted-foreground')}>
            {selectedLabel || placeholder}
          </span>
          <ChevronDown className={cn('h-4 w-4 shrink-0 text-muted-foreground transition-transform', open && 'rotate-180')} />
        </button>

        {open && rect
          ? createPortal(
              <div
                ref={panelRef}
                data-slot="select-popover"
                className="fixed z-[9999]"
                style={{
                  left: rect.left,
                  width: rect.width,
                  ...(rect.placement === 'bottom'
                    ? { top: rect.top }
                    : { top: rect.top, transform: 'translateY(-100%)' }),
                }}
              >
                <motion.div
                  initial={reduceMotion ? false : { opacity: 0, y: rect.placement === 'bottom' ? -6 : 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={reduceMotion ? { duration: 0 } : { type: 'spring', stiffness: 520, damping: 36, mass: 0.7 }}
                  className="overflow-hidden rounded-xl border border-border bg-card shadow-panel"
                >
                  {searchable ? (
                    <div className="flex h-10 items-center gap-2 border-b border-border px-2.5">
                      <Search className="size-3.5 shrink-0 text-muted-foreground" strokeWidth={1.75} />
                      <input
                        ref={searchRef}
                        type="text"
                        value={search}
                        placeholder={searchPlaceholder}
                        aria-label={searchPlaceholder}
                        className="h-full w-full bg-transparent text-sm text-foreground outline-none placeholder:text-muted-foreground"
                        onChange={(event) => {
                          setSearch(event.target.value)
                          setActiveIndex(0)
                        }}
                        onKeyDown={(event) => {
                          if (event.key === 'ArrowDown' || event.key === 'ArrowUp' || event.key === 'Enter' || event.key === 'Escape') {
                            onTriggerKeyDown(event as unknown as React.KeyboardEvent)
                          }
                        }}
                      />
                      {search ? (
                        <button
                          type="button"
                          aria-label="Очистить поиск"
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

                  <div className="overflow-auto p-1" style={{ maxHeight: rect.maxHeight }} role="listbox">
                    {!hasOptions ? (
                      <div className="px-3 py-2 text-sm text-muted-foreground">Ничего не найдено</div>
                    ) : (
                      visible.map((node) =>
                        node.kind === 'option' ? (
                          renderOption(node.option)
                        ) : (
                          <div key={node.id}>
                            <div className="px-2.5 pb-1 pt-2 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                              {node.label}
                            </div>
                            {node.options.map(renderOption)}
                          </div>
                        ),
                      )
                    )}
                  </div>
                </motion.div>
              </div>,
              document.body,
            )
          : null}
        <AnimatePresence />
      </>
    )
  },
)
SelectNative.displayName = 'SelectNative'

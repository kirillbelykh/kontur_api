import {
  Children,
  cloneElement,
  createContext,
  isValidElement,
  useContext,
  useEffect,
  useMemo,
  useState,
  type HTMLAttributes,
  type ReactElement,
  type ReactNode,
} from 'react'
import { Table as HeroTable } from '@heroui/react'
import { RotateCcw, Settings2 } from 'lucide-react'

import { cn } from '@/lib/utils'
import { resizeMapToWidths } from '@/lib/table-resize'
import { Checkbox } from '@/components/ui/checkbox'
import { Button } from '@/components/ui/button'

type TableColumn = {
  index: number
  id: string
  label: string
}

/**
 * Row activation runs through React Aria press handling, not DOM click bubbling,
 * so a checkbox inside a row would fire the control and the row action both.
 * One press is in flight at a time, so a module-level flag is enough.
 */
const ROW_CONTROL_SELECTOR =
  'input, button, a, label, select, textarea, [role="checkbox"], [role="button"], [data-slot="table-column-resizer"]'
let pressStartedOnControl = false

function notePressTarget(target: EventTarget | null) {
  pressStartedOnControl = target instanceof Element && Boolean(target.closest(ROW_CONTROL_SELECTOR))
}

type TablePreferences = {
  hidden: string[]
  widths: Record<string, number>
}

type TableLayoutValue = {
  widths: Record<string, number>
  hidden: string[]
  columns: TableColumn[]
}

const TableLayoutContext = createContext<TableLayoutValue>({
  widths: {},
  hidden: [],
  columns: [],
})

type TableProps = {
  children?: ReactNode
  className?: string
  'aria-label'?: string
  variant?: 'primary' | 'secondary'
}

type TableSectionProps = {
  children?: ReactNode
  className?: string
}

type TableHeadProps = Omit<HTMLAttributes<HTMLTableCellElement>, 'width' | 'id'> & {
  isRowHeader?: boolean
  id?: string
}

type TableRowProps = Omit<HTMLAttributes<HTMLTableRowElement>, 'onClick' | 'id'> & {
  id?: string | number
  onClick?: HTMLAttributes<HTMLTableRowElement>['onClick']
  children?: ReactNode
}

type TableCellProps = HTMLAttributes<HTMLTableCellElement> & {
  colSpan?: number
  textValue?: string
}

const TABLE_PREFS_VERSION = 'v3'
const TABLE_MIN_COLUMN_WIDTH = 72
const SELECT_COLUMN_WIDTH = 88
const SELECT_COLUMN_MIN_WIDTH = 56

function isSelectColumn(children: ReactNode) {
  return typeof children === 'string' && children.trim() === 'Выбор'
}

function normalizeColumnLabel(value: string, index: number) {
  const label = value.replace(/[↑↓]/g, '').replace(/\s+/g, ' ').trim()
  return label || `Колонка ${index + 1}`
}

function readPreferences(storageKey: string): TablePreferences {
  if (!storageKey || typeof window === 'undefined') return { hidden: [], widths: {} }
  try {
    const parsed = JSON.parse(window.localStorage.getItem(storageKey) || '{}')
    return {
      hidden: Array.isArray(parsed.hidden) ? parsed.hidden.filter((value: unknown): value is string => typeof value === 'string') : [],
      widths: parsed.widths && typeof parsed.widths === 'object' ? parsed.widths : {},
    }
  } catch {
    return { hidden: [], widths: {} }
  }
}

function writePreferences(storageKey: string, preferences: TablePreferences) {
  if (!storageKey || typeof window === 'undefined') return
  window.localStorage.setItem(storageKey, JSON.stringify(preferences))
}

function getPathname() {
  if (typeof window === 'undefined') return 'server'
  return window.location.pathname
}

function flattenHeaderColumns(children: ReactNode): ReactNode[] {
  return Children.toArray(children).flatMap((child) => {
    if (isValidElement(child) && child.type === TableRow) {
      return Children.toArray((child.props as { children?: ReactNode }).children)
    }
    return [child]
  })
}

function columnLabelFromNode(node: ReactNode): string {
  if (typeof node === 'string' || typeof node === 'number') return String(node)
  return Children.toArray(node)
    .map((child) => {
      if (typeof child === 'string' || typeof child === 'number') return String(child)
      if (isValidElement(child)) return columnLabelFromNode((child.props as { children?: ReactNode }).children)
      return ''
    })
    .join('')
}

function collectHeaderColumns(children: ReactNode): TableColumn[] {
  const result: TableColumn[] = []
  for (const child of Children.toArray(children)) {
    if (!isValidElement(child) || child.type !== TableHeader) continue
    const heads = flattenHeaderColumns((child.props as TableSectionProps).children)
    heads.forEach((head, index) => {
      if (!isValidElement(head)) return
      const props = head.props as TableHeadProps & { children?: ReactNode }
      result.push({
        index,
        id: String(props.id || `col-${index}`),
        label: normalizeColumnLabel(columnLabelFromNode(props.children), index),
      })
    })
  }
  return result
}

export function Table({ className, children, 'aria-label': ariaLabel, variant = 'primary', ...props }: TableProps) {
  const columns = useMemo(() => collectHeaderColumns(children), [children])
  const [preferences, setPreferences] = useState<TablePreferences>({ hidden: [], widths: {} })
  const [storageKey, setStorageKey] = useState('')
  const [settingsOpen, setSettingsOpen] = useState(false)

  useEffect(() => {
    if (columns.length === 0) return
    const signature = columns.map((column) => column.label).join('|')
    // aria-label keeps two same-shaped tables on one page from sharing column preferences
    const nextStorageKey = `kontur_table_preferences_${TABLE_PREFS_VERSION}_${getPathname()}_${ariaLabel || ''}_${signature}`
    if (nextStorageKey === storageKey) return
    setStorageKey(nextStorageKey)
    setPreferences(readPreferences(nextStorageKey))
  }, [ariaLabel, columns, storageKey])

  const updatePreferences = (updater: (current: TablePreferences) => TablePreferences) => {
    setPreferences((current) => {
      const next = updater(current)
      writePreferences(storageKey, next)
      return next
    })
  }

  const applyResize = (sizes: Map<unknown, unknown>, persist: boolean) => {
    const widths = resizeMapToWidths(sizes)
    if (Object.keys(widths).length === 0) return
    setPreferences((current) => {
      const next = { ...current, widths }
      if (persist) writePreferences(storageKey, next)
      return next
    })
  }

  const visibleColumnsCount = columns.filter((column) => !preferences.hidden.includes(column.label)).length

  const toggleColumn = (label: string) => {
    updatePreferences((current) => {
      const isHidden = current.hidden.includes(label)
      if (!isHidden && visibleColumnsCount <= 1) return current

      return {
        ...current,
        hidden: isHidden ? current.hidden.filter((item) => item !== label) : [...current.hidden, label],
        // Сброс ширин: иначе 1fr колонки остаются в пикселях и справа дыра
        widths: {},
      }
    })
  }

  const resetPreferences = () => {
    const next = { hidden: [], widths: {} }
    setPreferences(next)
    writePreferences(storageKey, next)
  }

  return (
    <TableLayoutContext.Provider value={{ widths: preferences.widths, hidden: preferences.hidden, columns }}>
      <div
        className="relative w-full"
        onPointerDownCapture={(event) => notePressTarget(event.target)}
        onKeyDownCapture={() => {
          pressStartedOnControl = false
        }}
        {...props}
      >
        {columns.length > 0 ? (
          <div className="pointer-events-none absolute right-1.5 top-1.5 z-30 print:hidden">
            <div className="pointer-events-auto relative">
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="h-7 w-7 bg-muted/80 text-muted-foreground shadow-sm hover:bg-muted hover:text-foreground"
                onClick={() => setSettingsOpen((current) => !current)}
                title="Настроить колонки"
                aria-label="Настроить колонки"
              >
                <Settings2 className="h-3.5 w-3.5" />
              </Button>
              {settingsOpen ? (
                <div className="absolute right-0 z-40 mt-1 w-72 rounded-lg border border-border bg-card p-3 text-sm shadow-panel">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <div className="font-medium">Колонки таблицы</div>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 text-muted-foreground"
                      onClick={resetPreferences}
                      title="Сбросить настройки"
                      aria-label="Сбросить настройки"
                    >
                      <RotateCcw className="h-4 w-4" />
                    </Button>
                  </div>
                  <div className="max-h-72 space-y-1 overflow-y-auto pr-1">
                    {columns.map((column) => {
                      const checked = !preferences.hidden.includes(column.label)
                      return (
                        <Checkbox
                          key={`${column.index}-${column.label}`}
                          isSelected={checked}
                          isDisabled={checked && visibleColumnsCount <= 1}
                          onChange={() => toggleColumn(column.label)}
                          className="w-full rounded-md px-2 py-1.5 hover:bg-muted/70"
                        >
                          <span className="min-w-0 flex-1 truncate">{column.label}</span>
                        </Checkbox>
                      )
                    })}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        ) : null}

        <HeroTable className={cn(className)} variant={variant}>
          <HeroTable.ResizableContainer
            className="w-full"
            onResize={(widths) => applyResize(widths as Map<unknown, unknown>, false)}
            onResizeEnd={(widths) => applyResize(widths as Map<unknown, unknown>, true)}
          >
            <HeroTable.Content aria-label={ariaLabel || 'Таблица'}>
              {children}
            </HeroTable.Content>
          </HeroTable.ResizableContainer>
        </HeroTable>
      </div>
    </TableLayoutContext.Provider>
  )
}

export function TableHeader({ className, children, ...props }: TableSectionProps) {
  const { hidden } = useContext(TableLayoutContext)
  const heads = flattenHeaderColumns(children)
    .map((child, index) => ({ child, index }))
    .filter(({ child, index }) => {
      if (!isValidElement(child)) return true
      const label = normalizeColumnLabel(
        columnLabelFromNode((child.props as { children?: ReactNode }).children),
        index,
      )
      return !hidden.includes(label)
    })
  const hasRowHeader = heads.some(
    ({ child }) => isValidElement(child) && Boolean((child.props as TableHeadProps).isRowHeader),
  )

  return (
    <HeroTable.Header className={className} {...props}>
      {heads.map(({ child, index }, visibleIndex) => {
        if (!isValidElement(child)) return child
        const element = child as ReactElement<TableHeadProps>
        return cloneElement(element, {
          isRowHeader: element.props.isRowHeader || (!hasRowHeader && visibleIndex === 0),
          id: element.props.id ?? `col-${index}`,
        })
      })}
    </HeroTable.Header>
  )
}

export function TableBody({ className, children, ...props }: TableSectionProps) {
  return (
    <HeroTable.Body className={className} {...props}>
      {children}
    </HeroTable.Body>
  )
}

export function TableRow({ className, children, onClick, id, ...props }: TableRowProps) {
  const { hidden, columns } = useContext(TableLayoutContext)
  const cells = Children.toArray(children).filter((_, index) => {
    const label = columns[index]?.label
    return !label || !hidden.includes(label)
  })
  return (
    <HeroTable.Row
      id={id}
      className={className}
      onAction={
        onClick
          ? () => {
              if (pressStartedOnControl) return
              onClick({
                target: null,
                currentTarget: null,
                preventDefault() {},
                stopPropagation() {},
              } as never)
            }
          : undefined
      }
      {...props}
    >
      {cells}
    </HeroTable.Row>
  )
}

export function TableHead({ className, children, isRowHeader, id, ...props }: TableHeadProps) {
  const { widths } = useContext(TableLayoutContext)
  const width = id ? widths[id] : undefined
  const selectColumn = isSelectColumn(children)
  return (
    <HeroTable.Column
      {...props}
      id={id}
      isRowHeader={isRowHeader}
      className={cn('relative', className)}
      minWidth={selectColumn ? SELECT_COLUMN_MIN_WIDTH : TABLE_MIN_COLUMN_WIDTH}
      defaultWidth={selectColumn ? SELECT_COLUMN_WIDTH : '1fr'}
      {...(typeof width === 'number' ? { width } : {})}
    >
      {children}
      <HeroTable.ColumnResizer />
    </HeroTable.Column>
  )
}

export function TableCell({ className, children, colSpan, textValue, ...props }: TableCellProps) {
  return (
    <HeroTable.Cell className={className} colSpan={colSpan} textValue={textValue} {...props}>
      {children}
    </HeroTable.Cell>
  )
}

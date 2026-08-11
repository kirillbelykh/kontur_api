import {
  Children,
  cloneElement,
  isValidElement,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type HTMLAttributes,
  type MouseEvent,
  type ReactElement,
  type ReactNode,
} from 'react'
import { Table as HeroTable } from '@heroui/react'
import { RotateCcw, Settings2 } from 'lucide-react'

import { cn } from '@/lib/utils'
import { Checkbox } from '@/components/ui/checkbox'
import { Button } from '@/components/ui/button'

type TableColumn = {
  index: number
  label: string
}

type TablePreferences = {
  hidden: string[]
  widths: Record<string, number>
}

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

type TableHeadProps = HTMLAttributes<HTMLTableCellElement> & {
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

const TABLE_PREFS_VERSION = 'v1'
const TABLE_MIN_COLUMN_WIDTH = 72

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

export function Table({ className, children, 'aria-label': ariaLabel, variant = 'primary', ...props }: TableProps) {
  const tableRef = useRef<HTMLTableElement>(null)
  const [columns, setColumns] = useState<TableColumn[]>([])
  const [preferences, setPreferences] = useState<TablePreferences>({ hidden: [], widths: {} })
  const [storageKey, setStorageKey] = useState('')
  const [settingsOpen, setSettingsOpen] = useState(false)

  useEffect(() => {
    const table = tableRef.current
    if (!table) return

    const headerCells = Array.from(table.querySelectorAll('thead tr:first-child th'))
    const nextColumns = headerCells.map((cell, index) => ({
      index,
      label: normalizeColumnLabel(cell.textContent || '', index),
    }))
    if (nextColumns.length === 0) return

    const signature = nextColumns.map((column) => column.label).join('|')
    const nextStorageKey = `kontur_table_preferences_${TABLE_PREFS_VERSION}_${getPathname()}_${signature}`

    setColumns((current) => {
      const currentSignature = current.map((column) => column.label).join('|')
      return currentSignature === signature ? current : nextColumns
    })
    if (nextStorageKey !== storageKey) {
      setStorageKey(nextStorageKey)
      setPreferences(readPreferences(nextStorageKey))
    }
  }, [children, storageKey])

  useLayoutEffect(() => {
    const table = tableRef.current
    if (!table || columns.length === 0) return

    const rows = Array.from(table.querySelectorAll('tr'))
    for (const column of columns) {
      const hidden = preferences.hidden.includes(column.label)
      const width = preferences.widths[column.label]

      for (const row of rows) {
        const cell = row.children.item(column.index) as HTMLElement | null
        if (!cell || Number(cell.getAttribute('colspan') || '1') > 1) continue

        cell.style.display = hidden ? 'none' : ''
        if (!hidden && width) {
          cell.style.width = `${width}px`
          cell.style.minWidth = `${width}px`
        } else {
          cell.style.width = ''
          cell.style.minWidth = ''
        }
      }
    }

    const hasCustomWidths = Object.keys(preferences.widths).length > 0
    table.style.tableLayout = hasCustomWidths ? 'fixed' : ''
    if (hasCustomWidths) {
      const visibleWidth = columns
        .filter((column) => !preferences.hidden.includes(column.label))
        .reduce((total, column) => total + (preferences.widths[column.label] || TABLE_MIN_COLUMN_WIDTH), 0)
      table.style.minWidth = `${Math.max(visibleWidth, table.parentElement?.clientWidth || 0)}px`
    } else {
      table.style.minWidth = ''
    }
  }, [children, columns, preferences])

  const updatePreferences = (updater: (current: TablePreferences) => TablePreferences) => {
    setPreferences((current) => {
      const next = updater(current)
      writePreferences(storageKey, next)
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
      }
    })
  }

  const resetPreferences = () => {
    const next = { hidden: [], widths: {} }
    setPreferences(next)
    writePreferences(storageKey, next)
  }

  const handleMouseDown = (event: MouseEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement | null
    if (!target?.matches('[data-table-resize-handle]')) return

    const headerCell = target.closest('th') as HTMLTableCellElement | null
    if (!headerCell || !headerCell.parentElement) return

    const columnIndex = Array.from(headerCell.parentElement.children).indexOf(headerCell)
    const column = columns.find((candidate) => candidate.index === columnIndex)
    if (!column || preferences.hidden.includes(column.label)) return

    event.preventDefault()
    event.stopPropagation()

    const startX = event.clientX
    const startWidth = headerCell.getBoundingClientRect().width
    const previousCursor = document.body.style.cursor
    const previousUserSelect = document.body.style.userSelect
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'

    const handleMouseMove = (moveEvent: globalThis.MouseEvent) => {
      const nextWidth = Math.max(TABLE_MIN_COLUMN_WIDTH, Math.round(startWidth + moveEvent.clientX - startX))
      updatePreferences((current) => ({
        ...current,
        widths: { ...current.widths, [column.label]: nextWidth },
      }))
    }

    const handleMouseUp = () => {
      document.body.style.cursor = previousCursor
      document.body.style.userSelect = previousUserSelect
      document.removeEventListener('mousemove', handleMouseMove)
      document.removeEventListener('mouseup', handleMouseUp)
    }

    document.addEventListener('mousemove', handleMouseMove)
    document.addEventListener('mouseup', handleMouseUp)
  }

  return (
    <div className="relative w-full" onMouseDown={handleMouseDown} {...props}>
      {columns.length > 0 ? (
        <div className="mb-2 flex justify-end print:hidden">
          <div className="relative">
            <Button
              type="button"
              variant="outline"
              size="icon"
              className="h-8 w-8 text-muted-foreground"
              onClick={() => setSettingsOpen((current) => !current)}
              title="Настроить колонки"
              aria-label="Настроить колонки"
            >
              <Settings2 className="h-4 w-4" />
            </Button>
            {settingsOpen ? (
              <div className="absolute right-0 z-40 mt-2 w-72 rounded-md border border-border bg-card p-3 text-sm shadow-panel">
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
        <HeroTable.ScrollContainer>
          <HeroTable.Content ref={tableRef} aria-label={ariaLabel || 'Таблица'}>
            {children}
          </HeroTable.Content>
        </HeroTable.ScrollContainer>
      </HeroTable>
    </div>
  )
}

export function TableHeader({ className, children, ...props }: TableSectionProps) {
  const columns = flattenHeaderColumns(children)

  return (
    <HeroTable.Header className={className} {...props}>
      {Children.map(columns, (child, index) => {
        if (!isValidElement(child)) return child
        const element = child as ReactElement<TableHeadProps>
        return cloneElement(element, {
          isRowHeader: element.props.isRowHeader ?? index === 0,
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
  return (
    <HeroTable.Row
      id={id}
      className={className}
      onAction={
        onClick
          ? () =>
              onClick({
                target: null,
                currentTarget: null,
                preventDefault() {},
                stopPropagation() {},
              } as never)
          : undefined
      }
      {...props}
    >
      {children}
    </HeroTable.Row>
  )
}

export function TableHead({ className, children, isRowHeader, id, ...props }: TableHeadProps) {
  return (
    <HeroTable.Column id={id} isRowHeader={isRowHeader} className={cn('relative', className)} {...props}>
      {children}
      <span
        data-table-resize-handle
        className="absolute right-0 top-0 z-10 h-full w-2 cursor-col-resize select-none touch-none opacity-0 transition hover:bg-ring/30 hover:opacity-100"
        aria-hidden="true"
      />
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

import {
  memo,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type PointerEvent,
} from 'react'
import { Label, ProgressBar } from '@heroui/react'
import { Download, Printer, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { celebrateSuccess } from '@/lib/celebrate'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { useRequestGuard } from '@/hooks/useRequestGuard'
import { cn, getErrorMessage, rowMatchesQuery } from '@/lib/utils'
import { EmptyState, PageHeader, StatRow } from '@/components/layout/PageHeader'
import { Badge, StatusBadge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { BusyLabel, Shimmer } from '@/components/ui/shimmer'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import { Checkbox } from '@/components/ui/checkbox'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { TablePagination, usePagination } from '@/components/ui/pagination'
import { SelectNative } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

type DownloadItem = {
  document_id?: string
  order_name?: string
  full_name?: string
  simpl?: string
  status?: string
  status_summary?: string
  gtin?: string
  file_label?: string
  codes_count?: number
  from_history?: boolean
}

type DownloadState = {
  items?: DownloadItem[]
  printers?: string[]
  default_printer?: string
}

type PrintResult = {
  selection?: {
    total_record_count?: number
    selected_record_number?: number | null
  }
}

type Progress = {
  active: boolean
  label: string
  processed: number
  total: number
}

type Selection = {
  ids: string[]
  focus: string
}

const EMPTY_SELECTION: Selection = { ids: [], focus: '' }

/** Клик по этим элементам внутри строки не должен менять выбор */
const CONTROL_SELECTOR = 'input, button, a, label, [role="checkbox"], [data-table-resize-handle]'

/** Строка заказа — memo: выбор строки не перерисовывает остальные 50 строк страницы. */
const DownloadRow = memo(function DownloadRow({
  item,
  rowId,
  index,
  checked,
  focused,
  liveStatus,
  onActivate,
  onToggle,
}: {
  item: DownloadItem
  rowId: string
  index: number
  checked: boolean
  focused: boolean
  liveStatus?: string
  onActivate: (documentId: string, rowIndex: number) => void
  onToggle: (documentId: string) => void
}) {
  const documentId = item.document_id || ''
  return (
    <TableRow
      id={rowId}
      className={cn('select-none', (checked || focused) && 'row-selected')}
      onClick={() => onActivate(documentId, index)}
    >
      <TableCell onClick={(event) => event.stopPropagation()}>
        <Checkbox
          isSelected={checked}
          aria-label={`Выбрать заказ ${item.order_name || 'без названия'}`}
          onChange={() => onToggle(documentId)}
        />
      </TableCell>
      <TableCell textValue={item.order_name || 'Без названия'}>
        <div className="font-medium">{item.order_name || 'Без названия'}</div>
      </TableCell>
      <TableCell className="text-muted-foreground">
        <div className="max-w-[280px] truncate">{item.full_name || item.simpl || '—'}</div>
        {item.from_history ? (
          <Badge tone="neutral" className="mt-1">
            Из истории
          </Badge>
        ) : null}
      </TableCell>
      <TableCell>
        <StatusBadge status={liveStatus || item.status} />
        {item.status_summary && !liveStatus ? (
          <div className="mt-1 text-xs text-muted-foreground">{item.status_summary}</div>
        ) : null}
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">{item.gtin || '—'}</TableCell>
      <TableCell className="text-xs text-muted-foreground">
        <div className="max-w-[220px] truncate">{item.file_label || '—'}</div>
      </TableCell>
    </TableRow>
  )
})

export function DownloadPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [state, setState] = useCachedState<DownloadState>('download.state', {})
  const guard = useRequestGuard()
  const [search, setSearch] = useState('')
  const [selection, setSelection] = useState<Selection>(EMPTY_SELECTION)
  const [printer, setPrinter] = useState('')
  const [recordNumber, setRecordNumber] = useState('')
  const [autoDownload, setAutoDownload] = useState(false)
  const [progress, setProgress] = useState<Progress | null>(null)
  const [liveStatus, setLiveStatus] = useState<Record<string, string>>({})
  const lastClickedIndex = useRef(-1)
  // TableRow отдаёт onAction без исходного события — модификаторы снимаем до его срабатывания
  const clickMeta = useRef({ ctrl: false, shift: false, fromControl: false })
  // Строки текущей страницы для стабильного activateRow (иначе memo строк бесполезен)
  const pageRowsRef = useRef<DownloadItem[]>([])

  const load = useCallback(async () => {
    const fresh = guard()
    setLoading(true)
    try {
      const result = await apiCall<DownloadState>('get_download_state')
      if (!fresh()) return
      setState(result)

      const printers = result.printers ?? []
      setPrinter((prev) => {
        if (prev && printers.includes(prev)) return prev
        const fallback = result.default_printer || ''
        if (fallback && printers.includes(fallback)) return fallback
        return printers[0] || ''
      })

      const items = result.items ?? []
      setSelection((prev) => {
        const ids = prev.ids.filter((id) => items.some((item) => item.document_id === id))
        let focus = items.some((item) => item.document_id === prev.focus) ? prev.focus : ''
        if (!focus) focus = ids[0] || items[0]?.document_id || ''
        if (!ids.length && focus) ids.push(focus)
        return { ids, focus }
      })
    } catch (error) {
      if (!fresh()) return
      toast.error(getErrorMessage(error, 'Не удалось загрузить список загрузок'))
    } finally {
      if (fresh()) setLoading(false)
    }
  }, [guard, setState])

  useEffect(() => {
    void load()
  }, [load])

  const items = useMemo(() => state.items ?? [], [state.items])
  const printers = state.printers ?? []

  const debouncedSearch = useDebouncedValue(search)
  const rows = useMemo(() => items.filter((item) => rowMatchesQuery(item, debouncedSearch)), [items, debouncedSearch])
  const pager = usePagination(rows)
  pageRowsRef.current = pager.pageRows

  const targetIds = selection.ids.length ? selection.ids : [selection.focus].filter(Boolean)
  const printTargetId = selection.focus || selection.ids[0] || ''
  const isBusy = Boolean(busy)

  const runBusy = async (key: string, action: () => Promise<void>, successMessage?: string, celebrate = false) => {
    setBusy(key)
    try {
      await action()
      if (successMessage) toast.success(successMessage)
      if (celebrate) celebrateSuccess()
    } catch (error) {
      toast.error(getErrorMessage(error))
    } finally {
      setBusy(null)
    }
  }

  const toggleId = useCallback((documentId: string) => {
    setSelection((prev) => ({
      ids: prev.ids.includes(documentId) ? prev.ids.filter((id) => id !== documentId) : [...prev.ids, documentId],
      focus: documentId,
    }))
  }, [])

  const visibleIds = useMemo(() => rows.map((row) => row.document_id || '').filter(Boolean), [rows])
  const allVisibleSelected = visibleIds.length > 0 && visibleIds.every((id) => selection.ids.includes(id))

  const toggleAllVisible = (nextSelected: boolean) => {
    setSelection((prev) => ({
      ids: nextSelected
        ? Array.from(new Set([...prev.ids, ...visibleIds]))
        : prev.ids.filter((id) => !visibleIds.includes(id)),
      focus: nextSelected ? prev.focus || visibleIds[0] || '' : '',
    }))
    lastClickedIndex.current = -1
  }

  const rememberPointerModifiers = (event: PointerEvent<HTMLDivElement>) => {
    const target = event.target as Element | null
    clickMeta.current = {
      ctrl: event.ctrlKey || event.metaKey,
      shift: event.shiftKey,
      fromControl: Boolean(target?.closest(CONTROL_SELECTOR)),
    }
  }

  const rememberKeyModifiers = (event: KeyboardEvent<HTMLDivElement>) => {
    clickMeta.current = {
      ctrl: event.ctrlKey || event.metaKey,
      shift: event.shiftKey,
      fromControl: false,
    }
  }

  const activateRow = useCallback((documentId: string, rowIndex: number) => {
    if (!documentId) return
    const { ctrl, shift, fromControl } = clickMeta.current
    if (fromControl) return
    if (shift && lastClickedIndex.current >= 0) {
      const start = Math.min(lastClickedIndex.current, rowIndex)
      const end = Math.max(lastClickedIndex.current, rowIndex)
      // Индексы — в пределах текущей страницы таблицы
      const rangeIds = pageRowsRef.current.slice(start, end + 1).map((row) => row.document_id || '').filter(Boolean)
      setSelection((prev) => ({
        ids: Array.from(new Set([...prev.ids, ...rangeIds])),
        focus: documentId,
      }))
    } else if (ctrl) {
      toggleId(documentId)
    } else {
      setSelection({ ids: [documentId], focus: documentId })
    }
    lastClickedIndex.current = rowIndex
  }, [toggleId])

  const syncStatuses = () =>
    runBusy(
      'sync',
      async () => {
        await apiCall('sync_download_statuses', autoDownload)
        await load()
      },
      'Статусы загрузки обновлены.',
    )

  const downloadSelected = () =>
    runBusy(
      'download',
      async () => {
        if (!targetIds.length) throw new Error('Выберите хотя бы один заказ для скачивания.')

        setLiveStatus(Object.fromEntries(targetIds.map((id) => [id, 'Скачивается'])))
        setProgress({ active: true, processed: 0, total: targetIds.length, label: `Скачивается: 0/${targetIds.length}` })
        let successCount = 0
        const errors: string[] = []

        try {
          for (let index = 0; index < targetIds.length; index += 1) {
            const documentId = targetIds[index]
            const order = items.find((item) => item.document_id === documentId)
            try {
              await apiCall('manual_download_order', documentId)
              successCount += 1
              setLiveStatus((prev) => ({ ...prev, [documentId]: 'Скачан' }))
            } catch (error) {
              errors.push(`${order?.order_name || documentId}: ${getErrorMessage(error)}`)
              setLiveStatus((prev) => ({ ...prev, [documentId]: 'Ошибка скачивания' }))
            }
            setProgress({
              active: true,
              processed: index + 1,
              total: targetIds.length,
              label: `Скачивается: ${index + 1}/${targetIds.length}`,
            })
          }
        } finally {
          setProgress({
            active: false,
            processed: successCount,
            total: targetIds.length,
            label: errors.length
              ? `Скачано ${successCount}/${targetIds.length}, ошибок: ${errors.length}`
              : `Скачано ${successCount}/${targetIds.length}`,
          })
        }

        try {
          await load()
        } finally {
          setLiveStatus({})
        }
        if (errors.length) {
          throw new Error(`Скачано ${successCount}/${targetIds.length}. Первая ошибка: ${errors[0]}`)
        }
      },
      'Заказы скачаны.',
      true,
    )

  const printLabels = () =>
    runBusy(
      'print',
      async () => {
        if (!printTargetId) throw new Error('Выберите заказ для печати термоэтикеток.')
        if (!printer) throw new Error('Выберите принтер термоэтикеток.')
        setLiveStatus({ [printTargetId]: 'Печатается' })
        try {
          await apiCall<PrintResult>(
            'print_download_order',
            printTargetId,
            printer,
            recordNumber.trim() || null,
          )
        } finally {
          setLiveStatus({})
        }
      },
      'Печать термоэтикеток запущена.',
      true,
    )

  return (
    <div className="page-shell">
      <PageHeader
        title="Загрузка кодов"
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading || isBusy}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button size="sm" onClick={() => void syncStatuses()} disabled={isBusy}>
              <BusyLabel busy={busy === 'sync'} pending="Обновляются…">
                Обновить статусы
              </BusyLabel>
            </Button>
          </>
        }
      />

      <StatRow
        items={[
          { label: 'Всего заказов', value: items.length },
          { label: 'Выбрано', value: selection.ids.length },
          { label: 'Принтеры', value: printers.length },
          { label: 'По умолчанию', value: state.default_printer || '—' },
        ]}
      />

      <Card className="mb-3">
        <CardHeader>
          <CardTitle>Активные загрузки</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-2 sm:grid-cols-2">
            <div>
              <FieldLabel>Принтер термоэтикеток</FieldLabel>
              <SelectNative value={printer} onChange={(event) => setPrinter(event.target.value)}>
                <option value="">Выберите принтер</option>
                {printers.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </SelectNative>
            </div>
            <div>
              <FieldLabel>Номер этикетки</FieldLabel>
              <TextInput
                type="number"
                min={1}
                step={1}
                value={recordNumber}
                onChange={(event) => setRecordNumber(event.target.value)}
                placeholder="Весь файл"
              />
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <Button size="sm" variant="outline" onClick={() => void downloadSelected()} disabled={isBusy || targetIds.length === 0}>
              <Download className="h-3.5 w-3.5" />
              <BusyLabel busy={busy === 'download'} pending="Скачивается…">
                Скачать выбранное
              </BusyLabel>
            </Button>
            <Button size="sm" onClick={() => void printLabels()} disabled={isBusy || !printTargetId || !printer}>
              <Printer className="h-3.5 w-3.5" />
              <BusyLabel busy={busy === 'print'} pending="Печатается…">
                Печать 30×20
              </BusyLabel>
            </Button>
            <span title="Скачивать готовые заказы при обновлении статусов">
              <Checkbox isSelected={autoDownload} isDisabled={isBusy} onChange={setAutoDownload}>
                <span className="text-sm">Автоскачивание</span>
              </Checkbox>
            </span>
          </div>

          {progress ? (
            <ProgressBar
              aria-label="Прогресс скачивания"
              className="w-full"
              value={progress.total ? Math.round((progress.processed / progress.total) * 100) : 0}
            >
              <Label>
                {progress.active ? <Shimmer className="text-xs">{progress.label}</Shimmer> : progress.label}
              </Label>
              <ProgressBar.Output />
              <ProgressBar.Track>
                <ProgressBar.Fill className={cn(progress.active && 'animate-pulse')} />
              </ProgressBar.Track>
            </ProgressBar>
          ) : null}
        </CardContent>
      </Card>

      <Card className="cv-auto">
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>Заказы</CardTitle>
            <CardDescription>Ctrl / Shift — множественный выбор</CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <Checkbox isSelected={allVisibleSelected} isDisabled={rows.length === 0} onChange={toggleAllVisible}>
              <span className="text-sm">Выбрать все</span>
            </Checkbox>
            <div className="w-56">
              <TableSearch value={search} onChange={setSearch} />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {loading && items.length === 0 ? (
            <TableSkeleton rows={8} />
          ) : rows.length === 0 ? (
            <EmptyState>Данных пока нет.</EmptyState>
          ) : (
            <div
              className="max-h-[520px] overflow-auto"
              onPointerDownCapture={rememberPointerModifiers}
              onKeyDownCapture={rememberKeyModifiers}
            >
              <Table aria-label="Заказы к загрузке">
                <TableHeader>
                  <TableRow>
                    <TableHead isRowHeader={false}>Выбор</TableHead>
                    <TableHead isRowHeader>Заявка</TableHead>
                    <TableHead>Полное наименование</TableHead>
                    <TableHead>Статус</TableHead>
                    <TableHead>GTIN</TableHead>
                    <TableHead>Файлы</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pager.pageRows.map((item, index) => {
                    const documentId = item.document_id || ''
                    const rowId = documentId || `${item.order_name}-${index}`
                    return (
                      <DownloadRow
                        key={rowId}
                        rowId={rowId}
                        item={item}
                        index={index}
                        checked={selection.ids.includes(documentId)}
                        focused={selection.focus === documentId}
                        liveStatus={liveStatus[documentId]}
                        onActivate={activateRow}
                        onToggle={toggleId}
                      />
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          )}
          <TablePagination
            page={pager.page}
            pageCount={pager.pageCount}
            total={pager.total}
            onPageChange={pager.setPage}
          />
        </CardContent>
      </Card>
    </div>
  )
}

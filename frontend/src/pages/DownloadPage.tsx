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
import { Download, Printer, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { useRequestGuard } from '@/hooks/useRequestGuard'
import { withPageJob } from '@/lib/jobs'
import { subscribeDownloadProgress } from '@/lib/progress'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatRow } from '@/components/layout/PageHeader'
import { Badge, StatusBadge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { BusyLabel } from '@/components/ui/shimmer'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import { Checkbox } from '@/components/ui/checkbox'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { TablePagination, DEFAULT_PAGE_SIZE } from '@/components/ui/pagination'
import { SelectNative } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow, TableSelectCell } from '@/components/ui/table'
import { usePageRefreshHotkey } from '@/lib/hotkeys'
import { OPS_AUTO_DOWNLOAD_KEY, useOpsPrinter, usePersistedState } from '@/lib/persist'

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
  total?: number
  page?: number
  page_size?: number
  printers?: string[]
  default_printer?: string
}

type PrintResult = {
  selection?: {
    total_record_count?: number
    selected_record_number?: number | null
  }
}

type Selection = {
  ids: string[]
  focus: string
}

const EMPTY_SELECTION: Selection = { ids: [], focus: '' }

/** Клик по этим элементам внутри строки не должен менять выбор */
const CONTROL_SELECTOR = 'input, button, a, label, [role="checkbox"], [data-slot="table-column-resizer"]'

/** Строка заказа — memo: выбор строки не перерисовывает остальные 50 строк страницы. */
const DownloadRow = memo(function DownloadRow({
  item,
  rowId,
  index,
  checked,
  focused,
  liveStatus,
  liveProgress,
  onActivate,
  onToggle,
}: {
  item: DownloadItem
  rowId: string
  index: number
  checked: boolean
  focused: boolean
  liveStatus?: string
  liveProgress?: number
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
      <TableSelectCell>
        <Checkbox
          isSelected={checked}
          aria-label={`Выбрать заказ ${item.order_name || 'без названия'}`}
          onChange={() => onToggle(documentId)}
        />
      </TableSelectCell>
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
        <StatusBadge status={liveStatus || item.status} progress={liveProgress} />
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
  const [page, setPage] = useState(0)
  const [selection, setSelection] = useState<Selection>(EMPTY_SELECTION)
  const [printer, setPrinter] = useOpsPrinter()
  const [recordNumber, setRecordNumber] = useState('')
  const [autoDownload, setAutoDownload] = usePersistedState(OPS_AUTO_DOWNLOAD_KEY, false)
  const [liveStatus, setLiveStatus] = useState<Record<string, string>>({})
  const [liveProgress, setLiveProgress] = useState<Record<string, number>>({})
  const lastClickedIndex = useRef(-1)
  // TableRow отдаёт onAction без исходного события — модификаторы снимаем до его срабатывания
  const clickMeta = useRef({ ctrl: false, shift: false, fromControl: false })
  // Строки текущей страницы для стабильного activateRow (иначе memo строк бесполезен)
  const pageRowsRef = useRef<DownloadItem[]>([])
  const debouncedSearch = useDebouncedValue(search)

  useEffect(
    () =>
      subscribeDownloadProgress((ids, progress) => {
        setLiveProgress((prev) => {
          const next = { ...prev }
          for (const id of ids) next[id] = progress
          return next
        })
        setLiveStatus((prev) => {
          const next = { ...prev }
          for (const id of ids) next[id] = progress >= 1 ? 'Скачан' : 'Скачивается'
          return next
        })
      }),
    [],
  )

  const load = useCallback(async () => {
    const fresh = guard()
    setLoading(true)
    try {
      const result = await apiCall<DownloadState>('get_download_state', debouncedSearch, page, DEFAULT_PAGE_SIZE)
      if (!fresh()) return
      setState(result)
      if (typeof result.page === 'number' && result.page !== page) setPage(result.page)

      const printers = result.printers ?? []
      setPrinter((prev) => {
        if (prev && printers.includes(prev)) return prev
        const fallback = result.default_printer || ''
        if (fallback && printers.includes(fallback)) return fallback
        return printers[0] || ''
      })

      const items = result.items ?? []
      setSelection((prev) => {
        if (prev.ids.length || prev.focus) return prev
        const first = items[0]?.document_id || ''
        return first ? { ids: [first], focus: first } : prev
      })
    } catch (error) {
      if (!fresh()) return
      toast.error(getErrorMessage(error, 'Не удалось загрузить список загрузок'))
    } finally {
      if (fresh()) setLoading(false)
    }
  }, [debouncedSearch, guard, page, setPrinter, setState])

  usePageRefreshHotkey(load)

  useEffect(() => {
    void load()
  }, [load])

  const items = useMemo(() => state.items ?? [], [state.items])
  const printers = state.printers ?? []
  const total = state.total ?? items.length
  const pageCount = Math.max(1, Math.ceil(total / DEFAULT_PAGE_SIZE))
  pageRowsRef.current = items

  const targetIds = selection.ids.length ? selection.ids : [selection.focus].filter(Boolean)
  const printTargetId = selection.focus || selection.ids[0] || ''
  const isBusy = Boolean(busy)

  const runBusy = (
    key: string,
    action: () => Promise<void>,
    successMessage?: string,
    pendingMessage?: string,
  ) =>
    withPageJob(setBusy, key, action, {
      id: `download:${key}`,
      success: successMessage,
      pending: pendingMessage,
    })

  const toggleId = useCallback((documentId: string) => {
    setSelection((prev) => ({
      ids: prev.ids.includes(documentId) ? prev.ids.filter((id) => id !== documentId) : [...prev.ids, documentId],
      focus: documentId,
    }))
  }, [])

  const visibleIds = useMemo(() => items.map((row) => row.document_id || '').filter(Boolean), [items])
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
        setLiveProgress(Object.fromEntries(targetIds.map((id) => [id, 0])))
        await new Promise<void>((resolve) => {
          requestAnimationFrame(() => window.setTimeout(resolve, 0))
        })
        try {
          const result = await apiCall<{
            downloaded?: number
            failed?: { document_id?: string; error?: string }[]
          }>('manual_download_orders', targetIds)
          await load()
          const failed = result.failed ?? []
          if (failed.length) {
            const names = new Map(items.map((item) => [item.document_id, item.order_name]))
            const first = failed[0]
            const label = names.get(first.document_id) || first.document_id
            throw new Error(
              `Скачано ${result.downloaded ?? targetIds.length - failed.length}/${targetIds.length}. Первая ошибка: ${label}: ${first.error}`,
            )
          }
        } finally {
          setLiveStatus({})
          setLiveProgress({})
        }
      },
      'Заказы скачаны.',
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
      'Печать этикеток…',
    )

  return (
    <div className="page-shell">
      <PageHeader
        title="Загрузка кодов"
        refreshing={loading && items.length > 0}
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
          { label: 'Всего заказов', value: total },
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
        </CardContent>
      </Card>

      <Card className="cv-auto">
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>Заказы</CardTitle>
            <CardDescription>Ctrl / Shift — множественный выбор</CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <Checkbox isSelected={allVisibleSelected} isDisabled={items.length === 0} onChange={toggleAllVisible}>
              <span className="text-sm">Выбрать страницу</span>
            </Checkbox>
            <div className="w-56">
              <TableSearch
                value={search}
                onChange={(value) => {
                  setSearch(value)
                  setPage(0)
                }}
              />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {loading && items.length === 0 ? (
            <TableSkeleton rows={8} />
          ) : items.length === 0 ? (
            <EmptyState>{debouncedSearch ? 'Ничего не найдено.' : 'Данных пока нет.'}</EmptyState>
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
                  {items.map((item, index) => {
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
                        liveProgress={liveProgress[documentId]}
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
            page={page}
            pageCount={pageCount}
            total={total}
            onPageChange={setPage}
          />
        </CardContent>
      </Card>
    </div>
  )
}

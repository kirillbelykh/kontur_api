import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { ChevronDown, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { useRequestGuard } from '@/hooks/useRequestGuard'
import { withPageJob } from '@/lib/jobs'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatRow } from '@/components/layout/PageHeader'
import { StatusBadge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import { DatePickerField } from '@/components/ui/date-picker'
import { dissolveToDust, restoreDissolved } from '@/components/ui/dust-effect'
import { Checkbox } from '@/components/ui/checkbox'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { Shimmer, BusyLabel } from '@/components/ui/shimmer'
import { Skeleton, TableSkeleton } from '@/components/ui/skeleton'
import { SelectNative } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow, TableSelectCell } from '@/components/ui/table'

type AggregationItem = {
  document_id?: string
  aggregate_code?: string
  comment?: string
  status?: string
  status_label?: string
  created_at?: string
  created_at_label?: string
  product_group?: string
  includes_units_count?: number
  codes_check_errors_count?: number
}

type StatusOption = { value?: string; label?: string }

type AggregationState = {
  items?: AggregationItem[]
  status_options?: StatusOption[]
  cache_age_seconds?: number
  total_items?: number
}

const PAGE_SIZE = 30

/** Строка АК — memo: выбор/снятие строки не перерисовывает остальные строки страницы. */
const AkRow = memo(function AkRow({
  row,
  rowId,
  globalIndex,
  selected,
  arrived,
  liveStatus,
  onToggle,
}: {
  row: AggregationItem
  rowId: string
  globalIndex: number
  selected: boolean
  arrived: boolean
  liveStatus?: string
  onToggle: (documentId: string, index: number) => void
}) {
  const id = String(row.document_id || '')
  const errorsCount = row.codes_check_errors_count ?? 0
  return (
    <TableRow
      id={rowId}
      className={cn(selected && 'row-selected', arrived && 'order-arrive')}
      onClick={() => onToggle(id, globalIndex)}
    >
      <TableSelectCell>
        <Checkbox isSelected={selected} onChange={() => onToggle(id, globalIndex)} aria-label="Выбрать АК" />
      </TableSelectCell>
      <TableCell>
        <div className="font-medium">{row.aggregate_code || '—'}</div>
        <div className="text-xs text-muted-foreground">{row.comment || '—'}</div>
      </TableCell>
      <TableCell>
        <StatusBadge status={liveStatus || row.status_label || row.status} />
        {row.status === 'readyForSendAfterApproved' ? (
          <div className="mt-1 text-xs text-muted-foreground">Состав изменён</div>
        ) : null}
      </TableCell>
      <TableCell className="text-xs text-muted-foreground">{row.created_at_label || '—'}</TableCell>
      <TableCell className="text-right tabular-nums">{row.includes_units_count ?? 0}</TableCell>
      <TableCell className={cn('text-right tabular-nums', errorsCount > 0 && 'font-medium text-rose-600 dark:text-rose-400')}>
        {errorsCount}
      </TableCell>
    </TableRow>
  )
})

export function AggregationPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [state, setState] = useCachedState<AggregationState>('aggregation.state', {})
  const guard = useRequestGuard()

  const [createComment, setCreateComment] = useState('')
  const [createCount, setCreateCount] = useState('1')

  const [commentFilter, setCommentFilter] = useState('')
  const [refillToken, setRefillToken] = useState('')
  const [productionDate, setProductionDate] = useState('')
  const [expirationDate, setExpirationDate] = useState('')
  const [batchNumber, setBatchNumber] = useState('')
  const [documentTitle, setDocumentTitle] = useState('')
  const [allowDisaggregate, setAllowDisaggregate] = useState(false)
  const [refillOpen, setRefillOpen] = useState(false)

  const [statusFilter, setStatusFilter] = useState('')
  const [searchQuery, setSearchQuery] = useState('')
  const [currentPage, setCurrentPage] = useState(0)
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  // Создание АК: плейсхолдеры в таблице + подсветка «прилетевших» строк
  const [pendingCreate, setPendingCreate] = useState(0)
  const [arrivedIds, setArrivedIds] = useState<Set<string>>(new Set())
  const [liveStatus, setLiveStatus] = useState<Record<string, string>>({})
  const prevIdsRef = useRef<Set<string> | null>(null)
  // TableRow отдаёт onAction без исходного события — модификатор снимаем в capture-фазе до действия строки
  const shiftPressedRef = useRef(false)
  // Ссылки для стабильного toggleRow (иначе memo строк бесполезен)
  const lastClickedIndexRef = useRef(-1)
  const filteredItemsRef = useRef<AggregationItem[]>([])
  const tableScrollRef = useRef<HTMLDivElement>(null)
  const load = useCallback(async (force = false) => {
    const fresh = guard()
    setLoading(true)
    try {
      const result = await apiCall<AggregationState>('get_aggregation_state', force)
      if (!fresh()) return
      setState(result)
      const known = new Set((result.items || []).map((item) => String(item.document_id || '')))
      setSelectedIds((prev) => new Set([...prev].filter((id) => known.has(id))))
    } catch (error) {
      if (!fresh()) return
      toast.error(getErrorMessage(error, 'Не удалось загрузить агрегацию'))
    } finally {
      if (fresh()) setLoading(false)
    }
  }, [guard, setState])

  useEffect(() => {
    void load(false)
    void apiCall<{ production_date?: string; expiration_date?: string }>('get_default_date_window')
      .then((window) => {
        setProductionDate((prev) => prev || String(window.production_date || ''))
        setExpirationDate((prev) => prev || String(window.expiration_date || ''))
      })
      .catch(() => null)
  }, [load])

  const items = useMemo(() => state.items ?? [], [state.items])

  // После создания АК подсвечиваем новые строки анимацией «прилёта»
  useEffect(() => {
    const previous = prevIdsRef.current
    if (!previous) return
    const fresh = new Set(
      items
        .map((item) => String(item.document_id || ''))
        .filter((id) => id && !previous.has(id)),
    )
    if (fresh.size === 0) return
    prevIdsRef.current = null
    setArrivedIds(fresh)
    const timer = window.setTimeout(() => setArrivedIds(new Set()), 400)
    return () => window.clearTimeout(timer)
  }, [items])

  // Debounce 150 мс: АК бывает 200+ строк, фильтр не пересчитывается на каждый символ
  const debouncedQuery = useDebouncedValue(searchQuery)
  const filteredItems = useMemo(() => {
    const query = debouncedQuery.trim().toLowerCase()
    const status = statusFilter.trim()
    return items.filter((item) => {
      if (status && item.status !== status) return false
      if (!query) return true
      const haystack = [item.aggregate_code, item.comment, item.status_label, item.created_at_label, item.document_id]
        .map((value) => String(value || '').toLowerCase())
        .join(' ')
      return haystack.includes(query)
    })
  }, [items, debouncedQuery, statusFilter])

  filteredItemsRef.current = filteredItems

  const totalPages = Math.max(1, Math.ceil(filteredItems.length / PAGE_SIZE))
  const page = Math.min(Math.max(0, currentPage), totalPages - 1)
  const pageStart = page * PAGE_SIZE
  const pageEnd = Math.min(pageStart + PAGE_SIZE, filteredItems.length)
  const pageRows = filteredItems.slice(pageStart, pageEnd)

  const isBusy = Boolean(busy)
  const hasSelection = selectedIds.size > 0

  const runBusy = (
    key: string,
    action: () => Promise<void>,
    successMessage?: string,
    pendingMessage?: string,
  ) =>
    withPageJob(setBusy, key, action, {
      id: `aggregation:${key}`,
      success: successMessage,
      pending: pendingMessage,
    })

  const overlaySelected = (status: string) => {
    setLiveStatus(Object.fromEntries(selectedIdList().map((id) => [id, status])))
  }

  const selectedIdList = () => {
    const ids = [...selectedIds]
    if (!ids.length) throw new Error('Выберите хотя бы один АК.')
    return ids
  }

  const toggleRow = useCallback((documentId: string, index: number) => {
    if (!documentId) return
    const shiftKey = shiftPressedRef.current
    const lastClickedIndex = lastClickedIndexRef.current
    setSelectedIds((prev) => {
      const next = new Set(prev)
      const selected = next.has(documentId)
      if (shiftKey && lastClickedIndex >= 0) {
        const start = Math.min(lastClickedIndex, index)
        const end = Math.max(lastClickedIndex, index)
        const shouldSelect = !selected
        filteredItemsRef.current.slice(start, end + 1).forEach((row) => {
          const id = String(row.document_id || '')
          if (!id) return
          if (shouldSelect) next.add(id)
          else next.delete(id)
        })
      } else if (selected) {
        next.delete(documentId)
      } else {
        next.add(documentId)
      }
      return next
    })
    lastClickedIndexRef.current = index
  }, [])

  const selectVisible = () => {
    setSelectedIds(new Set(filteredItems.map((item) => String(item.document_id || '')).filter(Boolean)))
  }

  const selectByName = () => {
    const selectedRow = items.find((item) => selectedIds.has(String(item.document_id || '')))
    const fallbackRow = filteredItems[0]
    const targetName = String(selectedRow?.comment || fallbackRow?.comment || '').trim()
    if (!targetName) {
      toast.error('Сначала выберите АК или задайте поиск по наименованию.')
      return
    }
    setSelectedIds(
      new Set(
        items
          .filter((item) => String(item.comment || '').trim() === targetName)
          .map((item) => String(item.document_id || ''))
          .filter(Boolean),
      ),
    )
  }

  const clearSelection = () => {
    setSelectedIds(new Set())
    lastClickedIndexRef.current = -1
  }

  const createCodes = () =>
    runBusy(
      'create',
      async () => {
        // Оптимистичные строки-плейсхолдеры сразу, реальные АК подъедут после создания
        const count = Math.max(1, Number(createCount || 0))
        prevIdsRef.current = new Set(
          (state.items || []).map((item) => String(item.document_id || '')).filter(Boolean),
        )
        setPendingCreate(count)
        try {
          await apiCall('create_aggregation_codes', createComment, count)
          await load(true)
        } finally {
          setPendingCreate(0)
        }
      },
      'Агрегационные коды созданы.',
      'Создание АК…',
    )

  const refreshList = () =>
    runBusy(
      'refresh',
      async () => {
        await load(true)
      },
      'Список АК обновлён.',
    )

  const downloadSelected = () =>
    runBusy(
      'download-selected',
      async () => {
        overlaySelected('Скачивается')
        try {
          await apiCall('download_selected_aggregations', selectedIdList())
          await load(true)
        } finally {
          setLiveStatus({})
        }
      },
      'Выбранные АК скачаны.',
      'Скачивание АК…',
    )

  const approveSelected = () =>
    runBusy(
      'approve-selected',
      async () => {
        const ids = selectedIdList()
        const allow =
          allowDisaggregate ||
          window.confirm(
            'Если среди выбранных АК есть коды, уже привязанные к другому АК, разрешить расформирование старого АК?',
          )
        overlaySelected('Проводится')
        try {
          await apiCall('approve_selected_aggregations', ids, allow)
          await load(true)
        } finally {
          setLiveStatus({})
        }
      },
      'Проведение выбранных АК завершено.',
      'Проведение АК…',
    )

  const archiveSelected = () =>
    runBusy(
      'archive-selected',
      async () => {
        const ids = selectedIdList()
        const selected = new Set(ids)
        // Строки текущей страницы идут в DOM в том же порядке, что и pageRows
        // (плюс плейсхолдеры создающихся АК сверху).
        const domRows = Array.from(tableScrollRef.current?.querySelectorAll('tbody tr') ?? [])
        const offset = page === 0 && pendingCreate > 0 ? Math.min(pendingCreate, 20) : 0
        const targets = pageRows
          .map((row, index) =>
            selected.has(String(row.document_id || ''))
              ? (domRows[offset + index] as HTMLElement | undefined)
              : undefined,
          )
          .filter((el): el is HTMLElement => Boolean(el))

        const dust = dissolveToDust(targets)
        try {
          await apiCall('archive_selected_aggregations', ids)
        } catch (error) {
          targets.forEach(restoreDissolved)
          throw error
        }
        await dust
        setSelectedIds(new Set())
        await load(true)
        // Если какая-то строка не ушла в архив и осталась в данных — вернуть ей вид
        targets.forEach(restoreDissolved)
      },
      'Выбранные АК отправлены в архив.',
    )

  const introduceSelected = () =>
    runBusy(
      'intro-selected',
      async () => {
        overlaySelected('Вводится в оборот')
        try {
          await apiCall(
            'introduce_selected_aggregations',
            selectedIdList(),
            productionDate,
            expirationDate,
            batchNumber,
            documentTitle,
          )
          await load(true)
        } finally {
          setLiveStatus({})
        }
      },
      'Ввод в оборот по выбранным АК завершён.',
      'Ввод в оборот…',
    )

  const refill = () =>
    runBusy(
      'refill',
      async () => {
        await apiCall('refill_aggregations', commentFilter, refillToken)
        await load(true)
      },
      'Повторное наполнение АК завершено.',
    )

  const cacheAge = Number(state.cache_age_seconds || 0)
  const statusOptions = state.status_options ?? []

  return (
    <div className="page-shell page-snappy">
      <PageHeader
        title="Коды агрегации"
        actions={
          <Button variant="outline" size="sm" onClick={() => void refreshList()} disabled={loading || isBusy}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <StatRow
        items={[
          { label: 'Всего АК', value: state.total_items ?? items.length },
          { label: 'Найдено', value: filteredItems.length },
          { label: 'Выбрано', value: selectedIds.size },
          { label: 'Возраст кэша', value: cacheAge > 0 ? `${cacheAge} с` : '—' },
        ]}
      />

      <div className="grid gap-3 xl:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Создание АК</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div>
              <FieldLabel>Название</FieldLabel>
              <TextInput
                value={createComment}
                onChange={(e) => setCreateComment(e.target.value)}
                placeholder="Название"
              />
            </div>
            <div>
              <FieldLabel>Количество агрегатов</FieldLabel>
              <TextInput
                type="number"
                min={1}
                step={1}
                value={createCount}
                onChange={(e) => setCreateCount(e.target.value)}
              />
            </div>
            <Button size="sm" onClick={() => void createCodes()} disabled={isBusy}>
              <BusyLabel busy={busy === 'create'} pending="Создаётся…">
                Создать
              </BusyLabel>
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Проведение и ввод в оборот</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-2">
              <div>
                <FieldLabel>Дата производства</FieldLabel>
                <DatePickerField value={productionDate} onChange={setProductionDate} />
              </div>
              <div>
                <FieldLabel>Срок годности</FieldLabel>
                <DatePickerField value={expirationDate} onChange={setExpirationDate} />
              </div>
              <div>
                <FieldLabel>Номер партии</FieldLabel>
                <TextInput value={batchNumber} onChange={(e) => setBatchNumber(e.target.value)} />
              </div>
              <div>
                <FieldLabel>Название документа</FieldLabel>
                <TextInput
                  value={documentTitle}
                  onChange={(e) => setDocumentTitle(e.target.value)}
                  placeholder="Автоназвание"
                />
              </div>
            </div>

            <Checkbox isSelected={allowDisaggregate} onChange={setAllowDisaggregate}>
              <span className="text-sm">Разрешить расформирование чужих АК</span>
            </Checkbox>

            <div className="flex flex-wrap gap-2">
              <Button size="sm" variant="outline" onClick={() => void downloadSelected()} disabled={isBusy || !hasSelection}>
                <BusyLabel busy={busy === 'download-selected'} pending="Скачивается…">
                  Скачать
                </BusyLabel>
              </Button>
              <Button size="sm" variant="outline" onClick={() => void approveSelected()} disabled={isBusy || !hasSelection}>
                <BusyLabel busy={busy === 'approve-selected'} pending="Проводится…">
                  Провести
                </BusyLabel>
              </Button>
              <Button size="sm" variant="outline" onClick={() => void archiveSelected()} disabled={isBusy || !hasSelection}>
                <BusyLabel busy={busy === 'archive-selected'} pending="В архив…">
                  В архив
                </BusyLabel>
              </Button>
              <Button size="sm" variant="outline" onClick={() => void introduceSelected()} disabled={isBusy || !hasSelection}>
                <BusyLabel busy={busy === 'intro-selected'} pending="Вводится в оборот…">
                  Ввести в оборот
                </BusyLabel>
              </Button>
              <Button
                size="sm"
                variant={refillOpen ? 'secondary' : 'ghost'}
                onClick={() => setRefillOpen((open) => !open)}
              >
                <ChevronDown className={cn('h-3.5 w-3.5 transition-transform duration-75', refillOpen && 'rotate-180')} />
                Повторное наполнение
              </Button>
            </div>

            <AnimatePresence initial={false}>
              {refillOpen ? (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  transition={{ duration: 0.12, ease: [0.22, 1, 0.36, 1] }}
                  className="overflow-hidden"
                >
                  <div className="space-y-3 border-t border-border pt-3">
                    <div>
                      <FieldLabel>Название АК</FieldLabel>
                      <TextInput
                        value={commentFilter}
                        onChange={(e) => setCommentFilter(e.target.value)}
                        placeholder="Название"
                      />
                    </div>
                    <div>
                      <FieldLabel>TSD токен</FieldLabel>
                      <TextInput value={refillToken} onChange={(e) => setRefillToken(e.target.value)} />
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Только для АК, не зарегистрированных в ГИС МТ.
                    </p>
                    <Button size="sm" onClick={() => void refill()} disabled={isBusy}>
                      <BusyLabel busy={busy === 'refill'} pending="Наполняется…">
                        Наполнить
                      </BusyLabel>
                    </Button>
                  </div>
                </motion.div>
              ) : null}
            </AnimatePresence>
          </CardContent>
        </Card>
      </div>

      <Card className="cv-auto mt-3">
        <CardHeader className="flex-row items-center justify-between gap-3 space-y-0">
          <CardTitle>Список АК</CardTitle>
          <div className="flex flex-wrap gap-1.5">
            <Button size="sm" variant="outline" onClick={selectVisible} disabled={filteredItems.length === 0}>
              Выбрать найденные
            </Button>
            <Button size="sm" variant="outline" onClick={selectByName} disabled={items.length === 0}>
              Выбрать одноимённые
            </Button>
            <Button size="sm" variant="ghost" onClick={clearSelection} disabled={!hasSelection}>
              Снять выделение
            </Button>
            <Button size="sm" variant="outline" onClick={() => void refreshList()} disabled={loading || isBusy}>
              Обновить
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-2">
          <div className="grid gap-2 sm:grid-cols-[minmax(0,220px)_minmax(0,1fr)]">
            <div>
              <FieldLabel>Статус</FieldLabel>
              <SelectNative
                value={statusFilter}
                onChange={(e) => {
                  setStatusFilter(e.target.value)
                  setCurrentPage(0)
                  lastClickedIndexRef.current = -1
                }}
              >
                {statusOptions.length === 0 ? <option value="">Все статусы</option> : null}
                {statusOptions.map((option) => (
                  <option key={String(option.value ?? '')} value={String(option.value ?? '')}>
                    {option.label || option.value || 'Все статусы'}
                  </option>
                ))}
              </SelectNative>
            </div>
            <div>
              <FieldLabel>Поиск</FieldLabel>
              <TableSearch
                value={searchQuery}
                onChange={(value) => {
                  setSearchQuery(value)
                  setCurrentPage(0)
                  lastClickedIndexRef.current = -1
                }}
              />
            </div>
          </div>

          {loading && items.length === 0 && pendingCreate === 0 ? (
            <TableSkeleton rows={8} />
          ) : filteredItems.length === 0 && pendingCreate === 0 ? (
            <EmptyState>Ничего не найдено</EmptyState>
          ) : (
            <>
              <div
                ref={tableScrollRef}
                className="max-h-[520px] overflow-auto"
                onMouseDownCapture={(event) => {
                  shiftPressedRef.current = event.shiftKey
                }}
                onKeyDownCapture={(event) => {
                  shiftPressedRef.current = event.shiftKey
                }}
              >
                <Table aria-label="Коды агрегации">
                  <TableHeader>
                    <TableRow>
                      <TableHead isRowHeader={false}>Выбор</TableHead>
                      <TableHead isRowHeader>АК</TableHead>
                      <TableHead>Статус</TableHead>
                      <TableHead>Создан</TableHead>
                      <TableHead className="text-right">КМ</TableHead>
                      <TableHead className="text-right">Ошибки</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {/* Плейсхолдеры создающихся АК: сразу видно строки, потом их заменяют реальные коды */}
                    {page === 0 && pendingCreate > 0
                      ? Array.from({ length: Math.min(pendingCreate, 20) }).map((_, index) => (
                          <TableRow key={`pending-${index}`} id={`pending-${index}`} className="order-arrive">
                            <TableCell>
                              <Skeleton className="h-4 w-4 rounded-sm" />
                            </TableCell>
                            <TableCell>
                              <div className="space-y-1.5">
                                <Skeleton className="h-4 w-56" />
                                <Skeleton className="h-3 w-36" />
                              </div>
                            </TableCell>
                            <TableCell>
                              <Shimmer className="text-xs">Создаётся…</Shimmer>
                            </TableCell>
                            <TableCell>
                              <Skeleton className="h-3 w-20" />
                            </TableCell>
                            <TableCell>
                              <Skeleton className="ml-auto h-3 w-8" />
                            </TableCell>
                            <TableCell>
                              <Skeleton className="ml-auto h-3 w-8" />
                            </TableCell>
                          </TableRow>
                        ))
                      : null}
                    {pageRows.map((row, localIndex) => {
                      const id = String(row.document_id || '')
                      const globalIndex = pageStart + localIndex
                      const rowId = id || `${row.aggregate_code}-${globalIndex}`
                      return (
                        <AkRow
                          key={rowId}
                          rowId={rowId}
                          row={row}
                          globalIndex={globalIndex}
                          selected={selectedIds.has(id)}
                          arrived={arrivedIds.has(id)}
                          liveStatus={liveStatus[id]}
                          onToggle={toggleRow}
                        />
                      )
                    })}
                  </TableBody>
                </Table>
              </div>

              <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                <div>
                  Показано {pageStart + 1}-{pageEnd} из {filteredItems.length}
                </div>
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="outline" onClick={() => setCurrentPage(page - 1)} disabled={page <= 0}>
                    Назад
                  </Button>
                  <span>
                    Страница {page + 1} из {totalPages}
                  </span>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setCurrentPage(page + 1)}
                    disabled={page >= totalPages - 1}
                  >
                    Вперёд
                  </Button>
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

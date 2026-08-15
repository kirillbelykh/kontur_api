import { memo, useCallback, useEffect, useMemo, useState } from 'react'
import { PenLine, PlayCircle, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { useRequestGuard } from '@/hooks/useRequestGuard'
import { withPageJob } from '@/lib/jobs'
import { usePageRefreshHotkey } from '@/lib/hotkeys'
import { useOpsDates } from '@/lib/persist'
import { cn, getErrorMessage, rowMatchesQuery } from '@/lib/utils'
import { EmptyState, PageHeader, StatRow } from '@/components/layout/PageHeader'
import { StatusBadge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { BusyLabel } from '@/components/ui/shimmer'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import { Checkbox } from '@/components/ui/checkbox'
import { DatePickerField } from '@/components/ui/date-picker'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { TablePagination, usePagination } from '@/components/ui/pagination'
import { SelectNative } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow, TableSelectCell } from '@/components/ui/table'

type TsdItem = {
  document_id?: string
  order_name?: string
  full_name?: string
  simpl?: string
  status?: string
  status_summary?: string
  tsd_status?: string
  tsd_created?: boolean
  tsd_intro_number?: string
  gtin?: string
  can_tsd?: boolean
}

type TsdState = {
  items?: TsdItem[]
  live?: boolean
  live_updated_at?: string | number | null
}

type TsdRunResult = {
  results?: Array<{ document_id?: string }>
  errors?: Array<{ document_id?: string; error?: string }>
  total?: number
}

type SignResult = {
  state?: TsdState
}

type TsdForm = {
  intro_number: string
  production_date: string
  expiration_date: string
  batch_number: string
}

/** Строка заказа ТСД — memo: выбор строки не перерисовывает остальные строки. */
const TsdRow = memo(function TsdRow({
  item,
  rowId,
  selected,
  liveTsdStatus,
  onToggle,
}: {
  item: TsdItem
  rowId: string
  selected: boolean
  liveTsdStatus?: string
  onToggle: (documentId: string) => void
}) {
  const documentId = item.document_id || ''
  return (
    <TableRow
      id={rowId}
      className={cn(selected && 'row-selected')}
      onClick={() => onToggle(documentId)}
    >
      <TableSelectCell>
        <Checkbox
          isSelected={selected}
          aria-label={`Выбрать заказ ${item.order_name || documentId}`}
          onChange={() => onToggle(documentId)}
        />
      </TableSelectCell>
      <TableCell textValue={item.order_name || documentId}>
        <div className="font-medium">{item.order_name || documentId || 'Без названия'}</div>
      </TableCell>
      <TableCell className="text-muted-foreground">
        <div className="max-w-[280px] truncate">{item.full_name || item.simpl || '—'}</div>
      </TableCell>
      <TableCell>
        <StatusBadge status={item.status} />
        {item.status_summary ? (
          <div className="mt-1 text-xs text-muted-foreground">{item.status_summary}</div>
        ) : null}
      </TableCell>
      <TableCell>
        <StatusBadge status={liveTsdStatus || item.tsd_status} />
        {item.tsd_intro_number && !liveTsdStatus ? (
          <div className="mt-1 font-mono text-xs text-muted-foreground">{item.tsd_intro_number}</div>
        ) : null}
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">{item.gtin || '—'}</TableCell>
    </TableRow>
  )
})

export function TsdPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [items, setItems] = useCachedState<TsdItem[]>('tsd.items', [])
  const guard = useRequestGuard()
  const [live, setLive] = useState(false)
  const [dates, setDates] = useOpsDates()
  const [introNumber, setIntroNumber] = useState('')
  const form: TsdForm = {
    intro_number: introNumber,
    production_date: dates.production,
    expiration_date: dates.expiration,
    batch_number: dates.batch,
  }
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [selectedIds, setSelectedIds] = useState<string[]>([])
  const [introNumberError, setIntroNumberError] = useState(false)
  const [liveTsdStatus, setLiveTsdStatus] = useState<Record<string, string>>({})

  const setField = <K extends keyof TsdForm>(key: K, value: TsdForm[K]) => {
    const text = String(value)
    if (key === 'intro_number') {
      setIntroNumber(text)
      return
    }
    if (key === 'production_date') setDates((prev) => ({ ...prev, production: text }))
    else if (key === 'expiration_date') setDates((prev) => ({ ...prev, expiration: text }))
    else setDates((prev) => ({ ...prev, batch: text }))
  }

  const applyItems = useCallback((next: TsdItem[]) => {
    setItems(next)
    setSelectedIds((prev) => prev.filter((id) => next.some((item) => item.document_id === id)))
  }, [setItems])

  const toggleId = useCallback((documentId: string) => {
    if (!documentId) return
    setSelectedIds((prev) =>
      prev.includes(documentId) ? prev.filter((id) => id !== documentId) : [...prev, documentId],
    )
  }, [])

  const load = useCallback(async (useLive = false) => {
    const fresh = guard()
    setLoading(true)
    try {
      const result = await apiCall<TsdState>('get_tsd_state', useLive)
      if (!fresh()) return
      applyItems(result.items ?? [])
      setLive(Boolean(result.live || useLive))
    } catch (error) {
      if (!fresh()) return
      toast.error(getErrorMessage(error, 'Не удалось загрузить задания ТСД'))
    } finally {
      if (fresh()) setLoading(false)
    }
  }, [applyItems, guard])

  const refreshPage = useCallback(() => {
    void load(false)
  }, [load])
  usePageRefreshHotkey(refreshPage)

  useEffect(() => {
    void load(false)
  }, [load])

  useEffect(() => {
    void apiCall<{ production_date?: string; expiration_date?: string }>('get_default_date_window')
      .then((window) => {
        setDates((prev) => ({
          ...prev,
          production: prev.production || String(window.production_date || ''),
          expiration: prev.expiration || String(window.expiration_date || ''),
        }))
      })
      .catch(() => null)
  }, [setDates])

  const statusOptions = useMemo(
    () => Array.from(new Set(items.map((item) => String(item.tsd_status || '').trim()).filter(Boolean))),
    [items],
  )

  const debouncedSearch = useDebouncedValue(search)
  const rows = useMemo(
    () =>
      items.filter((item) => {
        if (!rowMatchesQuery(item, debouncedSearch)) return false
        if (!statusFilter) return true
        return String(item.tsd_status || '').trim() === statusFilter
      }),
    [items, debouncedSearch, statusFilter],
  )

  const readyCount = useMemo(() => items.filter((item) => item.can_tsd).length, [items])
  const isBusy = Boolean(busy)
  const pager = usePagination(rows)

  // Подписать и ввести в оборот можно только для заказа в статусе «Наполнен на ТСД»
  const isFilledOnTsd = (item?: TsdItem) => {
    const haystack = `${item?.tsd_status || ''} ${item?.status || ''}`.toLowerCase()
    return haystack.includes('наполн')
  }
  const selectedItem = selectedIds.length === 1 ? items.find((item) => item.document_id === selectedIds[0]) : undefined
  const canSign = Boolean(selectedItem && isFilledOnTsd(selectedItem))

  const runBusy = (
    key: string,
    action: () => Promise<void>,
    successMessage?: string,
    pendingMessage?: string,
  ) =>
    withPageJob(setBusy, key, action, {
      id: `tsd:${key}`,
      success: successMessage,
      pending: pendingMessage,
    })

  const flagIntroNumberError = () => {
    setIntroNumberError(false)
    requestAnimationFrame(() => setIntroNumberError(true))
    window.setTimeout(() => setIntroNumberError(false), 3000)
  }

  const createTasks = () =>
    runBusy(
      'create',
      async () => {
        if (!selectedIds.length) throw new Error('Выберите хотя бы один заказ для задания на ТСД.')
        if (!form.intro_number.trim()) {
          flagIntroNumberError()
          throw new Error('Укажите номер ввода в оборот.')
        }

        setLiveTsdStatus(Object.fromEntries(selectedIds.map((id) => [id, 'Отправляется на ТСД'])))
        try {
          const result = await apiCall<TsdRunResult>(
            'create_tsd_tasks',
            selectedIds,
            form.intro_number,
            form.production_date,
            form.expiration_date,
            form.batch_number,
          )

          const failedIds = new Set((result.errors || []).map((entry) => entry.document_id))
          setSelectedIds((prev) => prev.filter((id) => failedIds.has(id)))
          await load(false)

          if (result.errors?.length) {
            const firstError = result.errors[0]
            throw new Error(
              `Создано ${result.results?.length || 0}/${selectedIds.length}. ${firstError?.error || 'Подробности в логе.'}`,
            )
          }
        } finally {
          setLiveTsdStatus({})
        }
      },
      'Задания на ТСД созданы.',
      'Отправка на ТСД…',
    )

  const signIntroduction = () => {
    if (selectedIds.length !== 1) {
      toast.error('Выберите один заказ для подписи.')
      return
    }
    if (!canSign) {
      toast.error('Подписать можно только заказ в статусе «Наполнен на ТСД».')
      return
    }
    if (!window.confirm('Подписать и ввести в оборот?')) return
    return runBusy(
      'sign',
      async () => {
        setLiveTsdStatus({ [selectedIds[0]]: 'Подписывается' })
        try {
          const result = await apiCall<SignResult>('sign_tsd_introduction', selectedIds[0])
          if (result?.state?.items) {
            setItems(result.state.items)
            setLive(true)
          } else {
            await load(true)
          }
        } finally {
          setLiveTsdStatus({})
        }
      },
      'Документ подписан и отправлен в ГИС МТ.',
      'Подпись…',
    )
  }

  return (
    <div className="page-shell">
      <PageHeader
        title="Задание на ТСД"
        refreshing={loading && items.length > 0}
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void load(false)} disabled={loading || isBusy}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button variant="outline" size="sm" onClick={() => void load(true)} disabled={loading || isBusy}>
              Live-статусы
            </Button>
          </>
        }
      />

      <StatRow
        items={[
          { label: 'Документов', value: items.length },
          { label: 'Готовы к ТСД', value: readyCount },
          { label: 'Выбрано', value: selectedIds.length },
          { label: 'Режим', value: live ? 'Live' : 'Кэш' },
        ]}
      />

      <Card className="mb-3">
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>Параметры задания</CardTitle>
          </div>
          <div className="flex flex-wrap gap-1.5">
            <Button
              size="sm"
              variant="success"
              onClick={() => void createTasks()}
              disabled={isBusy || selectedIds.length === 0 || !form.intro_number.trim()}
            >
              <PlayCircle className="h-3.5 w-3.5" />
              <BusyLabel busy={busy === 'create'} pending="Отправляется на ТСД…">
                Создать задания{selectedIds.length > 1 ? ` (${selectedIds.length})` : ''}
              </BusyLabel>
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => void signIntroduction()}
              disabled={isBusy || !canSign}
              title={!canSign ? 'Доступно только для статуса «Наполнен на ТСД»' : undefined}
            >
              <PenLine className="h-3.5 w-3.5" />
              <BusyLabel busy={busy === 'sign'} pending="Подписывается…">
                Подписать и ввести в оборот
              </BusyLabel>
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
            <div>
              <FieldLabel>Ввод в оборот №</FieldLabel>
              <TextInput
                value={form.intro_number}
                className={cn('t-input', introNumberError && 'is-error is-shaking')}
                onChange={(event) => {
                  setField('intro_number', event.target.value)
                  setIntroNumberError(false)
                }}
              />
            </div>
            <div>
              <FieldLabel>Дата производства</FieldLabel>
              <DatePickerField
                value={form.production_date}
                onChange={(value) => setField('production_date', value)}
              />
            </div>
            <div>
              <FieldLabel>Срок годности</FieldLabel>
              <DatePickerField
                value={form.expiration_date}
                onChange={(value) => setField('expiration_date', value)}
              />
            </div>
            <div>
              <FieldLabel>Номер партии</FieldLabel>
              <TextInput
                value={form.batch_number}
                onChange={(event) => setField('batch_number', event.target.value)}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="cv-auto">
        <CardHeader className="flex-row items-center justify-between gap-3 space-y-0">
          <CardTitle>Заказы</CardTitle>
          <div className="flex flex-wrap items-center gap-2">
            <div className="w-44">
              <SelectNative
                value={statusFilter}
                aria-label="Фильтр по статусу ТСД"
                onChange={(event) => setStatusFilter(event.target.value)}
              >
                <option value="">Все статусы</option>
                {statusOptions.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </SelectNative>
            </div>
            <div className="w-52">
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
            <div className="max-h-[520px] overflow-auto">
              <Table aria-label="Заказы для задания на ТСД">
                <TableHeader>
                  <TableRow>
                    <TableHead isRowHeader={false}>Выбор</TableHead>
                    <TableHead isRowHeader>Заявка</TableHead>
                    <TableHead>Полное наименование</TableHead>
                    <TableHead>Статус ЧЗ</TableHead>
                    <TableHead>На ТСД</TableHead>
                    <TableHead>GTIN</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pager.pageRows.map((item, index) => {
                    const documentId = item.document_id || ''
                    const rowId = documentId || `${item.order_name}-${index}`
                    return (
                      <TsdRow
                        key={rowId}
                        rowId={rowId}
                        item={item}
                        selected={Boolean(documentId) && selectedIds.includes(documentId)}
                        liveTsdStatus={liveTsdStatus[documentId]}
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

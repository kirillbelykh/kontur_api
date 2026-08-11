import { memo, useCallback, useEffect, useMemo, useState } from 'react'
import { PlayCircle, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { useRequestGuard } from '@/hooks/useRequestGuard'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatRow } from '@/components/layout/PageHeader'
import { StatusBadge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useDebouncedValue } from '@/hooks/useDebouncedValue'
import { Checkbox } from '@/components/ui/checkbox'
import { DatePickerField } from '@/components/ui/date-picker'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { TablePagination, usePagination } from '@/components/ui/pagination'
import { SelectNative } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

type IntroItem = {
  document_id?: string
  order_name?: string
  full_name?: string
  simpl?: string
  status?: string
  status_summary?: string
  intro_status?: string
  gtin?: string
  codes_count?: number
  can_intro?: boolean
}

type IntroState = {
  items?: IntroItem[]
}

type IntroResult = {
  success?: boolean
  results?: Array<{ document_id?: string }>
  errors?: Array<{ document_id?: string; error?: string }>
  state?: IntroState
}

/** Строка заявки — memo: выбор строки не перерисовывает остальные строки. */
const IntroRow = memo(function IntroRow({
  item,
  rowId,
  checked,
  onToggle,
}: {
  item: IntroItem
  rowId: string
  checked: boolean
  onToggle: (documentId: string) => void
}) {
  const documentId = item.document_id || ''
  return (
    <TableRow
      id={rowId}
      className={cn(checked && 'row-selected')}
      onClick={() => onToggle(documentId)}
    >
      <TableCell>
        <Checkbox
          isSelected={checked}
          aria-label={`Выбрать заказ ${item.order_name || documentId}`}
          onChange={() => onToggle(documentId)}
        />
      </TableCell>
      <TableCell>
        <div className="font-medium">{item.order_name || item.document_id || 'Без названия'}</div>
        <div className="font-mono text-xs text-muted-foreground">{item.document_id || '—'}</div>
      </TableCell>
      <TableCell className="text-muted-foreground">{item.full_name || item.simpl || '—'}</TableCell>
      <TableCell>
        <StatusBadge status={item.status} />
      </TableCell>
      <TableCell className="font-mono text-xs text-muted-foreground">{item.gtin || '—'}</TableCell>
    </TableRow>
  )
})

export function IntroPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [items, setItems] = useCachedState<IntroItem[]>('intro.items', [])
  const guard = useRequestGuard()
  const [selectedIds, setSelectedIds] = useState<string[]>([])
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [productionDate, setProductionDate] = useState('')
  const [expirationDate, setExpirationDate] = useState('')
  const [batchNumber, setBatchNumber] = useState('')
  const [batchError, setBatchError] = useState(false)

  const applyItems = useCallback(
    (next: IntroItem[]) => {
      setItems(next)
      setSelectedIds((prev) => prev.filter((id) => next.some((item) => item.document_id === id)))
    },
    [setItems],
  )

  const load = useCallback(async () => {
    const fresh = guard()
    setLoading(true)
    try {
      const result = await apiCall<IntroState>('get_intro_state')
      if (!fresh()) return
      applyItems(result.items ?? [])
    } catch (error) {
      if (!fresh()) return
      toast.error(getErrorMessage(error, 'Не удалось загрузить ввод в оборот'))
    } finally {
      if (fresh()) setLoading(false)
    }
  }, [applyItems, guard])

  useEffect(() => {
    void load()
  }, [load])

  // Автозаполнение дат (01-03-2026 / 01-03-2031 из бэкенда)
  useEffect(() => {
    void apiCall<{ production_date?: string; expiration_date?: string }>('get_default_date_window')
      .then((window) => {
        setProductionDate((prev) => prev || String(window.production_date || ''))
        setExpirationDate((prev) => prev || String(window.expiration_date || ''))
      })
      .catch(() => null)
  }, [])

  const statusOptions = useMemo(
    () => [...new Set(items.map((item) => String(item.status || '').trim()).filter(Boolean))],
    [items],
  )

  const debouncedSearch = useDebouncedValue(search)
  const filteredItems = useMemo(() => {
    const query = debouncedSearch.trim().toLowerCase()
    return items.filter((item) => {
      if (statusFilter && String(item.status || '').trim() !== statusFilter) return false
      if (!query) return true
      const haystack = [item.order_name, item.full_name, item.simpl, item.gtin, item.document_id, item.status]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
  }, [items, debouncedSearch, statusFilter])

  const toggleId = useCallback((documentId: string) => {
    if (!documentId) return
    setSelectedIds((prev) =>
      prev.includes(documentId) ? prev.filter((id) => id !== documentId) : [...prev, documentId],
    )
  }, [])

  const runBusy = async (key: string, action: () => Promise<void>, successMessage: string) => {
    setBusy(key)
    try {
      await action()
      toast.success(successMessage)
    } catch (error) {
      toast.error(getErrorMessage(error))
    } finally {
      setBusy(null)
    }
  }

  const refresh = () =>
    runBusy('refresh', async () => {
      await load()
    }, 'Список заказов обновлён.')

  const flagBatchError = () => {
    setBatchError(false)
    // Перезапуск shake-анимации: класс снимается и вешается заново
    requestAnimationFrame(() => setBatchError(true))
    window.setTimeout(() => setBatchError(false), 3000)
  }

  const runIntroduction = () =>
    runBusy(
      'run',
      async () => {
        if (!selectedIds.length) throw new Error('Выберите хотя бы один заказ для ввода в оборот.')
        if (!batchNumber.trim()) {
          flagBatchError()
          throw new Error('Укажите номер партии.')
        }
        const result = await apiCall<IntroResult>(
          'introduce_orders',
          selectedIds,
          productionDate,
          expirationDate,
          batchNumber,
        )
        if (result.state?.items) applyItems(result.state.items)
        else await load()
        const failed = result.errors ?? []
        if (failed.length) {
          throw new Error(failed[0]?.error || 'Не удалось ввести заказ в оборот.')
        }
      },
      'Ввод в оборот завершён.',
    )

  const isBusy = Boolean(busy) || loading
  const pager = usePagination(filteredItems)

  return (
    <div className="page-shell">
      <PageHeader
        title="Ввод в оборот"
        actions={
          <Button variant="outline" size="sm" onClick={() => void refresh()} disabled={isBusy}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <StatRow
        items={[
          { label: 'Документов', value: items.length },
          { label: 'Показано', value: filteredItems.length },
          { label: 'Выбрано', value: selectedIds.length },
        ]}
      />

      <div className="grid items-start gap-3 xl:grid-cols-[minmax(260px,0.55fr)_minmax(0,1.45fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Параметры</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
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
              <TextInput
                value={batchNumber}
                className={cn('t-input', batchError && 'is-error is-shaking')}
                onChange={(event) => {
                  setBatchNumber(event.target.value)
                  setBatchError(false)
                }}
              />
            </div>
            <Button size="sm" onClick={() => void runIntroduction()} disabled={isBusy || selectedIds.length === 0}>
              <PlayCircle className="h-3.5 w-3.5" />
              Ввести в оборот{selectedIds.length > 1 ? ` (${selectedIds.length})` : ''}
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Готовые заявки</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2">
            <div className="grid gap-2 sm:grid-cols-2">
              <SelectNative
                value={statusFilter}
                aria-label="Фильтр по статусу"
                onChange={(event) => setStatusFilter(event.target.value)}
              >
                <option value="">Все статусы</option>
                {statusOptions.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </SelectNative>
              <TableSearch value={search} onChange={setSearch} />
            </div>

            {loading && items.length === 0 ? (
              <TableSkeleton rows={8} />
            ) : filteredItems.length === 0 ? (
              <EmptyState>Нет документов для ввода в оборот</EmptyState>
            ) : (
              <div className="max-h-[560px] overflow-auto">
                <Table aria-label="Заказы для ввода в оборот">
                  <TableHeader>
                    <TableRow>
                      <TableHead isRowHeader={false}>Выбор</TableHead>
                      <TableHead isRowHeader>Заявка</TableHead>
                      <TableHead>Полное наименование</TableHead>
                      <TableHead>Статус</TableHead>
                      <TableHead>GTIN</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pager.pageRows.map((item, index) => {
                      const documentId = item.document_id || ''
                      const rowId = documentId || `${item.order_name}-${index}`
                      return (
                        <IntroRow
                          key={rowId}
                          rowId={rowId}
                          item={item}
                          checked={Boolean(documentId) && selectedIds.includes(documentId)}
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
    </div>
  )
}

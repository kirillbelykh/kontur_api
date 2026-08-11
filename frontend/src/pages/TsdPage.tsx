import { useCallback, useEffect, useMemo, useState } from 'react'
import { PenLine, PlayCircle, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { DatePickerField } from '@/components/ui/date-picker'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { TablePagination, usePagination } from '@/components/ui/pagination'
import { SelectNative } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

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

const EMPTY_FORM: TsdForm = {
  intro_number: '',
  production_date: '',
  expiration_date: '',
  batch_number: '',
}

function toneForStatus(status?: string) {
  const value = (status || '').toLowerCase()
  if (!value) return 'secondary' as const
  if (value.includes('ошиб') || value.includes('error') || value.includes('reject')) return 'danger' as const
  if (value.includes('не созд') || value.includes('ожид') || value.includes('pending') || value.includes('creat')) {
    return 'warning' as const
  }
  if (value.includes('созд') || value.includes('готов') || value.includes('ready') || value.includes('released') || value.includes('received')) {
    return 'success' as const
  }
  return 'info' as const
}

function matchesQuery(item: TsdItem, query: string) {
  const normalized = query.trim().toLowerCase()
  if (!normalized) return true
  return Object.values(item)
    .map((value) => String(value ?? '').toLowerCase())
    .join(' ')
    .includes(normalized)
}

export function TsdPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [items, setItems] = useCachedState<TsdItem[]>('tsd.items', [])
  const [live, setLive] = useState(false)
  const [form, setForm] = useState<TsdForm>(EMPTY_FORM)
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [selectedIds, setSelectedIds] = useState<string[]>([])
  const [introNumberError, setIntroNumberError] = useState(false)

  const setField = <K extends keyof TsdForm>(key: K, value: TsdForm[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const applyItems = (next: TsdItem[]) => {
    setItems(next)
    setSelectedIds((prev) => prev.filter((id) => next.some((item) => item.document_id === id)))
  }

  const toggleId = (documentId: string) => {
    if (!documentId) return
    setSelectedIds((prev) =>
      prev.includes(documentId) ? prev.filter((id) => id !== documentId) : [...prev, documentId],
    )
  }

  const load = useCallback(async (useLive = false) => {
    setLoading(true)
    try {
      const result = await apiCall<TsdState>('get_tsd_state', useLive)
      applyItems(result.items ?? [])
      setLive(Boolean(result.live || useLive))
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить задания ТСД'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(false)
  }, [load])

  // Автозаполнение дат (01-03-2026 / 01-03-2031 из бэкенда)
  useEffect(() => {
    void apiCall<{ production_date?: string; expiration_date?: string }>('get_default_date_window')
      .then((window) => {
        setForm((prev) => ({
          ...prev,
          production_date: prev.production_date || String(window.production_date || ''),
          expiration_date: prev.expiration_date || String(window.expiration_date || ''),
        }))
      })
      .catch(() => null)
  }, [])

  const statusOptions = useMemo(
    () => Array.from(new Set(items.map((item) => String(item.tsd_status || '').trim()).filter(Boolean))),
    [items],
  )

  const rows = useMemo(
    () =>
      items.filter((item) => {
        if (!matchesQuery(item, search)) return false
        if (!statusFilter) return true
        return String(item.tsd_status || '').trim() === statusFilter
      }),
    [items, search, statusFilter],
  )

  const readyCount = useMemo(() => items.filter((item) => item.can_tsd).length, [items])
  const isBusy = Boolean(busy)
  const pager = usePagination(rows)

  const runBusy = async (key: string, action: () => Promise<void>, successMessage?: string) => {
    setBusy(key)
    try {
      await action()
      if (successMessage) toast.success(successMessage)
    } catch (error) {
      toast.error(getErrorMessage(error))
    } finally {
      setBusy(null)
    }
  }

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
      },
      'Задания на ТСД созданы.',
    )

  const signIntroduction = () => {
    if (selectedIds.length !== 1) {
      toast.error('Выберите один заказ для подписи.')
      return
    }
    if (!window.confirm('Подписать и ввести в оборот?')) return
    return runBusy(
      'sign',
      async () => {
        const result = await apiCall<SignResult>('sign_tsd_introduction', selectedIds[0])
        if (result?.state?.items) {
          setItems(result.state.items)
          setLive(true)
        } else {
          await load(true)
        }
      },
      'Документ подписан и отправлен в ГИС МТ.',
    )
  }

  return (
    <div className="page-shell">
      <PageHeader
        title="Задание на ТСД"
        subtitle="Создание заданий на терминал сбора данных, контроль статусов и подпись ввода в оборот."
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

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Документов" value={items.length} />
        <StatPill label="Готовы к ТСД" value={readyCount} />
        <StatPill label="Выбрано" value={selectedIds.length} />
        <StatPill label="Режим" value={live ? 'Live' : 'Кэш'} />
      </div>

      <Card className="mb-4">
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
              Создать задания{selectedIds.length > 1 ? ` (${selectedIds.length})` : ''}
            </Button>
            <Button size="sm" variant="outline" onClick={() => void signIntroduction()} disabled={isBusy || selectedIds.length !== 1}>
              <PenLine className="h-3.5 w-3.5" />
              Подписать и ввести в оборот
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
                placeholder="Номер ввода в оборот"
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
                placeholder="Номер партии"
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>Заказы</CardTitle>
            <CardDescription>Отметьте заказы чекбоксами — задания создаются для всех выбранных.</CardDescription>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <div className="w-44">
              <SelectNative
                value={statusFilter}
                placeholder="Все статусы"
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
              <TableSearch value={search} onChange={setSearch} placeholder="Поиск по заказам" />
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
                    const selected = Boolean(documentId) && selectedIds.includes(documentId)
                    return (
                      <TableRow
                        key={rowId}
                        id={rowId}
                        className={cn(selected && 'bg-muted/60')}
                        onClick={() => toggleId(documentId)}
                      >
                        <TableCell>
                          <Checkbox
                            isSelected={selected}
                            aria-label={`Выбрать заказ ${item.order_name || documentId}`}
                            onChange={() => toggleId(documentId)}
                          />
                        </TableCell>
                        <TableCell textValue={item.order_name || documentId}>
                          <div className="font-medium">{item.order_name || documentId || 'Без названия'}</div>
                          <div className="font-mono text-[11px] text-muted-foreground">{documentId || '—'}</div>
                        </TableCell>
                        <TableCell className="text-muted-foreground">
                          <div className="max-w-[280px] truncate">{item.full_name || item.simpl || '—'}</div>
                        </TableCell>
                        <TableCell>
                          <Badge tone={toneForStatus(item.status)}>{item.status || '—'}</Badge>
                          {item.status_summary ? (
                            <div className="mt-1 text-[11px] text-muted-foreground">{item.status_summary}</div>
                          ) : null}
                        </TableCell>
                        <TableCell>
                          <Badge tone={toneForStatus(item.tsd_status)}>{item.tsd_status || '—'}</Badge>
                          {item.tsd_intro_number ? (
                            <div className="mt-1 font-mono text-[11px] text-muted-foreground">{item.tsd_intro_number}</div>
                          ) : null}
                        </TableCell>
                        <TableCell className="font-mono text-xs text-muted-foreground">{item.gtin || '—'}</TableCell>
                      </TableRow>
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

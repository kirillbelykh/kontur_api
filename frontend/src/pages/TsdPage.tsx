import { useCallback, useEffect, useMemo, useState, type InputHTMLAttributes, type ReactNode } from 'react'
import { PenLine, PlayCircle, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DatePickerField } from '@/components/ui/date-picker'
import { SelectNative } from '@/components/ui/select'
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

function FieldLabel({ children }: { children: ReactNode }) {
  return <label className="mb-1 block text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{children}</label>
}

function TextInput(props: InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={cn('input-thin h-9 w-full px-2.5 py-0 text-sm', props.className)} />
}

export function TsdPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [items, setItems] = useState<TsdItem[]>([])
  const [live, setLive] = useState(false)
  const [form, setForm] = useState<TsdForm>(EMPTY_FORM)
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [selectedId, setSelectedId] = useState('')

  const setField = <K extends keyof TsdForm>(key: K, value: TsdForm[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const applyItems = (next: TsdItem[]) => {
    setItems(next)
    setSelectedId((prev) => (next.some((item) => item.document_id === prev) ? prev : ''))
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

  const createTasks = () =>
    runBusy(
      'create',
      async () => {
        if (!selectedId) throw new Error('Выберите хотя бы один заказ для задания на ТСД.')
        if (!form.intro_number.trim()) throw new Error('Укажите номер ввода в оборот.')

        const selectedIds = [selectedId]
        const result = await apiCall<TsdRunResult>(
          'create_tsd_tasks',
          selectedIds,
          form.intro_number,
          form.production_date,
          form.expiration_date,
          form.batch_number,
        )

        const failedIds = new Set((result.errors || []).map((entry) => entry.document_id))
        setSelectedId(failedIds.has(selectedId) ? selectedId : '')
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
    if (!selectedId) {
      toast.error('Выберите заказ для подписи.')
      return
    }
    if (!window.confirm('Подписать и ввести в оборот?')) return
    return runBusy(
      'sign',
      async () => {
        const result = await apiCall<SignResult>('sign_tsd_introduction', selectedId)
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
        <StatPill label="Выбрано" value={selectedId ? 1 : 0} />
        <StatPill label="Режим" value={live ? 'Live' : 'Кэш'} />
      </div>

      <Card className="mb-4">
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>Параметры задания</CardTitle>
            <CardDescription>Номер ввода в оборот обязателен. Даты: YYYY-MM, YYYY-MM-DD или DD-MM-YYYY.</CardDescription>
          </div>
          <div className="flex flex-wrap gap-1.5">
            <Button
              size="sm"
              variant="success"
              onClick={() => void createTasks()}
              disabled={isBusy || !selectedId || !form.intro_number.trim()}
            >
              <PlayCircle className="h-3.5 w-3.5" />
              Создать задания
            </Button>
            <Button size="sm" variant="outline" onClick={() => void signIntroduction()} disabled={isBusy || !selectedId}>
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
                onChange={(event) => setField('intro_number', event.target.value)}
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
            <CardDescription>Выберите заказ кликом по строке — задание создаётся для выбранной заявки.</CardDescription>
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
              <TextInput value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Поиск по заказам" />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {rows.length === 0 ? (
            <EmptyState>Данных пока нет.</EmptyState>
          ) : (
            <div className="max-h-[520px] overflow-auto">
              <Table aria-label="Заказы для задания на ТСД">
                <TableHeader>
                  <TableRow>
                    <TableHead>Заявка</TableHead>
                    <TableHead>Полное наименование</TableHead>
                    <TableHead>Статус ЧЗ</TableHead>
                    <TableHead>На ТСД</TableHead>
                    <TableHead>GTIN</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((item, index) => {
                    const documentId = item.document_id || ''
                    const rowId = documentId || `${item.order_name}-${index}`
                    const selected = documentId === selectedId
                    return (
                      <TableRow
                        key={rowId}
                        id={rowId}
                        className={cn(selected && 'bg-muted/60')}
                        onClick={() => setSelectedId(documentId)}
                      >
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
        </CardContent>
      </Card>
    </div>
  )
}

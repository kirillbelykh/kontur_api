import { useCallback, useEffect, useMemo, useState, type InputHTMLAttributes, type ReactNode } from 'react'
import { PlayCircle, RefreshCw } from 'lucide-react'
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

function toneForStatus(status?: string) {
  const value = (status || '').toLowerCase()
  if (!value) return 'secondary' as const
  if (value.includes('ошиб') || value.includes('error')) return 'danger' as const
  if (value.includes('ожид') || value.includes('pending') || value.includes('creat')) return 'warning' as const
  if (value.includes('введен') || value.includes('готов') || value.includes('ready') || value.includes('received')) {
    return 'success' as const
  }
  return 'info' as const
}

function FieldLabel({ children }: { children: ReactNode }) {
  return <label className="mb-1 block text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{children}</label>
}

function TextInput(props: InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={cn('input-thin h-9 w-full px-2.5 py-0 text-sm', props.className)} />
}

export function IntroPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [items, setItems] = useState<IntroItem[]>([])
  const [selectedId, setSelectedId] = useState('')
  const [search, setSearch] = useState('')
  const [statusFilter, setStatusFilter] = useState('')
  const [productionDate, setProductionDate] = useState('')
  const [expirationDate, setExpirationDate] = useState('')
  const [batchNumber, setBatchNumber] = useState('')

  const applyItems = useCallback((next: IntroItem[]) => {
    setItems(next)
    setSelectedId((prev) => (next.some((item) => item.document_id === prev) ? prev : ''))
  }, [])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const result = await apiCall<IntroState>('get_intro_state')
      applyItems(result.items ?? [])
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить ввод в оборот'))
    } finally {
      setLoading(false)
    }
  }, [applyItems])

  useEffect(() => {
    void load()
  }, [load])

  const statusOptions = useMemo(
    () => [...new Set(items.map((item) => String(item.status || '').trim()).filter(Boolean))],
    [items],
  )

  const filteredItems = useMemo(() => {
    const query = search.trim().toLowerCase()
    return items.filter((item) => {
      if (statusFilter && String(item.status || '').trim() !== statusFilter) return false
      if (!query) return true
      const haystack = [item.order_name, item.full_name, item.simpl, item.gtin, item.document_id, item.status]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
  }, [items, search, statusFilter])

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

  const runIntroduction = () =>
    runBusy(
      'run',
      async () => {
        if (!selectedId) throw new Error('Выберите хотя бы один заказ для ввода в оборот.')
        if (!batchNumber.trim()) throw new Error('Укажите номер партии.')
        const result = await apiCall<IntroResult>(
          'introduce_orders',
          [selectedId],
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

  return (
    <div className="page-shell">
      <PageHeader
        title="Ввод в оборот"
        subtitle="Документы для ввода кодов маркировки в оборот."
        actions={
          <Button variant="outline" size="sm" onClick={() => void refresh()} disabled={isBusy}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить список заказов
          </Button>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-3">
        <StatPill label="Документов" value={items.length} />
        <StatPill label="Показано" value={filteredItems.length} />
        <StatPill label="Выбрано" value={selectedId ? 1 : 0} />
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Параметры ввода в оборот</CardTitle>
            <CardDescription>Даты принимаются в форматах YYYY-MM, YYYY-MM-DD или DD-MM-YYYY.</CardDescription>
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
                onChange={(event) => setBatchNumber(event.target.value)}
                placeholder="Номер партии"
              />
            </div>
            <Button size="sm" onClick={() => void runIntroduction()} disabled={isBusy || !selectedId}>
              <PlayCircle className="h-3.5 w-3.5" />
              Выполнить ввод в оборот
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Готовые заявки</CardTitle>
            <CardDescription>Выберите заказ в таблице — клик по строке выбирает документ.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-2">
              <SelectNative value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                <option value="">Все статусы</option>
                {statusOptions.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </SelectNative>
              <TextInput
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Поиск по заказам"
              />
            </div>

            {filteredItems.length === 0 ? (
              <EmptyState>Нет документов для ввода в оборот</EmptyState>
            ) : (
              <div className="max-h-[420px] overflow-auto">
                <Table aria-label="Заказы для ввода в оборот">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Заявка</TableHead>
                      <TableHead>Полное наименование</TableHead>
                      <TableHead>Статус</TableHead>
                      <TableHead>GTIN</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredItems.map((item, index) => {
                      const rowId = item.document_id || `${item.order_name}-${index}`
                      const selected = Boolean(item.document_id) && item.document_id === selectedId
                      return (
                        <TableRow
                          key={rowId}
                          id={rowId}
                          className={cn(selected && 'bg-muted/60')}
                          onClick={() => setSelectedId(item.document_id || '')}
                        >
                          <TableCell>
                            <div className="font-medium">{item.order_name || item.document_id || 'Без названия'}</div>
                            <div className="font-mono text-[11px] text-muted-foreground">{item.document_id || '—'}</div>
                          </TableCell>
                          <TableCell className="text-muted-foreground">{item.full_name || item.simpl || '—'}</TableCell>
                          <TableCell>
                            <Badge tone={toneForStatus(item.status)}>{item.status || '—'}</Badge>
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
    </div>
  )
}

import { useCallback, useEffect, useMemo, useState, type InputHTMLAttributes, type ReactNode, type SelectHTMLAttributes } from 'react'
import { RefreshCw, Search, Trash2, Undo2, X } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { SelectNative } from '@/components/ui/select'

type OrderMode = 'params' | 'gtin'

type QueueItem = {
  uid?: string
  order_name?: string
  simpl_name?: string
  gtin?: string
  codes_count?: number
  size?: string
  color?: string
  units_per_pack?: string
  full_name?: string
  mode?: string
}

type OrderRow = {
  document_id?: string
  order_name?: string
  name?: string
  status?: string
  status_summary?: string
  gtin?: string
  full_name?: string
  simpl?: string
  codes_count?: number | string
  created_at?: string
  deleted_at?: string
  deleted_by?: string
}

type OrdersViewState = {
  queue?: QueueItem[]
  session_orders?: OrderRow[]
  history?: OrderRow[]
  deleted_orders?: OrderRow[]
}

type OptionsState = {
  simplified_options?: string[]
  color_options?: string[]
  size_options?: string[]
  units_options?: string[]
  color_required?: string[]
  venchik_options?: string[]
  venchik_required?: string[]
}

type LookupResult = {
  gtin?: string
  full_name?: string
  tnved_code?: string
  simpl_name?: string
}

type OrderDetailsField = { label?: string; value?: string }
type OrderDetailsPayload = {
  document_id?: string
  fields?: OrderDetailsField[]
  source_type?: string
}

type OrderForm = {
  order_name: string
  name: string
  gtin: string
  size: string
  color: string
  venchik: string
  units_per_pack: string
  codes_count: string
}

const EMPTY_FORM: OrderForm = {
  order_name: '',
  name: '',
  gtin: '',
  size: '',
  color: '',
  venchik: '',
  units_per_pack: '',
  codes_count: '',
}

function rowTitle(item: { order_name?: string; name?: string; document_id?: string }) {
  return item.order_name || item.name || item.document_id || 'Без названия'
}

function toneForStatus(status?: string) {
  const value = (status || '').toLowerCase()
  if (!value) return 'secondary' as const
  if (value.includes('ошиб') || value.includes('error') || value.includes('reject')) return 'danger' as const
  if (value.includes('ожид') || value.includes('pending') || value.includes('creat')) return 'warning' as const
  if (value.includes('готов') || value.includes('ready') || value.includes('released') || value.includes('received')) {
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

function TextSelect(props: SelectHTMLAttributes<HTMLSelectElement>) {
  return <SelectNative {...props} className={cn('w-full', props.className)} />
}

export function OrdersPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [state, setState] = useState<OrdersViewState>({})
  const [options, setOptions] = useState<OptionsState>({})
  const [mode, setMode] = useState<OrderMode>('params')
  const [form, setForm] = useState<OrderForm>(EMPTY_FORM)
  const [lookup, setLookup] = useState<LookupResult | null>(null)
  const [selectedQueueId, setSelectedQueueId] = useState('')
  const [selectedHistoryId, setSelectedHistoryId] = useState('')
  const [selectedDeletedId, setSelectedDeletedId] = useState('')
  const [historySearch, setHistorySearch] = useState('')
  const [showDeleted, setShowDeleted] = useState(false)
  const [details, setDetails] = useState<OrderDetailsPayload | null>(null)
  const [detailsOpen, setDetailsOpen] = useState(false)

  const setField = <K extends keyof OrderForm>(key: K, value: OrderForm[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const colorNeeded = Boolean(form.name && (options.color_required || []).includes(form.name))
  const venchikNeeded = Boolean(form.name && (options.venchik_required || []).includes(form.name))
  const paramsMode = mode === 'params'

  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<OrdersViewState>('get_orders_view_state', force)
      setState(result)
      setSelectedQueueId((prev) => ((result.queue || []).some((item) => item.uid === prev) ? prev : ''))
      setSelectedHistoryId((prev) => ((result.history || []).some((item) => item.document_id === prev) ? prev : ''))
      setSelectedDeletedId((prev) => ((result.deleted_orders || []).some((item) => item.document_id === prev) ? prev : ''))
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить заказы'))
    } finally {
      setLoading(false)
    }
  }, [])

  const loadOptions = useCallback(async () => {
    try {
      const result = await apiCall<OptionsState>('get_options')
      setOptions(result)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить справочники'))
    }
  }, [])

  useEffect(() => {
    void load(false)
    void loadOptions()
  }, [load, loadOptions])

  const queue = state.queue ?? []
  const sessionOrders = state.session_orders ?? []
  const history = state.history ?? []
  const deletedOrders = state.deleted_orders ?? []

  const filteredHistory = useMemo(() => {
    const query = historySearch.trim().toLowerCase()
    if (!query) return history
    return history.filter((item) => {
      const haystack = [item.order_name, item.full_name, item.gtin, item.document_id, item.status]
        .filter(Boolean)
        .join(' ')
        .toLowerCase()
      return haystack.includes(query)
    })
  }, [history, historySearch])

  const buildPayload = () => ({
    order_name: form.order_name.trim(),
    name: form.name.trim(),
    gtin: form.gtin.trim(),
    size: form.size,
    color: form.color,
    venchik: form.venchik,
    units_per_pack: form.units_per_pack,
    codes_count: Number(form.codes_count || 0),
    mode,
  })

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

  const lookupByParams = () =>
    runBusy('lookup', async () => {
      const result = await apiCall<LookupResult>(
        'lookup_gtin',
        form.name,
        form.size,
        form.units_per_pack,
        form.color,
        form.venchik,
      )
      setLookup(result)
      if (result.gtin) setField('gtin', result.gtin)
    })

  const lookupByGtin = () =>
    runBusy('lookup-gtin', async () => {
      const result = await apiCall<LookupResult>('lookup_gtin_by_code', form.gtin)
      setLookup(result)
      if (result.simpl_name) setField('name', result.simpl_name)
      if (result.gtin) setField('gtin', result.gtin)
    })

  const addToQueue = () =>
    runBusy(
      'add',
      async () => {
        const result = await apiCall<{ queue?: QueueItem[]; item?: QueueItem }>('add_order_item', buildPayload())
        if (result.queue) setState((prev) => ({ ...prev, queue: result.queue }))
        if (result.item?.uid) setSelectedQueueId(result.item.uid)
        setForm((prev) => ({ ...EMPTY_FORM, order_name: prev.order_name }))
        setLookup(null)
      },
      'Позиция добавлена в очередь',
    )

  const createNow = () =>
    runBusy(
      'create',
      async () => {
        await apiCall('create_order', buildPayload())
        setForm((prev) => ({ ...EMPTY_FORM, order_name: prev.order_name }))
        setLookup(null)
        await load(true)
      },
      'Заказ создан',
    )

  const submitQueue = () =>
    runBusy(
      'submit',
      async () => {
        const result = await apiCall<{ state?: OrdersViewState; errors?: Array<{ order_name?: string; error?: string }> }>(
          'submit_order_queue',
        )
        if (result.state) setState(result.state)
        else await load(true)
        setSelectedQueueId('')
        if (result.errors?.length) {
          toast.error(`Часть заказов с ошибками: ${result.errors.length}`)
        }
      },
      'Очередь заказов выполнена',
    )

  const clearQueue = () =>
    runBusy(
      'clear',
      async () => {
        const result = await apiCall<{ queue?: QueueItem[] }>('clear_order_queue')
        setState((prev) => ({ ...prev, queue: result.queue || [] }))
        setSelectedQueueId('')
      },
      'Очередь очищена',
    )

  const removeQueueItem = () =>
    runBusy(
      'remove',
      async () => {
        if (!selectedQueueId) throw new Error('Выберите позицию в очереди')
        const result = await apiCall<{ queue?: QueueItem[] }>('remove_order_item', selectedQueueId)
        setState((prev) => ({ ...prev, queue: result.queue || [] }))
        setSelectedQueueId('')
      },
      'Позиция удалена из очереди',
    )

  const deleteHistoryOrder = () =>
    runBusy(
      'delete',
      async () => {
        if (!selectedHistoryId) throw new Error('Выберите заказ в истории')
        await apiCall('delete_order', selectedHistoryId)
        await load(true)
      },
      'Заказ перемещён в удалённые',
    )

  const restoreDeleted = () =>
    runBusy(
      'restore',
      async () => {
        if (!selectedDeletedId) throw new Error('Выберите удалённый заказ')
        await apiCall('restore_deleted_order', selectedDeletedId)
        await load(true)
      },
      'Заказ восстановлен',
    )

  const exportHistory = () =>
    runBusy(
      'export',
      async () => {
        const result = await apiCall<{ state?: OrdersViewState }>('export_order_history')
        if (result.state) setState(result.state)
        else await load(false)
      },
      'История заказов выгружена',
    )

  const addHistoryToActive = () =>
    runBusy(
      'to-active',
      async () => {
        if (!selectedHistoryId) throw new Error('Выберите заказ в истории')
        await apiCall('add_history_orders_to_active', [selectedHistoryId])
      },
      'Заказ добавлен в загрузку',
    )

  const openDetails = (documentId?: string) =>
    runBusy('details', async () => {
      const id = String(documentId || selectedHistoryId || '').trim()
      if (!id) throw new Error('Выберите заказ')
      setDetailsOpen(true)
      setDetails(null)
      const payload = await apiCall<OrderDetailsPayload>('get_order_details', id)
      setDetails(payload)
    })

  const isBusy = Boolean(busy)

  return (
    <div className="page-shell">
      <PageHeader
        title="Заказ кодов"
        subtitle="Создание, очередь и история заказов кодов маркировки."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void load(true)} disabled={loading || isBusy}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button variant="outline" size="sm" onClick={() => void exportHistory()} disabled={isBusy}>
              Выгрузить историю
            </Button>
          </>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Очередь" value={queue.length} />
        <StatPill label="Сессия" value={sessionOrders.length} />
        <StatPill label="История" value={history.length} />
        <StatPill label="Удалённые" value={deletedOrders.length} />
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.05fr)_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Новый заказ</CardTitle>
            <CardDescription>Параметры номенклатуры или прямой GTIN → очередь / сразу в Контур.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="inline-flex rounded-md border border-border bg-muted/40 p-0.5">
              <button
                type="button"
                className={cn(
                  'rounded px-3 py-1.5 text-xs font-medium transition',
                  mode === 'params' ? 'bg-card text-foreground shadow-sm' : 'text-muted-foreground',
                )}
                onClick={() => setMode('params')}
              >
                По параметрам
              </button>
              <button
                type="button"
                className={cn(
                  'rounded px-3 py-1.5 text-xs font-medium transition',
                  mode === 'gtin' ? 'bg-card text-foreground shadow-sm' : 'text-muted-foreground',
                )}
                onClick={() => setMode('gtin')}
              >
                По GTIN
              </button>
            </div>

            <div className="grid gap-2 sm:grid-cols-2">
              <div className="sm:col-span-2">
                <FieldLabel>Название заявки</FieldLabel>
                <TextInput value={form.order_name} onChange={(e) => setField('order_name', e.target.value)} placeholder="Номер / имя заказа" />
              </div>

              <div className="sm:col-span-2">
                <FieldLabel>Наименование</FieldLabel>
                <TextInput
                  list="orders-product-options"
                  value={form.name}
                  disabled={!paramsMode}
                  onChange={(e) => setField('name', e.target.value)}
                  placeholder="Упрощённое имя"
                />
                <datalist id="orders-product-options">
                  {(options.simplified_options || []).map((value) => (
                    <option key={value} value={value} />
                  ))}
                </datalist>
              </div>

              <div>
                <FieldLabel>Размер</FieldLabel>
                <TextSelect value={form.size} disabled={!paramsMode} onChange={(e) => setField('size', e.target.value)}>
                  <option value="">Выберите</option>
                  {(options.size_options || []).map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </TextSelect>
              </div>

              <div>
                <FieldLabel>Ед. в упаковке</FieldLabel>
                <TextSelect
                  value={form.units_per_pack}
                  disabled={!paramsMode}
                  onChange={(e) => setField('units_per_pack', e.target.value)}
                >
                  <option value="">Выберите</option>
                  {(options.units_options || []).map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </TextSelect>
              </div>

              <div>
                <FieldLabel>Цвет{colorNeeded ? ' *' : ''}</FieldLabel>
                <TextSelect
                  value={form.color}
                  disabled={!paramsMode || !colorNeeded}
                  onChange={(e) => setField('color', e.target.value)}
                >
                  <option value="">Выберите</option>
                  {(options.color_options || []).map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </TextSelect>
              </div>

              <div>
                <FieldLabel>Венчик{venchikNeeded ? ' *' : ''}</FieldLabel>
                <TextSelect
                  value={form.venchik}
                  disabled={!paramsMode || !venchikNeeded}
                  onChange={(e) => setField('venchik', e.target.value)}
                >
                  <option value="">Выберите</option>
                  {(options.venchik_options || []).map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </TextSelect>
              </div>

              <div>
                <FieldLabel>Количество кодов</FieldLabel>
                <TextInput
                  type="number"
                  min={1}
                  value={form.codes_count}
                  onChange={(e) => setField('codes_count', e.target.value)}
                  placeholder="0"
                />
              </div>

              <div>
                <FieldLabel>GTIN</FieldLabel>
                <TextInput
                  value={form.gtin}
                  disabled={paramsMode}
                  onChange={(e) => setField('gtin', e.target.value)}
                  placeholder="014..."
                  className="font-mono"
                />
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              {paramsMode ? (
                <Button size="sm" variant="outline" onClick={() => void lookupByParams()} disabled={isBusy}>
                  <Search className="h-3.5 w-3.5" />
                  Найти GTIN
                </Button>
              ) : (
                <Button size="sm" variant="outline" onClick={() => void lookupByGtin()} disabled={isBusy}>
                  <Search className="h-3.5 w-3.5" />
                  Найти по GTIN
                </Button>
              )}
              <Button size="sm" onClick={() => void addToQueue()} disabled={isBusy}>
                В очередь
              </Button>
              <Button size="sm" variant="secondary" onClick={() => void createNow()} disabled={isBusy}>
                Создать сразу
              </Button>
            </div>

            {lookup ? (
              <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-xs">
                <div>
                  <span className="text-muted-foreground">GTIN: </span>
                  <span className="font-mono">{lookup.gtin || '—'}</span>
                </div>
                <div className="mt-1">
                  <span className="text-muted-foreground">Полное имя: </span>
                  {lookup.full_name || '—'}
                </div>
                <div className="mt-1">
                  <span className="text-muted-foreground">ТН ВЭД: </span>
                  {lookup.tnved_code || '—'}
                </div>
              </div>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
            <div>
              <CardTitle>Очередь</CardTitle>
              <CardDescription>Позиции к массовой отправке в Контур.</CardDescription>
            </div>
            <div className="flex flex-wrap gap-1.5">
              <Button size="sm" onClick={() => void submitQueue()} disabled={isBusy || queue.length === 0}>
                Отправить очередь
              </Button>
              <Button size="sm" variant="outline" onClick={() => void removeQueueItem()} disabled={isBusy || !selectedQueueId}>
                Удалить
              </Button>
              <Button size="sm" variant="ghost" onClick={() => void clearQueue()} disabled={isBusy || queue.length === 0}>
                Очистить
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {queue.length === 0 ? (
              <EmptyState>Очередь пуста</EmptyState>
            ) : (
              <div className="max-h-[360px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Заявка</th>
                      <th className="px-2 py-2 font-medium">Товар</th>
                      <th className="px-2 py-2 font-medium">GTIN</th>
                      <th className="px-2 py-2 font-medium">Кодов</th>
                    </tr>
                  </thead>
                  <tbody>
                    {queue.map((item) => {
                      const selected = item.uid === selectedQueueId
                      return (
                        <tr
                          key={item.uid || item.order_name}
                          className={cn(
                            'cursor-pointer border-b border-border/70 hover:bg-muted/40',
                            selected && 'bg-muted/60',
                          )}
                          onClick={() => setSelectedQueueId(item.uid || '')}
                        >
                          <td className="px-2 py-2 font-medium">{item.order_name || '—'}</td>
                          <td className="px-2 py-2 text-muted-foreground">{item.simpl_name || '—'}</td>
                          <td className="px-2 py-2 font-mono text-xs text-muted-foreground">{item.gtin || '—'}</td>
                          <td className="px-2 py-2 tabular-nums">{item.codes_count ?? '—'}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Сессия</CardTitle>
            <CardDescription>Заказы, созданные в текущем запуске приложения.</CardDescription>
          </CardHeader>
          <CardContent>
            {sessionOrders.length === 0 ? (
              <EmptyState>В этой сессии заказов ещё нет</EmptyState>
            ) : (
              <div className="max-h-[280px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Заявка</th>
                      <th className="px-2 py-2 font-medium">Статус</th>
                      <th className="px-2 py-2 font-medium">GTIN</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sessionOrders.map((item, index) => (
                      <tr key={item.document_id || `${rowTitle(item)}-${index}`} className="border-b border-border/70">
                        <td className="px-2 py-2">
                          <div className="font-medium">{rowTitle(item)}</div>
                          <div className="font-mono text-[11px] text-muted-foreground">{item.document_id || '—'}</div>
                        </td>
                        <td className="px-2 py-2">
                          <Badge tone={toneForStatus(item.status)}>{item.status || '—'}</Badge>
                        </td>
                        <td className="px-2 py-2 font-mono text-xs text-muted-foreground">{item.gtin || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
            <div>
              <CardTitle>История</CardTitle>
              <CardDescription>Документы Контура / локальная история.</CardDescription>
            </div>
            <div className="flex flex-wrap gap-1.5">
              <Button size="sm" variant="outline" onClick={() => void openDetails()} disabled={isBusy || !selectedHistoryId}>
                Подробнее
              </Button>
              <Button size="sm" variant="outline" onClick={() => void addHistoryToActive()} disabled={isBusy || !selectedHistoryId}>
                В загрузку
              </Button>
              <Button size="sm" variant="danger" onClick={() => void deleteHistoryOrder()} disabled={isBusy || !selectedHistoryId}>
                <Trash2 className="h-3.5 w-3.5" />
                Удалить
              </Button>
              <Button size="sm" variant="ghost" onClick={() => setShowDeleted((v) => !v)}>
                {showDeleted ? 'Скрыть удалённые' : 'Удалённые'}
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <TextInput
              value={historySearch}
              onChange={(e) => setHistorySearch(e.target.value)}
              placeholder="Поиск по заявке, GTIN, ID…"
            />
            {filteredHistory.length === 0 ? (
              <EmptyState>История пуста</EmptyState>
            ) : (
              <div className="max-h-[320px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Заявка</th>
                      <th className="px-2 py-2 font-medium">Статус</th>
                      <th className="px-2 py-2 font-medium">GTIN</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredHistory.slice(0, 100).map((item, index) => {
                      const selected = item.document_id === selectedHistoryId
                      return (
                        <tr
                          key={item.document_id || `${rowTitle(item)}-${index}`}
                          className={cn(
                            'cursor-pointer border-b border-border/70 hover:bg-muted/40',
                            selected && 'bg-muted/60',
                          )}
                          onClick={() => setSelectedHistoryId(item.document_id || '')}
                          onDoubleClick={() => void openDetails(item.document_id)}
                        >
                          <td className="px-2 py-2">
                            <div className="font-medium">{rowTitle(item)}</div>
                            <div className="truncate text-xs text-muted-foreground">{item.full_name || item.simpl || '—'}</div>
                          </td>
                          <td className="px-2 py-2">
                            <Badge tone={toneForStatus(item.status)}>{item.status || '—'}</Badge>
                          </td>
                          <td className="px-2 py-2 font-mono text-xs text-muted-foreground">{item.gtin || '—'}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {showDeleted ? (
        <Card className="mt-4">
          <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
            <div>
              <CardTitle>Удалённые</CardTitle>
              <CardDescription>Архив удалённых заказов — можно восстановить.</CardDescription>
            </div>
            <Button size="sm" variant="outline" onClick={() => void restoreDeleted()} disabled={isBusy || !selectedDeletedId}>
              <Undo2 className="h-3.5 w-3.5" />
              Восстановить
            </Button>
          </CardHeader>
          <CardContent>
            {deletedOrders.length === 0 ? (
              <EmptyState>Удалённых заказов нет</EmptyState>
            ) : (
              <div className="max-h-[260px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Заявка</th>
                      <th className="px-2 py-2 font-medium">Удалён</th>
                      <th className="px-2 py-2 font-medium">Кем</th>
                    </tr>
                  </thead>
                  <tbody>
                    {deletedOrders.map((item, index) => {
                      const selected = item.document_id === selectedDeletedId
                      return (
                        <tr
                          key={item.document_id || `${rowTitle(item)}-${index}`}
                          className={cn(
                            'cursor-pointer border-b border-border/70 hover:bg-muted/40',
                            selected && 'bg-muted/60',
                          )}
                          onClick={() => setSelectedDeletedId(item.document_id || '')}
                        >
                          <td className="px-2 py-2">
                            <div className="font-medium">{rowTitle(item)}</div>
                            <div className="font-mono text-[11px] text-muted-foreground">{item.document_id || '—'}</div>
                          </td>
                          <td className="px-2 py-2 text-xs text-muted-foreground">{item.deleted_at || '—'}</td>
                          <td className="px-2 py-2 text-xs text-muted-foreground">{item.deleted_by || '—'}</td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      ) : null}

      {detailsOpen ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/40 p-4"
          onClick={() => setDetailsOpen(false)}
        >
          <div
            className="max-h-[85vh] w-full max-w-2xl overflow-hidden rounded-lg border border-border bg-card shadow-panel"
            onClick={(e) => e.stopPropagation()}
            role="dialog"
            aria-modal="true"
          >
            <div className="flex items-start justify-between gap-3 border-b border-border px-4 py-3">
              <div>
                <h2 className="text-base font-semibold">Подробнее о заказе</h2>
                <p className="text-xs text-muted-foreground">
                  {details?.document_id ? `Документ ${details.document_id}` : 'Метаданные из Контура и локальной истории'}
                </p>
              </div>
              <Button size="icon" variant="ghost" onClick={() => setDetailsOpen(false)} aria-label="Закрыть">
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="max-h-[70vh] overflow-auto px-4 py-3">
              {!details ? (
                <div className="py-8 text-center text-sm text-muted-foreground">Загружаем метаданные…</div>
              ) : (details.fields || []).length === 0 ? (
                <EmptyState>Нет данных по заказу</EmptyState>
              ) : (
                <dl className="grid gap-2 sm:grid-cols-2">
                  {(details.fields || []).map((field, index) => (
                    <div key={`${field.label}-${index}`} className="rounded-md border border-border/80 px-3 py-2">
                      <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">{field.label || '—'}</dt>
                      <dd className="mt-0.5 break-words text-sm">{field.value || '—'}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

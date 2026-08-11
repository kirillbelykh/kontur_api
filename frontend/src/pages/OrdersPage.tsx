import { useCallback, useEffect, useMemo, useRef, useState, type InputHTMLAttributes, type ReactNode } from 'react'
import { motion } from 'framer-motion'
import { Maximize2, RefreshCw, Search, Trash2, Undo2 } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { SelectNative, type SelectNativeProps } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'

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

function TextSelect(props: SelectNativeProps) {
  return <SelectNative {...props} className={cn('w-full', props.className)} />
}

function ModeToggle({ mode, onChange }: { mode: OrderMode; onChange: (mode: OrderMode) => void }) {
  return (
    <div className="inline-flex rounded-[var(--field-radius)] border border-border bg-muted/40 p-0.5">
      {(
        [
          { id: 'params', label: 'По параметрам' },
          { id: 'gtin', label: 'По GTIN' },
        ] as const
      ).map((item) => (
        <button
          key={item.id}
          type="button"
          onClick={() => onChange(item.id)}
          className={cn(
            'relative rounded-[var(--field-radius)] border-0 bg-transparent px-3 py-1.5 text-xs font-medium transition-colors',
            mode === item.id ? 'text-foreground' : 'text-muted-foreground hover:text-foreground',
          )}
        >
          {mode === item.id ? (
            <motion.span
              layoutId="orders-mode-toggle"
              transition={{ type: 'spring', stiffness: 520, damping: 38, mass: 0.8 }}
              className="absolute inset-0 rounded-[var(--field-radius)] bg-card shadow-sm"
            />
          ) : null}
          <span className="relative z-10">{item.label}</span>
        </button>
      ))}
    </div>
  )
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
  const [selectedHistoryIds, setSelectedHistoryIds] = useState<string[]>([])
  const [selectedDeletedId, setSelectedDeletedId] = useState('')
  const [historySearch, setHistorySearch] = useState('')
  const [showDeleted, setShowDeleted] = useState(false)
  const [details, setDetails] = useState<OrderDetailsPayload | null>(null)
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [historyFullscreen, setHistoryFullscreen] = useState(false)
  // Анимация «Очередь → История»: строки очереди улетают, новые строки истории прилетают
  const [queueLeaving, setQueueLeaving] = useState(false)
  const [arrivedIds, setArrivedIds] = useState<Set<string>>(new Set())
  const prevHistoryIdsRef = useRef<Set<string> | null>(null)

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
      setSelectedHistoryIds((prev) => prev.filter((id) => (result.history || []).some((item) => item.document_id === id)))
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
  const history = state.history ?? []
  const deletedOrders = state.deleted_orders ?? []

  // Помечаем свежепоявившиеся документы Истории для анимации «прилёта»
  useEffect(() => {
    const currentIds = new Set(history.map((item) => item.document_id || '').filter(Boolean))
    const previous = prevHistoryIdsRef.current
    prevHistoryIdsRef.current = currentIds
    if (!previous) return
    const fresh = new Set([...currentIds].filter((id) => !previous.has(id)))
    if (fresh.size === 0) return
    setArrivedIds(fresh)
    const timer = window.setTimeout(() => setArrivedIds(new Set()), 900)
    return () => window.clearTimeout(timer)
  }, [history])

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
        setQueueLeaving(true)
        try {
          // Даём строкам очереди «улететь» перед отправкой
          await new Promise((resolve) => window.setTimeout(resolve, 340))
          const result = await apiCall<{ state?: OrdersViewState; errors?: Array<{ order_name?: string; error?: string }> }>(
            'submit_order_queue',
          )
          if (result.state) setState(result.state)
          else await load(true)
          setSelectedQueueId('')
          if (result.errors?.length) {
            toast.error(`Часть заказов с ошибками: ${result.errors.length}`)
          }
        } finally {
          setQueueLeaving(false)
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
        if (selectedHistoryIds.length !== 1) throw new Error('Выберите один заказ в истории')
        await apiCall('delete_order', selectedHistoryIds[0])
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
        if (!selectedHistoryIds.length) throw new Error('Выберите заказы в истории')
        await apiCall('add_history_orders_to_active', selectedHistoryIds)
      },
      'Заказы добавлены в загрузку',
    )

  const openDetails = (documentId?: string) =>
    runBusy('details', async () => {
      const id = String(documentId || (selectedHistoryIds.length === 1 ? selectedHistoryIds[0] : '') || '').trim()
      if (!id) throw new Error('Выберите заказ')
      setDetailsOpen(true)
      setDetails(null)
      const payload = await apiCall<OrderDetailsPayload>('get_order_details', id)
      setDetails(payload)
    })

  const isBusy = Boolean(busy)

  const toggleHistoryId = (documentId: string) => {
    if (!documentId) return
    setSelectedHistoryIds((prev) =>
      prev.includes(documentId) ? prev.filter((id) => id !== documentId) : [...prev, documentId],
    )
  }

  const renderHistoryTable = (limit: number) => (
    <Table aria-label="История заказов">
      <TableHeader>
        <TableRow>
          <TableHead isRowHeader={false}>Выбор</TableHead>
          <TableHead isRowHeader>Заявка</TableHead>
          <TableHead>Статус</TableHead>
          <TableHead>GTIN</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {filteredHistory.slice(0, limit).map((item, index) => {
          const documentId = item.document_id || ''
          const rowId = documentId || `${rowTitle(item)}-${index}`
          const checked = Boolean(documentId) && selectedHistoryIds.includes(documentId)
          return (
            <TableRow
              key={rowId}
              id={rowId}
              className={cn(checked && 'bg-muted/60', arrivedIds.has(documentId) && 'order-arrive')}
              onClick={() => toggleHistoryId(documentId)}
            >
              <TableCell>
                <Checkbox
                  isSelected={checked}
                  aria-label={`Выбрать заказ ${rowTitle(item)}`}
                  onChange={() => toggleHistoryId(documentId)}
                />
              </TableCell>
              <TableCell>
                <div className="font-medium">{rowTitle(item)}</div>
                <div className="truncate text-xs text-muted-foreground">{item.full_name || item.simpl || '—'}</div>
              </TableCell>
              <TableCell>
                <Badge tone={toneForStatus(item.status)}>{item.status || '—'}</Badge>
              </TableCell>
              <TableCell className="font-mono text-xs text-muted-foreground">{item.gtin || '—'}</TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )

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

      <div className="mb-4 grid grid-cols-3 gap-2">
        <StatPill label="Очередь" value={queue.length} />
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
            <ModeToggle mode={mode} onChange={setMode} />

            <div className="grid gap-2 sm:grid-cols-2">
              <div className="sm:col-span-2">
                <FieldLabel>Название заявки</FieldLabel>
                <TextInput value={form.order_name} onChange={(e) => setField('order_name', e.target.value)} placeholder="Номер / имя заказа" />
              </div>

              <div className="sm:col-span-2">
                <FieldLabel>Наименование</FieldLabel>
                <TextSelect
                  searchable
                  placeholder="Упрощённое имя"
                  value={form.name}
                  disabled={!paramsMode}
                  onChange={(e) => setField('name', e.target.value)}
                >
                  <option value="">Выберите</option>
                  {(options.simplified_options || []).map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </TextSelect>
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
                <Table aria-label="Очередь заявок">
                  <TableHeader>
                    <TableRow>
                      <TableHead isRowHeader={false}>Выбор</TableHead>
                      <TableHead isRowHeader>Заявка</TableHead>
                      <TableHead>Товар</TableHead>
                      <TableHead>GTIN</TableHead>
                      <TableHead>Кодов</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {queue.map((item, index) => {
                      const rowId = item.uid || `${item.order_name}-${index}`
                      const checked = Boolean(item.uid) && item.uid === selectedQueueId
                      return (
                        <TableRow
                          key={rowId}
                          id={rowId}
                          className={cn(checked && 'bg-muted/60', queueLeaving && 'order-leave')}
                          onClick={() => setSelectedQueueId(item.uid === selectedQueueId ? '' : item.uid || '')}
                        >
                          <TableCell>
                            <Checkbox
                              isSelected={checked}
                              aria-label={`Выбрать позицию ${item.order_name || rowId}`}
                              onChange={(next) => setSelectedQueueId(next ? item.uid || '' : '')}
                            />
                          </TableCell>
                          <TableCell className="font-medium">{item.order_name || '—'}</TableCell>
                          <TableCell className="text-muted-foreground">{item.simpl_name || '—'}</TableCell>
                          <TableCell className="font-mono text-xs text-muted-foreground">{item.gtin || '—'}</TableCell>
                          <TableCell className="tabular-nums">{item.codes_count ?? '—'}</TableCell>
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

      <div className="mt-4">
        <Card>
          <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
            <div>
              <CardTitle>История</CardTitle>
              <CardDescription>Документы Контура / локальная история.</CardDescription>
            </div>
            <div className="flex flex-wrap gap-1.5">
              <Button size="sm" variant="outline" onClick={() => void openDetails()} disabled={isBusy || selectedHistoryIds.length !== 1}>
                Подробнее
              </Button>
              <Button size="sm" variant="outline" onClick={() => void addHistoryToActive()} disabled={isBusy || selectedHistoryIds.length === 0}>
                В загрузку{selectedHistoryIds.length > 1 ? ` (${selectedHistoryIds.length})` : ''}
              </Button>
              <Button size="sm" variant="danger" onClick={() => void deleteHistoryOrder()} disabled={isBusy || selectedHistoryIds.length !== 1}>
                <Trash2 className="h-3.5 w-3.5" />
                Удалить
              </Button>
              <Button size="sm" variant="ghost" onClick={() => setShowDeleted((v) => !v)}>
                {showDeleted ? 'Скрыть удалённые' : 'Удалённые'}
              </Button>
              <Button
                size="icon"
                variant="ghost"
                onClick={() => setHistoryFullscreen(true)}
                aria-label="Развернуть таблицу"
                title="Развернуть таблицу"
              >
                <Maximize2 className="h-4 w-4" />
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-3">
            <TextInput
              value={historySearch}
              onChange={(e) => setHistorySearch(e.target.value)}
              placeholder="Поиск по заявке, GTIN, ID…"
            />
            {loading && history.length === 0 ? (
              <TableSkeleton rows={6} />
            ) : filteredHistory.length === 0 ? (
              <EmptyState>История пуста</EmptyState>
            ) : (
              <div className="max-h-[360px] overflow-auto">{renderHistoryTable(100)}</div>
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
                <Table aria-label="Удалённые заказы">
                  <TableHeader>
                    <TableRow>
                      <TableHead isRowHeader={false}>Выбор</TableHead>
                      <TableHead isRowHeader>Заявка</TableHead>
                      <TableHead>Удалён</TableHead>
                      <TableHead>Кем</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {deletedOrders.map((item, index) => {
                      const documentId = item.document_id || ''
                      const rowId = documentId || `${rowTitle(item)}-${index}`
                      const checked = Boolean(documentId) && documentId === selectedDeletedId
                      return (
                        <TableRow
                          key={rowId}
                          id={rowId}
                          className={cn(checked && 'bg-muted/60')}
                          onClick={() => setSelectedDeletedId(documentId === selectedDeletedId ? '' : documentId)}
                        >
                          <TableCell>
                            <Checkbox
                              isSelected={checked}
                              aria-label={`Выбрать удалённый заказ ${rowTitle(item)}`}
                              onChange={(next) => setSelectedDeletedId(next ? documentId : '')}
                            />
                          </TableCell>
                          <TableCell>
                            <div className="font-medium">{rowTitle(item)}</div>
                            <div className="font-mono text-[11px] text-muted-foreground">{item.document_id || '—'}</div>
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">{item.deleted_at || '—'}</TableCell>
                          <TableCell className="text-xs text-muted-foreground">{item.deleted_by || '—'}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      ) : null}

      <Dialog open={historyFullscreen} onOpenChange={setHistoryFullscreen}>
        <DialogContent className="max-w-[96vw]">
          <DialogHeader>
            <DialogTitle>История заказов</DialogTitle>
            <p className="text-xs text-muted-foreground">Escape — закрыть. Показаны первые 500 записей.</p>
          </DialogHeader>
          <div className="max-h-[78vh] overflow-auto">{renderHistoryTable(500)}</div>
        </DialogContent>
      </Dialog>

      <Dialog open={detailsOpen} onOpenChange={setDetailsOpen}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>Подробнее о заказе</DialogTitle>
            <p className="text-xs text-muted-foreground">
              {details?.document_id ? `Документ ${details.document_id}` : 'Метаданные из Контура и локальной истории'}
            </p>
          </DialogHeader>
          <div className="max-h-[70vh] overflow-auto">
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
        </DialogContent>
      </Dialog>
    </div>
  )
}

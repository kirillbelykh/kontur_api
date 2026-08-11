import { useCallback, useEffect, useState } from 'react'
import { Plus, RefreshCw, Send, Trash2, RotateCcw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type OrderRow = {
  uid?: string
  document_id?: string
  order_name?: string
  name?: string
  simpl_name?: string
  status?: string
  gtin?: string
  quantity?: number | string
  codes_count?: number | string
  size?: string
  color?: string
}

type OrdersViewState = {
  queue?: OrderRow[]
  session_orders?: OrderRow[]
  history?: OrderRow[]
  deleted_orders?: OrderRow[]
}

type OrderForm = {
  mode: 'params' | 'gtin'
  order_name: string
  name: string
  size: string
  units_per_pack: string
  color: string
  venchik: string
  gtin: string
  codes_count: string
}

const emptyForm = (): OrderForm => ({
  mode: 'params',
  order_name: '',
  name: '',
  size: '',
  units_per_pack: '',
  color: '',
  venchik: '',
  gtin: '',
  codes_count: '',
})

function rowTitle(item: OrderRow) {
  return item.order_name || item.name || item.simpl_name || item.document_id || 'Без названия'
}

function rowMeta(item: OrderRow) {
  const parts = [item.gtin, item.size, item.color, item.codes_count ?? item.quantity]
    .map((value) => String(value || '').trim())
    .filter(Boolean)
  return parts.join(' · ')
}

export function OrdersPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState(false)
  const [state, setState] = useState<OrdersViewState>({})
  const [form, setForm] = useState<OrderForm>(emptyForm)

  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<OrdersViewState>('get_orders_view_state', force)
      setState(result ?? {})
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить заказы'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(false)
  }, [load])

  const setField = <K extends keyof OrderForm>(key: K, value: OrderForm[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }))
  }

  const addToQueue = async () => {
    setBusy(true)
    try {
      const payload = {
        mode: form.mode,
        order_name: form.order_name.trim(),
        name: form.name.trim(),
        size: form.size.trim(),
        units_per_pack: form.units_per_pack.trim(),
        color: form.color.trim(),
        venchik: form.venchik.trim(),
        gtin: form.gtin.trim(),
        codes_count: Number(form.codes_count || 0),
      }
      const result = await apiCall<{ success?: boolean; error?: string; queue?: OrderRow[] }>(
        'add_order_item',
        payload,
      )
      if (!result?.success) {
        throw new Error(result?.error || 'Не удалось добавить в очередь')
      }
      if (result.queue) {
        setState((prev) => ({ ...prev, queue: result.queue }))
      } else {
        await load(true)
      }
      setForm((prev) => ({ ...emptyForm(), mode: prev.mode }))
      toast.success('Добавлено в очередь')
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка добавления'))
    } finally {
      setBusy(false)
    }
  }

  const removeFromQueue = async (uid: string) => {
    setBusy(true)
    try {
      const result = await apiCall<{ success?: boolean; error?: string; queue?: OrderRow[] }>(
        'remove_order_item',
        uid,
      )
      if (!result?.success) {
        throw new Error(result?.error || 'Не удалось удалить из очереди')
      }
      setState((prev) => ({ ...prev, queue: result.queue ?? [] }))
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка удаления из очереди'))
    } finally {
      setBusy(false)
    }
  }

  const clearQueue = async () => {
    setBusy(true)
    try {
      const result = await apiCall<{ success?: boolean; error?: string }>('clear_order_queue')
      if (!result?.success) {
        throw new Error(result?.error || 'Не удалось очистить очередь')
      }
      setState((prev) => ({ ...prev, queue: [] }))
      toast.success('Очередь очищена')
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка очистки'))
    } finally {
      setBusy(false)
    }
  }

  const submitQueue = async () => {
    setBusy(true)
    try {
      const result = await apiCall<{ success?: boolean; error?: string; message?: string }>(
        'submit_order_queue',
      )
      if (!result?.success) {
        throw new Error(result?.error || 'Не удалось отправить очередь')
      }
      toast.success(result.message || 'Очередь отправлена')
      await load(true)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка отправки'))
    } finally {
      setBusy(false)
    }
  }

  const deleteHistoryOrder = async (documentId: string) => {
    setBusy(true)
    try {
      const result = await apiCall<{ success?: boolean; error?: string }>('delete_order', documentId)
      if (!result?.success) {
        throw new Error(result?.error || 'Не удалось удалить заказ')
      }
      toast.success('Заказ перемещён в удалённые')
      await load(true)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка удаления'))
    } finally {
      setBusy(false)
    }
  }

  const restoreOrder = async (documentId: string) => {
    setBusy(true)
    try {
      const result = await apiCall<{ success?: boolean; error?: string }>(
        'restore_deleted_order',
        documentId,
      )
      if (!result?.success) {
        throw new Error(result?.error || 'Не удалось восстановить заказ')
      }
      toast.success('Заказ восстановлен')
      await load(true)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка восстановления'))
    } finally {
      setBusy(false)
    }
  }

  const history = state.history ?? []
  const queue = state.queue ?? []
  const sessionOrders = state.session_orders ?? []
  const deleted = state.deleted_orders ?? []

  return (
    <div className="page-shell space-y-4">
      <PageHeader
        title="Заказ кодов"
        subtitle="Очередь, отправка в Контур.Маркировку и история документов."
        actions={
          <Button variant="outline" size="sm" onClick={() => void load(true)} disabled={loading || busy}>
            <RefreshCw className={cn('h-3.5 w-3.5', loading && 'animate-spin')} />
            Обновить
          </Button>
        }
      />

      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Очередь" value={queue.length} />
        <StatPill label="Сессия" value={sessionOrders.length} />
        <StatPill label="История" value={history.length} />
        <StatPill label="Удалённые" value={deleted.length} />
      </div>

      <div className="grid gap-4 xl:grid-cols-[340px_minmax(0,1fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Новая позиция</CardTitle>
            <CardDescription>Добавление в очередь перед отправкой.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex gap-2">
              <Button
                type="button"
                size="sm"
                variant={form.mode === 'params' ? 'default' : 'outline'}
                onClick={() => setField('mode', 'params')}
              >
                По параметрам
              </Button>
              <Button
                type="button"
                size="sm"
                variant={form.mode === 'gtin' ? 'default' : 'outline'}
                onClick={() => setField('mode', 'gtin')}
              >
                По GTIN
              </Button>
            </div>

            <label className="block space-y-1 text-sm">
              <span className="text-muted-foreground">Название заявки</span>
              <input
                className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                value={form.order_name}
                onChange={(event) => setField('order_name', event.target.value)}
              />
            </label>

            {form.mode === 'gtin' ? (
              <label className="block space-y-1 text-sm">
                <span className="text-muted-foreground">GTIN</span>
                <input
                  className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                  value={form.gtin}
                  onChange={(event) => setField('gtin', event.target.value)}
                />
              </label>
            ) : (
              <>
                <label className="block space-y-1 text-sm">
                  <span className="text-muted-foreground">Наименование</span>
                  <input
                    className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                    value={form.name}
                    onChange={(event) => setField('name', event.target.value)}
                  />
                </label>
                <div className="grid grid-cols-2 gap-2">
                  <label className="block space-y-1 text-sm">
                    <span className="text-muted-foreground">Размер</span>
                    <input
                      className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                      value={form.size}
                      onChange={(event) => setField('size', event.target.value)}
                    />
                  </label>
                  <label className="block space-y-1 text-sm">
                    <span className="text-muted-foreground">Ед. в упаковке</span>
                    <input
                      className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                      value={form.units_per_pack}
                      onChange={(event) => setField('units_per_pack', event.target.value)}
                    />
                  </label>
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <label className="block space-y-1 text-sm">
                    <span className="text-muted-foreground">Цвет</span>
                    <input
                      className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                      value={form.color}
                      onChange={(event) => setField('color', event.target.value)}
                    />
                  </label>
                  <label className="block space-y-1 text-sm">
                    <span className="text-muted-foreground">Венчик</span>
                    <input
                      className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                      value={form.venchik}
                      onChange={(event) => setField('venchik', event.target.value)}
                    />
                  </label>
                </div>
              </>
            )}

            <label className="block space-y-1 text-sm">
              <span className="text-muted-foreground">Количество кодов</span>
              <input
                type="number"
                min={1}
                className="thin-border-focus w-full rounded-md bg-background px-3 py-2"
                value={form.codes_count}
                onChange={(event) => setField('codes_count', event.target.value)}
              />
            </label>

            <Button className="w-full" disabled={busy} onClick={() => void addToQueue()}>
              <Plus className="h-3.5 w-3.5" />
              В очередь
            </Button>
          </CardContent>
        </Card>

        <div className="space-y-4">
          <Card>
            <CardHeader className="flex-row items-start justify-between space-y-0">
              <div>
                <CardTitle>Очередь</CardTitle>
                <CardDescription>Позиции к отправке в Контур.</CardDescription>
              </div>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={busy || queue.length === 0}
                  onClick={() => void clearQueue()}
                >
                  Очистить
                </Button>
                <Button size="sm" disabled={busy || queue.length === 0} onClick={() => void submitQueue()}>
                  <Send className="h-3.5 w-3.5" />
                  Отправить
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {queue.length === 0 ? (
                <EmptyState>Очередь пуста — добавьте позицию слева.</EmptyState>
              ) : (
                <ul className="divide-y divide-border/80">
                  {queue.map((item) => (
                    <li key={item.uid || rowTitle(item)} className="flex items-start justify-between gap-3 py-3">
                      <div className="min-w-0">
                        <div className="truncate font-medium">{rowTitle(item)}</div>
                        <div className="truncate text-xs text-muted-foreground">{rowMeta(item)}</div>
                      </div>
                      <Button
                        size="sm"
                        variant="ghost"
                        disabled={busy || !item.uid}
                        onClick={() => item.uid && void removeFromQueue(item.uid)}
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </Button>
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Сессия</CardTitle>
              <CardDescription>Заказы, созданные в текущем запуске.</CardDescription>
            </CardHeader>
            <CardContent>
              {sessionOrders.length === 0 ? (
                <EmptyState>Пока нет заказов сессии.</EmptyState>
              ) : (
                <ul className="divide-y divide-border/80">
                  {sessionOrders.slice(0, 40).map((item) => (
                    <li key={item.document_id || rowTitle(item)} className="flex items-center justify-between gap-3 py-3">
                      <div className="min-w-0">
                        <div className="truncate font-medium">{rowTitle(item)}</div>
                        <div className="truncate text-xs text-muted-foreground">{rowMeta(item)}</div>
                      </div>
                      {item.status ? <Badge tone="neutral">{item.status}</Badge> : null}
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>История</CardTitle>
            <CardDescription>Документы из общей истории заказов.</CardDescription>
          </CardHeader>
          <CardContent>
            {history.length === 0 ? (
              <EmptyState>История пуста.</EmptyState>
            ) : (
              <ul className="max-h-[420px] divide-y divide-border/80 overflow-auto">
                {history.slice(0, 80).map((item) => (
                  <li key={item.document_id || rowTitle(item)} className="flex items-start justify-between gap-3 py-3">
                    <div className="min-w-0">
                      <div className="truncate font-medium">{rowTitle(item)}</div>
                      <div className="truncate text-xs text-muted-foreground">{rowMeta(item)}</div>
                    </div>
                    <div className="flex shrink-0 items-center gap-2">
                      {item.status ? <Badge tone="info">{item.status}</Badge> : null}
                      <Button
                        size="sm"
                        variant="ghost"
                        disabled={busy || !item.document_id}
                        onClick={() => item.document_id && void deleteHistoryOrder(item.document_id)}
                      >
                        <Trash2 className="h-3.5 w-3.5" />
                      </Button>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Удалённые</CardTitle>
            <CardDescription>Архив мягкого удаления.</CardDescription>
          </CardHeader>
          <CardContent>
            {deleted.length === 0 ? (
              <EmptyState>Удалённых заказов нет.</EmptyState>
            ) : (
              <ul className="max-h-[420px] divide-y divide-border/80 overflow-auto">
                {deleted.slice(0, 80).map((item) => (
                  <li key={item.document_id || rowTitle(item)} className="flex items-start justify-between gap-3 py-3">
                    <div className="min-w-0">
                      <div className="truncate font-medium">{rowTitle(item)}</div>
                      <div className="truncate text-xs text-muted-foreground">{rowMeta(item)}</div>
                    </div>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={busy || !item.document_id}
                      onClick={() => item.document_id && void restoreOrder(item.document_id)}
                    >
                      <RotateCcw className="h-3.5 w-3.5" />
                      Вернуть
                    </Button>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type OrderRow = {
  document_id?: string
  order_name?: string
  name?: string
  status?: string
  gtin?: string
  quantity?: number | string
}

type OrdersViewState = {
  queue?: OrderRow[]
  session_orders?: OrderRow[]
  history?: OrderRow[]
  deleted_orders?: OrderRow[]
}

function rowTitle(item: OrderRow) {
  return item.order_name || item.name || item.document_id || 'Без названия'
}

export function OrdersPage() {
  const [loading, setLoading] = useState(false)
  const [state, setState] = useState<OrdersViewState>({})

  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<OrdersViewState>('get_orders_view_state', force)
      setState(result)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить заказы'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(false)
  }, [load])

  const history = state.history ?? []
  const queue = state.queue ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Заказ кодов"
        subtitle="Очередь заказов и история документов Контур.Маркировка."
        actions={
          <Button variant="outline" size="sm" onClick={() => void load(true)} disabled={loading}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Очередь" value={queue.length} />
        <StatPill label="Сессия" value={(state.session_orders ?? []).length} />
        <StatPill label="История" value={history.length} />
        <StatPill label="Удалённые" value={(state.deleted_orders ?? []).length} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Очередь</CardTitle>
            <CardDescription>Позиции, подготовленные к отправке.</CardDescription>
          </CardHeader>
          <CardContent>
            {queue.length === 0 ? (
              <EmptyState>Очередь пуста</EmptyState>
            ) : (
              <div className="max-h-[420px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Название</th>
                      <th className="px-2 py-2 font-medium">GTIN</th>
                      <th className="px-2 py-2 font-medium">Кол-во</th>
                    </tr>
                  </thead>
                  <tbody>
                    {queue.slice(0, 50).map((item, index) => (
                      <tr key={item.document_id || `${rowTitle(item)}-${index}`} className="border-b border-border/70">
                        <td className="px-2 py-2 font-medium">{rowTitle(item)}</td>
                        <td className="px-2 py-2 font-mono text-xs text-muted-foreground">{item.gtin || '—'}</td>
                        <td className="px-2 py-2 tabular-nums">{item.quantity ?? '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>История</CardTitle>
            <CardDescription>Последние заказы из Контура.</CardDescription>
          </CardHeader>
          <CardContent>
            {history.length === 0 ? (
              <EmptyState>История пока пуста</EmptyState>
            ) : (
              <div className="max-h-[420px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Документ</th>
                      <th className="px-2 py-2 font-medium">Статус</th>
                    </tr>
                  </thead>
                  <tbody>
                    {history.slice(0, 50).map((item, index) => (
                      <tr key={item.document_id || `${rowTitle(item)}-${index}`} className="border-b border-border/70">
                        <td className="px-2 py-2">
                          <div className="font-medium">{rowTitle(item)}</div>
                          <div className="font-mono text-[11px] text-muted-foreground">{item.document_id || '—'}</div>
                        </td>
                        <td className="px-2 py-2">
                          <Badge tone="secondary">{item.status || '—'}</Badge>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

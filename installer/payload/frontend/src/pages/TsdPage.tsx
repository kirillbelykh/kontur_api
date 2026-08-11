import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type TsdItem = {
  document_id?: string
  order_name?: string
  name?: string
  status?: string
  intro_number?: string
  tsd_status?: string
}

type TsdState = {
  items?: TsdItem[]
  live?: boolean
}

export function TsdPage() {
  const [loading, setLoading] = useState(false)
  const [items, setItems] = useState<TsdItem[]>([])
  const [live, setLive] = useState(false)

  const load = useCallback(async (useLive = false) => {
    setLoading(true)
    try {
      const result = await apiCall<TsdState>('get_tsd_state', useLive)
      setItems(result.items ?? [])
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

  return (
    <div className="page-shell">
      <PageHeader
        title="Задание на ТСД"
        subtitle="Создание и контроль заданий на терминал сбора данных."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void load(false)} disabled={loading}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button size="sm" onClick={() => void load(true)} disabled={loading}>
              Live-статусы
            </Button>
          </>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-3">
        <StatPill label="Документов" value={items.length} />
        <StatPill label="Режим" value={live ? 'Live' : 'Кэш'} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Задания</CardTitle>
          <CardDescription>Документы, доступные для создания заданий на ТСД.</CardDescription>
        </CardHeader>
        <CardContent>
          {items.length === 0 ? (
            <EmptyState>Нет заданий</EmptyState>
          ) : (
            <div className="max-h-[520px] overflow-auto">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                  <tr className="border-b border-border">
                    <th className="px-2 py-2 font-medium">Заказ</th>
                    <th className="px-2 py-2 font-medium">Статус</th>
                    <th className="px-2 py-2 font-medium">Ввод</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((item) => (
                    <tr key={item.document_id} className="border-b border-border/70">
                      <td className="px-2 py-2">
                        <div className="font-medium">{item.order_name || item.name || item.document_id}</div>
                        <div className="font-mono text-[11px] text-muted-foreground">{item.document_id}</div>
                      </td>
                      <td className="px-2 py-2">
                        <Badge tone="secondary">{item.tsd_status || item.status || '—'}</Badge>
                      </td>
                      <td className="px-2 py-2 font-mono text-xs text-muted-foreground">
                        {item.intro_number || '—'}
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
  )
}

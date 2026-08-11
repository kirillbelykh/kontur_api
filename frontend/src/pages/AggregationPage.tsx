import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type AggregationItem = {
  document_id?: string
  comment?: string
  status?: string
  codes_count?: number | string
  created_at?: string
}

type AggregationState = {
  items?: AggregationItem[]
  status_options?: string[]
  cache_age_seconds?: number
  total_items?: number
}

export function AggregationPage() {
  const [loading, setLoading] = useState(false)
  const [state, setState] = useState<AggregationState>({})

  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<AggregationState>('get_aggregation_state', force)
      setState(result)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить агрегацию'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(false)
  }, [load])

  const items = state.items ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Коды агрегации"
        subtitle="Создание, скачивание и статус кодов агрегации."
        actions={
          <Button variant="outline" size="sm" onClick={() => void load(true)} disabled={loading}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Всего" value={state.total_items ?? items.length} />
        <StatPill label="На экране" value={items.length} />
        <StatPill label="Статусы" value={(state.status_options ?? []).length} />
        <StatPill
          label="Возраст кэша"
          value={
            typeof state.cache_age_seconds === 'number'
              ? `${Math.round(state.cache_age_seconds)} с`
              : '—'
          }
        />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Документы агрегации</CardTitle>
          <CardDescription>Список из Контура (кэш с фоновым обновлением).</CardDescription>
        </CardHeader>
        <CardContent>
          {items.length === 0 ? (
            <EmptyState>Нет документов агрегации</EmptyState>
          ) : (
            <div className="max-h-[520px] overflow-auto">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                  <tr className="border-b border-border">
                    <th className="px-2 py-2 font-medium">Документ</th>
                    <th className="px-2 py-2 font-medium">Статус</th>
                    <th className="px-2 py-2 font-medium">Коды</th>
                  </tr>
                </thead>
                <tbody>
                  {items.slice(0, 100).map((item) => (
                    <tr key={item.document_id} className="border-b border-border/70">
                      <td className="px-2 py-2">
                        <div className="font-medium">{item.comment || item.document_id}</div>
                        <div className="font-mono text-[11px] text-muted-foreground">{item.document_id}</div>
                      </td>
                      <td className="px-2 py-2">
                        <Badge tone="secondary">{item.status || '—'}</Badge>
                      </td>
                      <td className="px-2 py-2 tabular-nums">{item.codes_count ?? '—'}</td>
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

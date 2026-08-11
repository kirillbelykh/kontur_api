import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type ChzRequest = {
  request_id?: string | number
  order_name?: string
  items_summary?: string
  status?: string
  created_at?: string
}

type ChzViewState = {
  new_requests?: ChzRequest[]
  in_progress?: ChzRequest[]
  archive?: ChzRequest[]
  counts?: { new?: number; in_progress?: number; archive?: number }
}

function toneForStatus(status?: string) {
  switch (status) {
    case 'ready':
      return 'success' as const
    case 'acknowledged':
      return 'info' as const
    case 'requested':
      return 'warning' as const
    case 'cancelled':
      return 'danger' as const
    default:
      return 'neutral' as const
  }
}

function RequestList({ title, items }: { title: string; items: ChzRequest[] }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{items.length} запрос(ов)</CardDescription>
      </CardHeader>
      <CardContent>
        {items.length === 0 ? (
          <EmptyState>Нет записей</EmptyState>
        ) : (
          <div className="max-h-[360px] space-y-2 overflow-auto">
            {items.slice(0, 40).map((item) => (
              <div
                key={String(item.request_id)}
                className="flex items-start justify-between gap-3 rounded-md border border-border px-3 py-2"
              >
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium">
                    {item.order_name || item.items_summary || `Запрос ${item.request_id}`}
                  </div>
                  <div className="truncate text-xs text-muted-foreground">
                    ID {item.request_id ?? '—'} · {item.created_at || 'без даты'}
                  </div>
                </div>
                <Badge tone={toneForStatus(item.status)}>{item.status || '—'}</Badge>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

export function ChzPage() {
  const [loading, setLoading] = useState(false)
  const [state, setState] = useState<ChzViewState>({})

  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<ChzViewState>('get_chz_requests_view_state', force)
      setState(result)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить запросы ЧЗ'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(false)
  }, [load])

  const neu = state.new_requests ?? []
  const work = state.in_progress ?? []
  const archive = state.archive ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Запросы ЧЗ"
        subtitle="Запросы Honest Sign из WMS: новые, в работе и архив."
        actions={
          <Button variant="outline" size="sm" onClick={() => void load(true)} disabled={loading}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Синхронизировать
          </Button>
        }
      />

      <div className="mb-4 grid grid-cols-3 gap-2">
        <StatPill label="Новые" value={state.counts?.new ?? neu.length} />
        <StatPill label="В работе" value={state.counts?.in_progress ?? work.length} />
        <StatPill label="Архив" value={state.counts?.archive ?? archive.length} />
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        <RequestList title="Новые" items={neu} />
        <RequestList title="В работе" items={work} />
        <RequestList title="Архив" items={archive} />
      </div>
    </div>
  )
}

import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type IntroItem = {
  document_id?: string
  order_name?: string
  name?: string
  status?: string
  intro_status?: string
  production_date?: string
  expiration_date?: string
}

type IntroState = {
  items?: IntroItem[]
}

export function IntroPage() {
  const [loading, setLoading] = useState(false)
  const [items, setItems] = useState<IntroItem[]>([])

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const result = await apiCall<IntroState>('get_intro_state')
      setItems(result.items ?? [])
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить ввод в оборот'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  return (
    <div className="page-shell">
      <PageHeader
        title="Ввод в оборот"
        subtitle="Документы для ввода кодов маркировки в оборот."
        actions={
          <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <div className="mb-4">
        <StatPill label="Документов" value={items.length} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Список</CardTitle>
          <CardDescription>Готовые к вводу и уже обработанные позиции.</CardDescription>
        </CardHeader>
        <CardContent>
          {items.length === 0 ? (
            <EmptyState>Нет документов для ввода в оборот</EmptyState>
          ) : (
            <div className="max-h-[520px] overflow-auto">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                  <tr className="border-b border-border">
                    <th className="px-2 py-2 font-medium">Заказ</th>
                    <th className="px-2 py-2 font-medium">Статус</th>
                    <th className="px-2 py-2 font-medium">Даты</th>
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
                        <Badge tone="secondary">{item.intro_status || item.status || '—'}</Badge>
                      </td>
                      <td className="px-2 py-2 text-xs text-muted-foreground">
                        {item.production_date || '—'} → {item.expiration_date || '—'}
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

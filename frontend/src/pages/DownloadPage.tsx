import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type DownloadItem = {
  document_id?: string
  order_name?: string
  name?: string
  status?: string
  codes_path?: string
  file_label?: string
}

type DownloadState = {
  items?: DownloadItem[]
  printers?: string[]
  default_printer?: string
}

export function DownloadPage() {
  const [loading, setLoading] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [state, setState] = useState<DownloadState>({})

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const result = await apiCall<DownloadState>('get_download_state')
      setState(result)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить список загрузок'))
    } finally {
      setLoading(false)
    }
  }, [])

  const syncStatuses = useCallback(async () => {
    setSyncing(true)
    try {
      await apiCall('sync_download_statuses', true)
      toast.success('Статусы синхронизированы')
      await load()
    } catch (error) {
      toast.error(getErrorMessage(error, 'Ошибка синхронизации статусов'))
    } finally {
      setSyncing(false)
    }
  }, [load])

  useEffect(() => {
    void load()
  }, [load])

  const items = state.items ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Загрузка кодов"
        subtitle="Скачивание и статусы заказов маркировки. Принтер по умолчанию для печати DataMatrix."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button size="sm" onClick={() => void syncStatuses()} disabled={syncing}>
              Синхронизировать статусы
            </Button>
          </>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-3">
        <StatPill label="Документов" value={items.length} />
        <StatPill label="Принтеры" value={(state.printers ?? []).length} />
        <StatPill label="По умолчанию" value={state.default_printer || '—'} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Документы</CardTitle>
          <CardDescription>Готовые к загрузке и уже скачанные заказы.</CardDescription>
        </CardHeader>
        <CardContent>
          {items.length === 0 ? (
            <EmptyState>Нет документов для загрузки</EmptyState>
          ) : (
            <div className="max-h-[520px] overflow-auto">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                  <tr className="border-b border-border">
                    <th className="px-2 py-2 font-medium">Заказ</th>
                    <th className="px-2 py-2 font-medium">Статус</th>
                    <th className="px-2 py-2 font-medium">Файл</th>
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
                        <Badge tone="secondary">{item.status || '—'}</Badge>
                      </td>
                      <td className="px-2 py-2 text-xs text-muted-foreground">
                        {item.file_label || item.codes_path || '—'}
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

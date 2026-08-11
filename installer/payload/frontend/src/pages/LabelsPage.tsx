import { useCallback, useEffect, useState } from 'react'
import { RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'

type SheetFormat = { key?: string; label?: string }
type TemplateItem = { key?: string; name?: string; path?: string }
type FileItem = { name?: string; path?: string }

type LabelsState = {
  sheet_formats?: SheetFormat[]
  default_sheet_format?: string
  templates?: TemplateItem[]
  aggregation_files?: FileItem[]
  marking_files?: FileItem[]
  orders?: Array<{ document_id?: string; order_name?: string }>
  printers?: string[]
  default_printer?: string
}

export function LabelsPage() {
  const [loading, setLoading] = useState(false)
  const [state, setState] = useState<LabelsState>({})

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const result = await apiCall<LabelsState>('get_labels_state')
      setState(result)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить состояние этикеток'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  const templates = state.templates ?? []
  const formats = state.sheet_formats ?? []
  const printers = state.printers ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Печать этикеток"
        subtitle="Шаблоны BarTender 100×180 / 100×136, принтеры и файлы кодов."
        actions={
          <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Форматы" value={formats.length} />
        <StatPill label="Шаблоны" value={templates.length} />
        <StatPill label="Принтеры" value={printers.length} />
        <StatPill label="По умолчанию" value={state.default_printer || '—'} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Форматы листа</CardTitle>
            <CardDescription>
              По умолчанию: {state.default_sheet_format || '100x180'}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {formats.length === 0 ? (
              <EmptyState>Форматы не найдены</EmptyState>
            ) : (
              <div className="flex flex-wrap gap-2">
                {formats.map((format) => (
                  <Badge key={format.key || format.label} tone="secondary">
                    {format.label || format.key}
                  </Badge>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Принтеры</CardTitle>
            <CardDescription>Доступные устройства печати.</CardDescription>
          </CardHeader>
          <CardContent>
            {printers.length === 0 ? (
              <EmptyState>Принтеры не обнаружены</EmptyState>
            ) : (
              <ul className="max-h-48 space-y-1 overflow-auto text-sm">
                {printers.map((printer) => (
                  <li key={printer} className="rounded-md border border-border px-3 py-1.5">
                    {printer}
                    {printer === state.default_printer ? (
                      <span className="ml-2 text-xs text-muted-foreground">по умолчанию</span>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Шаблоны</CardTitle>
            <CardDescription>BarTender .btw из assets/labels.</CardDescription>
          </CardHeader>
          <CardContent>
            {templates.length === 0 ? (
              <EmptyState>Шаблоны не найдены</EmptyState>
            ) : (
              <div className="max-h-[320px] overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead className="sticky top-0 bg-card text-[11px] uppercase tracking-wide text-muted-foreground">
                    <tr className="border-b border-border">
                      <th className="px-2 py-2 font-medium">Название</th>
                      <th className="px-2 py-2 font-medium">Ключ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {templates.slice(0, 80).map((template) => (
                      <tr key={template.key || template.path || template.name} className="border-b border-border/70">
                        <td className="px-2 py-2 font-medium">{template.name || template.path || '—'}</td>
                        <td className="px-2 py-2 font-mono text-xs text-muted-foreground">
                          {template.key || '—'}
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

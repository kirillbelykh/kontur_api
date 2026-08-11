import { useCallback, useEffect, useMemo, useState, type MouseEvent } from 'react'
import { Archive, ArchiveRestore, CheckCheck, PlayCircle, RefreshCw, X } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

type ChzItem = {
  order_item_id?: string | number
  item_id?: string | number
  item_title?: string
  item_size?: string
  item_color?: string
  item_venchik?: string
  batch_number?: string
  pairs_quantity?: number
}

type ChzRequest = {
  request_key?: string
  request_id?: number
  order_name?: string
  order_number?: string
  customer?: string
  comment?: string
  request_type?: string
  type_label?: string
  author?: string
  status?: string
  status_label?: string
  is_active?: boolean
  requested_at?: string
  requested_at_label?: string
  updated_at_label?: string
  items?: ChzItem[]
  item_title?: string
  item_size?: string
  item_color?: string
  batch_number?: string
  items_summary?: string
  positions_count?: number
  pairs_total?: number
}

type ChzViewState = {
  new_requests?: ChzRequest[]
  in_progress?: ChzRequest[]
  archive?: ChzRequest[]
  counts?: { new?: number; in_progress?: number; archive?: number }
}

/** Бэкенд принимает и request_key, и числовой request_id; ключ точнее — он различает отгрузку и производство. */
function refOf(request: ChzRequest): string {
  return String(request.request_key || request.request_id || '')
}

function toneForStatus(status?: string) {
  switch (status) {
    case 'ready':
      return 'success' as const
    case 'acknowledged':
      return 'info' as const
    case 'requested':
      return 'warning' as const
    case 'archived':
      return 'secondary' as const
    case 'cancelled':
    case 'deleted':
      return 'danger' as const
    default:
      return 'neutral' as const
  }
}

function ChzTable({
  rows,
  ariaLabel,
  selected,
  onToggle,
  onToggleAll,
  onOpenDetails,
  timeLabel,
}: {
  rows: ChzRequest[]
  ariaLabel: string
  selected: Set<string>
  onToggle: (ref: string) => void
  onToggleAll: (refs: string[], nextSelected: boolean) => void
  onOpenDetails: (request: ChzRequest) => void
  timeLabel: string
}) {
  const refs = rows.map(refOf).filter(Boolean)
  const allSelected = refs.length > 0 && refs.every((ref) => selected.has(ref))

  /* React Aria отбрасывает onDoubleClick на Row, поэтому ловим двойной клик на обёртке и ищем строку по позиции в tbody. */
  const openDetailsFromEvent = (event: MouseEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement | null
    const rowElement = target?.closest('tbody tr')
    const body = rowElement?.parentElement
    if (!rowElement || !body) return
    const row = rows[Array.from(body.children).indexOf(rowElement)]
    if (row) onOpenDetails(row)
  }

  return (
    <div className="max-h-[360px] overflow-auto" onDoubleClick={openDetailsFromEvent}>
      <Table aria-label={ariaLabel}>
        <TableHeader>
          <TableRow>
            <TableHead>
              <Checkbox
                isSelected={allSelected}
                aria-label={allSelected ? 'Снять выделение' : 'Выделить видимые'}
                onChange={() => onToggleAll(refs, !allSelected)}
              />
            </TableHead>
            <TableHead>Тип</TableHead>
            <TableHead>Заказ №</TableHead>
            <TableHead>Автор</TableHead>
            <TableHead>Номенклатура</TableHead>
            <TableHead>Размер</TableHead>
            <TableHead>Партия</TableHead>
            <TableHead>Цвет</TableHead>
            <TableHead>Кол-во пар</TableHead>
            <TableHead>Статус</TableHead>
            <TableHead>{timeLabel}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row, index) => {
            const ref = refOf(row)
            const rowId = ref || `chz-row-${index}`
            const isSelected = selected.has(ref)
            return (
              <TableRow
                key={rowId}
                id={rowId}
                className={cn(isSelected && 'bg-muted/60')}
                onClick={() => onToggle(ref)}
                title="Клик — выбрать, двойной клик — подробнее"
              >
                <TableCell onClick={(event) => event.stopPropagation()}>
                  {/* Свой span: React Aria не пробрасывает onClick ячейки в DOM, а press строки нужно погасить до всплытия. */}
                  <span
                    className="inline-flex"
                    onPointerDown={(event) => event.stopPropagation()}
                    onClick={(event) => event.stopPropagation()}
                  >
                    <Checkbox
                      isSelected={isSelected}
                      aria-label={`Выбрать запрос ${row.order_number || row.request_id || ''}`}
                      onChange={() => onToggle(ref)}
                    />
                  </span>
                </TableCell>
                <TableCell>{row.type_label || '—'}</TableCell>
                <TableCell className="font-medium">{row.order_number || row.order_name || '—'}</TableCell>
                <TableCell className="text-muted-foreground">{row.author || 'WMS'}</TableCell>
                <TableCell>{row.item_title || '—'}</TableCell>
                <TableCell className="text-muted-foreground">{row.item_size || '—'}</TableCell>
                <TableCell className="text-muted-foreground">{row.batch_number || '—'}</TableCell>
                <TableCell className="text-muted-foreground">{row.item_color || '—'}</TableCell>
                <TableCell className="tabular-nums">{row.pairs_total ?? 0}</TableCell>
                <TableCell>
                  <Badge tone={toneForStatus(row.status)}>{row.status_label || row.status || '—'}</Badge>
                </TableCell>
                <TableCell className="text-xs text-muted-foreground">{row.requested_at_label || '—'}</TableCell>
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </div>
  )
}

export function ChzPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [state, setState] = useState<ChzViewState>({})
  const [selectedNew, setSelectedNew] = useState<Set<string>>(new Set())
  const [selectedWork, setSelectedWork] = useState<Set<string>>(new Set())
  const [selectedArchive, setSelectedArchive] = useState<Set<string>>(new Set())
  const [showArchive, setShowArchive] = useState(false)
  const [details, setDetails] = useState<ChzRequest | null>(null)

  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<ChzViewState>('get_chz_requests_view_state', force)
      setState(result)
      const keep = (rows: ChzRequest[] | undefined) => {
        const available = new Set((rows || []).map(refOf))
        return (prev: Set<string>) => new Set([...prev].filter((ref) => available.has(ref)))
      }
      setSelectedNew(keep(result.new_requests))
      setSelectedWork(keep(result.in_progress))
      setSelectedArchive(keep(result.archive))
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

  const selectedTotal = useMemo(
    () => selectedNew.size + selectedWork.size + selectedArchive.size,
    [selectedNew, selectedWork, selectedArchive],
  )

  const toggleIn =
    (setter: (updater: (prev: Set<string>) => Set<string>) => void) =>
    (ref: string) => {
      if (!ref) return
      setter((prev) => {
        const next = new Set(prev)
        if (next.has(ref)) next.delete(ref)
        else next.add(ref)
        return next
      })
    }

  const toggleAllIn =
    (setter: (updater: (prev: Set<string>) => Set<string>) => void) =>
    (refs: string[], nextSelected: boolean) => {
      setter((prev) => {
        const next = new Set(prev)
        refs.forEach((ref) => (nextSelected ? next.add(ref) : next.delete(ref)))
        return next
      })
    }

  const runBusy = async (key: string, action: () => Promise<void>, successMessage: string) => {
    setBusy(key)
    try {
      await action()
      toast.success(successMessage)
    } catch (error) {
      toast.error(getErrorMessage(error))
    } finally {
      setBusy(null)
    }
  }

  const refresh = () =>
    runBusy('refresh', async () => {
      await load(true)
    }, 'Запросы ЧЗ обновлены.')

  const takeToWork = () =>
    runBusy(
      'take',
      async () => {
        const refs = [...selectedNew]
        if (!refs.length) throw new Error('Выберите новые запросы ЧЗ.')
        await apiCall('acknowledge_wms_chz_requests', refs)
        setSelectedNew(new Set())
        await load(true)
      },
      'Запросы ЧЗ взяты в работу.',
    )

  const markReady = () =>
    runBusy(
      'ready',
      async () => {
        const refs = [...selectedWork]
        if (!refs.length) throw new Error('Выберите запросы ЧЗ в работе.')
        await apiCall('mark_wms_chz_requests_ready', refs)
        setSelectedWork(new Set())
        await load(true)
      },
      'WMS уведомлена о готовности кодов.',
    )

  const archiveSelected = () =>
    runBusy(
      'archive',
      async () => {
        const refs = [...selectedNew, ...selectedWork, ...selectedArchive]
        if (!refs.length) throw new Error('Выберите запросы ЧЗ.')
        await apiCall('archive_wms_chz_requests', refs)
        setSelectedNew(new Set())
        setSelectedWork(new Set())
        setSelectedArchive(new Set())
        await load(true)
      },
      'Запросы ЧЗ перенесены в архив.',
    )

  const restoreSelected = () =>
    runBusy(
      'restore',
      async () => {
        const refs = [...selectedArchive]
        if (!refs.length) throw new Error('Выберите архивные запросы ЧЗ.')
        await apiCall('restore_wms_chz_requests', refs)
        setSelectedArchive(new Set())
        await load(true)
      },
      'Запросы ЧЗ возвращены из архива.',
    )

  const isBusy = Boolean(busy) || loading
  const detailItems = details?.items ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Запросы ЧЗ"
        subtitle="Запросы Honest Sign из WMS: новые, в работе и архив."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void refresh()} disabled={isBusy}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button size="sm" onClick={() => void takeToWork()} disabled={isBusy || selectedNew.size === 0}>
              <PlayCircle className="h-3.5 w-3.5" />
              Взять в работу
            </Button>
            <Button variant="success" size="sm" onClick={() => void markReady()} disabled={isBusy || selectedWork.size === 0}>
              <CheckCheck className="h-3.5 w-3.5" />
              Коды готовы
            </Button>
            <Button variant="outline" size="sm" onClick={() => void archiveSelected()} disabled={isBusy || selectedTotal === 0}>
              <Archive className="h-3.5 w-3.5" />В архив
            </Button>
            <Button variant="ghost" size="sm" onClick={() => setShowArchive((value) => !value)}>
              {showArchive ? 'Скрыть архив' : 'Архив'}
            </Button>
            {showArchive ? (
              <Button
                variant="outline"
                size="sm"
                onClick={() => void restoreSelected()}
                disabled={isBusy || selectedArchive.size === 0}
              >
                <ArchiveRestore className="h-3.5 w-3.5" />
                Вернуть
              </Button>
            ) : null}
          </>
        }
      />

      <div className="mb-2 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Новых" value={state.counts?.new ?? neu.length} />
        <StatPill label="В работе" value={state.counts?.in_progress ?? work.length} />
        <StatPill label="Архив" value={state.counts?.archive ?? archive.length} />
        <StatPill label="Выбрано" value={selectedTotal} />
      </div>

      <p className="mb-4 text-xs text-muted-foreground">
        {`Новых: ${neu.length} • В работе: ${work.length} • Архив: ${archive.length} • Выбрано: ${selectedTotal}`}
      </p>

      <div className="space-y-4">
        <Card>
          <CardHeader>
            <CardTitle>Новые запросы</CardTitle>
            <CardDescription>Клик по строке выбирает запрос, двойной клик открывает подробности.</CardDescription>
          </CardHeader>
          <CardContent>
            {neu.length === 0 ? (
              <EmptyState>Новых запросов ЧЗ нет</EmptyState>
            ) : (
              <ChzTable
                rows={neu}
                ariaLabel="Новые запросы ЧЗ"
                selected={selectedNew}
                onToggle={toggleIn(setSelectedNew)}
                onToggleAll={toggleAllIn(setSelectedNew)}
                onOpenDetails={setDetails}
                timeLabel="Время"
              />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Запросы в работе</CardTitle>
            <CardDescription>Приняты оператором — после подготовки кодов отметьте готовность.</CardDescription>
          </CardHeader>
          <CardContent>
            {work.length === 0 ? (
              <EmptyState>Запросов в работе нет</EmptyState>
            ) : (
              <ChzTable
                rows={work}
                ariaLabel="Запросы ЧЗ в работе"
                selected={selectedWork}
                onToggle={toggleIn(setSelectedWork)}
                onToggleAll={toggleAllIn(setSelectedWork)}
                onOpenDetails={setDetails}
                timeLabel="Время"
              />
            )}
          </CardContent>
        </Card>

        {showArchive ? (
          <Card>
            <CardHeader>
              <CardTitle>Архив запросов</CardTitle>
              <CardDescription>Закрытые и архивные запросы — можно вернуть в работу.</CardDescription>
            </CardHeader>
            <CardContent>
              {archive.length === 0 ? (
                <EmptyState>Архив пуст</EmptyState>
              ) : (
                <ChzTable
                  rows={archive}
                  ariaLabel="Архив запросов ЧЗ"
                  selected={selectedArchive}
                  onToggle={toggleIn(setSelectedArchive)}
                  onToggleAll={toggleAllIn(setSelectedArchive)}
                  onOpenDetails={setDetails}
                  timeLabel="Время"
                />
              )}
            </CardContent>
          </Card>
        ) : null}
      </div>

      {details ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/40 p-4"
          onClick={() => setDetails(null)}
        >
          <div
            className="max-h-[85vh] w-full max-w-2xl overflow-hidden rounded-lg border border-border bg-card shadow-panel"
            onClick={(event) => event.stopPropagation()}
            role="dialog"
            aria-modal="true"
          >
            <div className="flex items-start justify-between gap-3 border-b border-border px-4 py-3">
              <div>
                <h2 className="text-base font-semibold">
                  {`Запрос ЧЗ ${details.order_name || `#${details.request_id ?? ''}`}`}
                </h2>
                <p className="text-xs text-muted-foreground">
                  {`${details.type_label || 'Запрос'} • ${details.status_label || details.status || 'Статус не указан'}`}
                </p>
              </div>
              <Button size="icon" variant="ghost" onClick={() => setDetails(null)} aria-label="Закрыть">
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="max-h-[70vh] space-y-4 overflow-auto px-4 py-3">
              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Основное</h3>
                <dl className="grid gap-2 sm:grid-cols-2">
                  {[
                    { label: 'Тип', value: details.type_label || '—' },
                    { label: 'Автор', value: details.author || 'WMS' },
                    {
                      label: 'Заказ №',
                      value: details.order_number || details.order_name || String(details.request_id ?? '—'),
                    },
                    { label: 'Статус', value: details.status_label || details.status || '—' },
                    { label: 'Время', value: details.requested_at_label || details.requested_at || '—' },
                  ].map((field) => (
                    <div key={field.label} className="rounded-md border border-border/80 px-3 py-2">
                      <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">{field.label}</dt>
                      <dd className="mt-0.5 break-words text-sm">{field.value}</dd>
                    </div>
                  ))}
                </dl>
              </section>

              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  Комментарий заказчика
                </h3>
                <div className="rounded-md border border-border/80 bg-muted/30 px-3 py-2 text-sm">
                  {details.comment || 'Комментарий не указан.'}
                </div>
              </section>

              <section>
                <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">Позиции</h3>
                {detailItems.length === 0 ? (
                  <EmptyState>Позиции не переданы</EmptyState>
                ) : (
                  <dl className="space-y-2">
                    {detailItems.map((item, index) => (
                      <div key={`${item.order_item_id ?? index}`} className="rounded-md border border-border/80 px-3 py-2">
                        <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">{`Позиция ${index + 1}`}</dt>
                        <dd className="mt-0.5 break-words text-sm">
                          {[
                            item.item_title || 'Номенклатура',
                            item.item_size ? `р. ${item.item_size}` : '',
                            item.batch_number ? `партия ${item.batch_number}` : '',
                            item.item_color || '',
                            item.item_venchik || '',
                            `${item.pairs_quantity || 0} пар`,
                          ]
                            .filter(Boolean)
                            .join(' • ')}
                        </dd>
                      </div>
                    ))}
                  </dl>
                )}
              </section>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

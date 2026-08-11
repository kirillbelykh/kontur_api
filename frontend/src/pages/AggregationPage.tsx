import { useCallback, useEffect, useMemo, useRef, useState, type InputHTMLAttributes, type ReactNode } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { ChevronDown, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DatePickerField } from '@/components/ui/date-picker'
import { Checkbox } from '@/components/ui/checkbox'
import { SelectNative } from '@/components/ui/select'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

type AggregationItem = {
  document_id?: string
  aggregate_code?: string
  comment?: string
  status?: string
  status_label?: string
  created_at?: string
  created_at_label?: string
  product_group?: string
  includes_units_count?: number
  codes_check_errors_count?: number
}

type StatusOption = { value?: string; label?: string }

type AggregationState = {
  items?: AggregationItem[]
  status_options?: StatusOption[]
  cache_age_seconds?: number
  total_items?: number
}

const PAGE_SIZE = 200

function toneForStatus(status?: string) {
  const value = (status || '').toLowerCase()
  if (!value) return 'secondary' as const
  if (value.includes('ошиб') || value.includes('fail') || value.includes('error')) return 'danger' as const
  if (value.includes('ожид') || value.includes('готов к') || value.includes('process')) return 'warning' as const
  if (value.includes('провед') || value.includes('зарегистр') || value.includes('approved')) return 'success' as const
  return 'info' as const
}

function FieldLabel({ children }: { children: ReactNode }) {
  return <label className="mb-1 block text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{children}</label>
}

function TextInput(props: InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={cn('input-thin h-9 w-full px-2.5 py-0 text-sm', props.className)} />
}

export function AggregationPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [state, setState] = useState<AggregationState>({})

  const [createComment, setCreateComment] = useState('')
  const [createCount, setCreateCount] = useState('1')

  const [commentFilter, setCommentFilter] = useState('')
  const [refillToken, setRefillToken] = useState('')
  const [productionDate, setProductionDate] = useState('')
  const [expirationDate, setExpirationDate] = useState('')
  const [batchNumber, setBatchNumber] = useState('')
  const [documentTitle, setDocumentTitle] = useState('')
  const [allowDisaggregate, setAllowDisaggregate] = useState(false)
  const [refillOpen, setRefillOpen] = useState(false)

  const [statusFilter, setStatusFilter] = useState('')
  const [searchQuery, setSearchQuery] = useState('')
  const [currentPage, setCurrentPage] = useState(0)
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [lastClickedIndex, setLastClickedIndex] = useState(-1)
  // TableRow отдаёт onAction без исходного события — модификатор снимаем в capture-фазе до действия строки
  const shiftPressedRef = useRef(false)
  const load = useCallback(async (force = false) => {
    setLoading(true)
    try {
      const result = await apiCall<AggregationState>('get_aggregation_state', force)
      setState(result)
      const known = new Set((result.items || []).map((item) => String(item.document_id || '')))
      setSelectedIds((prev) => new Set([...prev].filter((id) => known.has(id))))
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить агрегацию'))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(false)
    void apiCall<{ production_date?: string; expiration_date?: string }>('get_default_date_window')
      .then((window) => {
        setProductionDate((prev) => prev || String(window.production_date || ''))
        setExpirationDate((prev) => prev || String(window.expiration_date || ''))
      })
      .catch(() => null)
  }, [load])

  const items = useMemo(() => state.items ?? [], [state.items])

  const filteredItems = useMemo(() => {
    const query = searchQuery.trim().toLowerCase()
    const status = statusFilter.trim()
    return items.filter((item) => {
      if (status && item.status !== status) return false
      if (!query) return true
      const haystack = [item.aggregate_code, item.comment, item.status_label, item.created_at_label, item.document_id]
        .map((value) => String(value || '').toLowerCase())
        .join(' ')
      return haystack.includes(query)
    })
  }, [items, searchQuery, statusFilter])

  const totalPages = Math.max(1, Math.ceil(filteredItems.length / PAGE_SIZE))
  const page = Math.min(Math.max(0, currentPage), totalPages - 1)
  const pageStart = page * PAGE_SIZE
  const pageEnd = Math.min(pageStart + PAGE_SIZE, filteredItems.length)
  const pageRows = filteredItems.slice(pageStart, pageEnd)

  const isBusy = Boolean(busy)
  const hasSelection = selectedIds.size > 0

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

  const selectedIdList = () => {
    const ids = [...selectedIds]
    if (!ids.length) throw new Error('Выберите хотя бы один АК.')
    return ids
  }

  const toggleRow = (documentId: string, index: number) => {
    if (!documentId) return
    const shiftKey = shiftPressedRef.current
    setSelectedIds((prev) => {
      const next = new Set(prev)
      const selected = next.has(documentId)
      if (shiftKey && lastClickedIndex >= 0) {
        const start = Math.min(lastClickedIndex, index)
        const end = Math.max(lastClickedIndex, index)
        const shouldSelect = !selected
        filteredItems.slice(start, end + 1).forEach((row) => {
          const id = String(row.document_id || '')
          if (!id) return
          if (shouldSelect) next.add(id)
          else next.delete(id)
        })
      } else if (selected) {
        next.delete(documentId)
      } else {
        next.add(documentId)
      }
      return next
    })
    setLastClickedIndex(index)
  }

  const selectVisible = () => {
    setSelectedIds(new Set(filteredItems.map((item) => String(item.document_id || '')).filter(Boolean)))
  }

  const selectByName = () => {
    const selectedRow = items.find((item) => selectedIds.has(String(item.document_id || '')))
    const fallbackRow = filteredItems[0]
    const targetName = String(selectedRow?.comment || fallbackRow?.comment || '').trim()
    if (!targetName) {
      toast.error('Сначала выберите АК или задайте поиск по наименованию.')
      return
    }
    setSelectedIds(
      new Set(
        items
          .filter((item) => String(item.comment || '').trim() === targetName)
          .map((item) => String(item.document_id || ''))
          .filter(Boolean),
      ),
    )
  }

  const clearSelection = () => {
    setSelectedIds(new Set())
    setLastClickedIndex(-1)
  }

  const createCodes = () =>
    runBusy(
      'create',
      async () => {
        await apiCall('create_aggregation_codes', createComment, Number(createCount || 0))
        await load(true)
      },
      'Агрегационные коды созданы.',
    )

  const refreshList = () =>
    runBusy(
      'refresh',
      async () => {
        await load(true)
      },
      'Список АК обновлён.',
    )

  const downloadSelected = () =>
    runBusy(
      'download-selected',
      async () => {
        await apiCall('download_selected_aggregations', selectedIdList())
        await load(true)
      },
      'Выбранные АК скачаны.',
    )

  const approveSelected = () =>
    runBusy(
      'approve-selected',
      async () => {
        const ids = selectedIdList()
        const allow =
          allowDisaggregate ||
          window.confirm(
            'Если среди выбранных АК есть коды, уже привязанные к другому АК, разрешить расформирование старого АК?',
          )
        await apiCall('approve_selected_aggregations', ids, allow)
        await load(true)
      },
      'Проведение выбранных АК завершено.',
    )

  const archiveSelected = () =>
    runBusy(
      'archive-selected',
      async () => {
        await apiCall('archive_selected_aggregations', selectedIdList())
        await load(true)
      },
      'Выбранные АК отправлены в архив.',
    )

  const introduceSelected = () =>
    runBusy(
      'intro-selected',
      async () => {
        await apiCall(
          'introduce_selected_aggregations',
          selectedIdList(),
          productionDate,
          expirationDate,
          batchNumber,
          documentTitle,
        )
        await load(true)
      },
      'Ввод в оборот по выбранным АК завершён.',
    )

  const refill = () =>
    runBusy(
      'refill',
      async () => {
        await apiCall('refill_aggregations', commentFilter, refillToken)
        await load(true)
      },
      'Повторное наполнение АК завершено.',
    )

  const cacheAge = Number(state.cache_age_seconds || 0)
  const statusOptions = state.status_options ?? []

  return (
    <div className="page-shell">
      <PageHeader
        title="Коды агрегации"
        subtitle="Создание, скачивание, проведение и повторное наполнение АК."
        actions={
          <Button variant="outline" size="sm" onClick={() => void refreshList()} disabled={loading || isBusy}>
            <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
            Обновить
          </Button>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Всего АК" value={state.total_items ?? items.length} />
        <StatPill label="Найдено" value={filteredItems.length} />
        <StatPill label="Выбрано" value={selectedIds.size} />
        <StatPill label="Возраст кэша" value={cacheAge > 0 ? `${cacheAge} с` : '—'} />
      </div>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,0.8fr)_minmax(0,1.2fr)]">
        <Card>
          <CardHeader>
            <CardTitle>Создание АК</CardTitle>
            <CardDescription>Запрос новых агрегационных кодов в Контур.Маркировке.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div>
              <FieldLabel>Название</FieldLabel>
              <TextInput
                value={createComment}
                onChange={(e) => setCreateComment(e.target.value)}
                placeholder="Например: лат диаг S 260316 (249к)"
              />
            </div>
            <div>
              <FieldLabel>Количество агрегатов</FieldLabel>
              <TextInput
                type="number"
                min={1}
                step={1}
                value={createCount}
                onChange={(e) => setCreateCount(e.target.value)}
              />
            </div>
            <Button size="sm" onClick={() => void createCodes()} disabled={isBusy}>
              Создать коды агрегации
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Проведение и ввод в оборот</CardTitle>
            <CardDescription>Действия над выбранными АК.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-2">
              <div>
                <FieldLabel>Дата производства</FieldLabel>
                <DatePickerField value={productionDate} onChange={setProductionDate} />
              </div>
              <div>
                <FieldLabel>Срок годности</FieldLabel>
                <DatePickerField value={expirationDate} onChange={setExpirationDate} />
              </div>
              <div>
                <FieldLabel>Номер партии</FieldLabel>
                <TextInput
                  value={batchNumber}
                  onChange={(e) => setBatchNumber(e.target.value)}
                  placeholder="Введите номер партии"
                />
              </div>
              <div>
                <FieldLabel>Название документа ввода в оборот</FieldLabel>
                <TextInput
                  value={documentTitle}
                  onChange={(e) => setDocumentTitle(e.target.value)}
                  placeholder="Можно оставить пустым для автоназвания"
                />
              </div>
            </div>

            <Checkbox isSelected={allowDisaggregate} onChange={setAllowDisaggregate}>
              <span className="text-sm">Разрешить расформирование чужих АК при проведении</span>
            </Checkbox>

            <div className="flex flex-wrap gap-2">
              <Button size="sm" variant="outline" onClick={() => void downloadSelected()} disabled={isBusy || !hasSelection}>
                Скачать выбранные АК
              </Button>
              <Button size="sm" variant="outline" onClick={() => void approveSelected()} disabled={isBusy || !hasSelection}>
                Провести выбранные АК
              </Button>
              <Button size="sm" variant="outline" onClick={() => void archiveSelected()} disabled={isBusy || !hasSelection}>
                В архив
              </Button>
              <Button size="sm" variant="outline" onClick={() => void introduceSelected()} disabled={isBusy || !hasSelection}>
                Ввести в оборот выбранные АК
              </Button>
              <Button
                size="sm"
                variant={refillOpen ? 'secondary' : 'ghost'}
                onClick={() => setRefillOpen((open) => !open)}
              >
                <ChevronDown className={cn('h-3.5 w-3.5 transition-transform', refillOpen && 'rotate-180')} />
                Повторное наполнение
              </Button>
            </div>

            <AnimatePresence initial={false}>
              {refillOpen ? (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: 'auto' }}
                  exit={{ opacity: 0, height: 0 }}
                  transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
                  className="overflow-hidden"
                >
                  <div className="space-y-3 border-t border-border pt-3">
                    <div>
                      <FieldLabel>Название для повторного наполнения</FieldLabel>
                      <TextInput
                        value={commentFilter}
                        onChange={(e) => setCommentFilter(e.target.value)}
                        placeholder="Название АК для поиска"
                      />
                    </div>
                    <div>
                      <FieldLabel>TSD токен</FieldLabel>
                      <TextInput
                        value={refillToken}
                        onChange={(e) => setRefillToken(e.target.value)}
                        placeholder="Токен ТСД"
                      />
                    </div>
                    <p className="rounded-md border border-border bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
                      Повторное наполнение используйте только для АК, которые ещё не были зарегистрированы в ГИС МТ. Для
                      уже зарегистрированных АК нужна переагрегация только по изменяемым кодам, а не повторная отправка
                      всего состава.
                    </p>
                    <Button size="sm" onClick={() => void refill()} disabled={isBusy}>
                      Выполнить повторное наполнение
                    </Button>
                  </div>
                </motion.div>
              ) : null}
            </AnimatePresence>
          </CardContent>
        </Card>
      </div>

      <Card className="mt-4">
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>Список АК из Контур.Маркировки</CardTitle>
            <CardDescription>
              Всего АК: {state.total_items ?? items.length} • Найдено: {filteredItems.length} • Выбрано:{' '}
              {selectedIds.size} • Страница: {page + 1}/{totalPages}
              {cacheAge > 0 ? ` • Кэш: ${cacheAge} сек.` : ''}
            </CardDescription>
          </div>
          <div className="flex flex-wrap gap-1.5">
            <Button size="sm" variant="outline" onClick={selectVisible} disabled={filteredItems.length === 0}>
              Выбрать найденные
            </Button>
            <Button size="sm" variant="outline" onClick={selectByName} disabled={items.length === 0}>
              Выбрать одноимённые
            </Button>
            <Button size="sm" variant="ghost" onClick={clearSelection} disabled={!hasSelection}>
              Снять выделение
            </Button>
            <Button size="sm" variant="outline" onClick={() => void refreshList()} disabled={loading || isBusy}>
              Обновить
            </Button>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid gap-2 sm:grid-cols-[minmax(0,220px)_minmax(0,1fr)]">
            <div>
              <FieldLabel>Статус</FieldLabel>
              <SelectNative
                value={statusFilter}
                onChange={(e) => {
                  setStatusFilter(e.target.value)
                  setCurrentPage(0)
                  setLastClickedIndex(-1)
                }}
              >
                {statusOptions.length === 0 ? <option value="">Все статусы</option> : null}
                {statusOptions.map((option) => (
                  <option key={String(option.value ?? '')} value={String(option.value ?? '')}>
                    {option.label || option.value || 'Все статусы'}
                  </option>
                ))}
              </SelectNative>
            </div>
            <div>
              <FieldLabel>Поиск</FieldLabel>
              <TextInput
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value)
                  setCurrentPage(0)
                  setLastClickedIndex(-1)
                }}
                placeholder="Название или код агрегации"
              />
            </div>
          </div>

          {filteredItems.length === 0 ? (
            <EmptyState>Агрегационные коды по текущему фильтру не найдены.</EmptyState>
          ) : (
            <>
              <div
                className="max-h-[520px] overflow-auto"
                onMouseDownCapture={(event) => {
                  shiftPressedRef.current = event.shiftKey
                }}
                onKeyDownCapture={(event) => {
                  shiftPressedRef.current = event.shiftKey
                }}
              >
                <Table aria-label="Коды агрегации">
                  <TableHeader>
                    <TableRow>
                      <TableHead>✓</TableHead>
                      <TableHead>АК</TableHead>
                      <TableHead>Статус</TableHead>
                      <TableHead>Создан</TableHead>
                      <TableHead>КМ</TableHead>
                      <TableHead>Ошибки</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {pageRows.map((row, localIndex) => {
                      const id = String(row.document_id || '')
                      const selected = selectedIds.has(id)
                      const globalIndex = pageStart + localIndex
                      return (
                        <TableRow
                          key={id || `${row.aggregate_code}-${globalIndex}`}
                          id={id || `${row.aggregate_code}-${globalIndex}`}
                          className={cn(selected && 'bg-muted/60')}
                          onClick={() => toggleRow(id, globalIndex)}
                        >
                          <TableCell onClick={(event) => event.stopPropagation()}>
                            <Checkbox
                              isSelected={selected}
                              onChange={() => toggleRow(id, globalIndex)}
                              aria-label="Выбрать АК"
                            />
                          </TableCell>
                          <TableCell>
                            <div className="font-medium">{row.aggregate_code || '—'}</div>
                            <div className="text-[11px] text-muted-foreground">{row.comment || '—'}</div>
                          </TableCell>
                          <TableCell>
                            <Badge tone={toneForStatus(row.status_label || row.status)}>
                              {row.status_label || row.status || '—'}
                            </Badge>
                            {row.status === 'readyForSendAfterApproved' ? (
                              <div className="mt-1 text-[11px] text-muted-foreground">
                                Изменённый состав после прошлой регистрации
                              </div>
                            ) : null}
                          </TableCell>
                          <TableCell className="text-xs text-muted-foreground">{row.created_at_label || '—'}</TableCell>
                          <TableCell className="tabular-nums">{row.includes_units_count ?? 0}</TableCell>
                          <TableCell className="tabular-nums">{row.codes_check_errors_count ?? 0}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>

              <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
                <div>
                  Показано {pageStart + 1}-{pageEnd} из {filteredItems.length}
                </div>
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="outline" onClick={() => setCurrentPage(page - 1)} disabled={page <= 0}>
                    Назад
                  </Button>
                  <span>
                    Страница {page + 1} из {totalPages}
                  </span>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => setCurrentPage(page + 1)}
                    disabled={page >= totalPages - 1}
                  >
                    Вперёд
                  </Button>
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

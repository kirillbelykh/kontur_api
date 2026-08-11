import { useCallback, useEffect, useMemo, useState } from 'react'
import { ChevronLeft, ChevronRight, Maximize2, RefreshCw } from 'lucide-react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { useCachedState } from '@/lib/view-cache'
import { cn, getErrorMessage } from '@/lib/utils'
import { EmptyState, PageHeader, StatPill } from '@/components/layout/PageHeader'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { DatePickerField } from '@/components/ui/date-picker'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { FieldLabel, TableSearch, TextInput } from '@/components/ui/field'
import { TablePagination, usePagination } from '@/components/ui/pagination'
import { SelectNative } from '@/components/ui/select'
import { TableSkeleton } from '@/components/ui/skeleton'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

type SheetFormat = { key?: string; label?: string }

type TemplateItem = {
  name?: string
  category?: string
  relative_path?: string
  path?: string
  data_source_kind?: string
  source_label?: string
  sheet_format?: string
  sheet_format_label?: string
}

type FileItem = {
  name?: string
  folder_name?: string
  path?: string
  record_count?: number
  modified_timestamp?: number
}

type LabelOrder = {
  document_id?: string
  order_name?: string
  status?: string
  gtin?: string
  full_name?: string
  size?: string
  batch?: string
}

type LabelsState = {
  sheet_formats?: SheetFormat[]
  default_sheet_format?: string
  templates?: TemplateItem[]
  aggregation_files?: FileItem[]
  marking_files?: FileItem[]
  orders?: LabelOrder[]
  printers?: string[]
  default_printer?: string
}

type PreviewPayload = {
  order_name?: string
  sheet_format?: string
  sheet_format_label?: string
  template_category?: string
  data_source_kind?: string
  print_scope_label?: string
  size?: string
  batch?: string
  color?: string
  manufacture_date?: string
  expiration_date?: string
  quantity_pairs?: number
  quantity_pairs_word?: string
  package_text?: string | null
  label_count?: number
  total_record_count?: number
  selected_record_number?: number | null
  selected_record_end_number?: number | null
  range_record_count?: number
  selected_code_label?: string
  selected_code_value_short?: string
  selected_code_gtin?: string
  selected_code_name?: string
  manual_override_used?: boolean
}

type ManualFields = {
  gtin: string
  size: string
  batch: string
  color: string
  units_per_pack: string
}

type LabelActionResult = {
  preview?: PreviewPayload
  needs_manual_input?: boolean
  prompt?: string
  manual_form?: { prompt?: string; error?: string; fields?: Partial<ManualFields> }
}

type PrintScope = 'all' | 'single' | 'range'
type TableKey = 'orders' | 'aggregation' | 'marking'
type Row = Record<string, unknown>
type Column = { label: string; key: string }

const TEMPLATE_PAGE_SIZE = 3
const EMPTY_MANUAL: ManualFields = { gtin: '', size: '', batch: '', color: '', units_per_pack: '' }

function filterRows(rows: Row[], query: string): Row[] {
  const normalized = query.trim().toLowerCase()
  if (!normalized) return rows
  return rows.filter((row) =>
    Object.values(row)
      .map((value) => String(value ?? '').toLowerCase())
      .join(' ')
      .includes(normalized),
  )
}

function SelectableTable({
  ariaLabel,
  rows,
  columns,
  rowId,
  selectedId,
  onSelect,
  maxHeight = 'max-h-[260px]',
  emptyText,
}: {
  ariaLabel: string
  rows: Row[]
  columns: Column[]
  rowId: (row: Row) => string
  selectedId: string
  onSelect: (id: string) => void
  maxHeight?: string
  emptyText: string
}) {
  // Пагинация внутри таблиц печати — списки файлов/заказов бывают большими
  const pager = usePagination(rows, 25)

  if (rows.length === 0) return <EmptyState>{emptyText}</EmptyState>

  return (
    <div>
    <div className={cn('overflow-auto', maxHeight)}>
      <Table aria-label={ariaLabel}>
        <TableHeader>
          <TableRow>
            <TableHead isRowHeader={false}>Выбор</TableHead>
            {columns.map((column, index) => (
              <TableHead key={column.key} isRowHeader={index === 0}>{column.label}</TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {pager.pageRows.map((row, index) => {
            const id = rowId(row)
            const selected = Boolean(id) && id === selectedId
            return (
              <TableRow
                key={id || index}
                id={id || `row-${index}`}
                className={cn(selected && 'bg-muted/60')}
                onClick={() => onSelect(selected ? '' : id)}
              >
                <TableCell>
                  <Checkbox
                    isSelected={selected}
                    aria-label={`Выбрать строку ${index + 1}`}
                    onChange={(next) => onSelect(next ? id : '')}
                  />
                </TableCell>
                {columns.map((column) => (
                  <TableCell key={column.key}>{String(row[column.key] ?? '') || '—'}</TableCell>
                ))}
              </TableRow>
            )
          })}
        </TableBody>
      </Table>
    </div>
    <TablePagination
      page={pager.page}
      pageCount={pager.pageCount}
      total={pager.total}
      pageSize={25}
      onPageChange={pager.setPage}
    />
    </div>
  )
}

export function LabelsPage() {
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [state, setState] = useCachedState<LabelsState>('labels.state', {})

  const [sheetFormat, setSheetFormat] = useState('')
  const [printer, setPrinter] = useState('')
  const [manufactureDate, setManufactureDate] = useState('')
  const [expirationDate, setExpirationDate] = useState('')
  const [quantityValue, setQuantityValue] = useState('')

  const [templatePath, setTemplatePath] = useState('')
  const [templatePage, setTemplatePage] = useState(0)

  const [orderId, setOrderId] = useState('')
  const [aggregationPath, setAggregationPath] = useState('')
  const [markingPath, setMarkingPath] = useState('')

  const [printScope, setPrintScope] = useState<PrintScope>('all')
  const [recordNumber, setRecordNumber] = useState(1)
  const [rangeStart, setRangeStart] = useState(1)
  const [rangeEnd, setRangeEnd] = useState(1)

  const [preview, setPreview] = useState<PreviewPayload | null>(null)
  const [manualEnabled, setManualEnabled] = useState(false)
  const [manualPrompt, setManualPrompt] = useState('')
  const [manualFields, setManualFields] = useState<ManualFields>(EMPTY_MANUAL)

  const [search, setSearch] = useState<Record<TableKey, string>>({ orders: '', aggregation: '', marking: '' })
  const [fullscreen, setFullscreen] = useState<TableKey | ''>('')

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const result = await apiCall<LabelsState>('get_labels_state')
      setState(result)
      setSheetFormat((prev) => prev || String(result.default_sheet_format || '100x180'))
      setPrinter((prev) => prev || String(result.default_printer || ''))
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось загрузить состояние этикеток'))
    } finally {
      setLoading(false)
    }
  }, [setState])

  useEffect(() => {
    void load()
    void apiCall<{ production_date?: string; expiration_date?: string }>('get_default_date_window')
      .then((window) => {
        setManufactureDate((prev) => prev || String(window.production_date || ''))
        setExpirationDate((prev) => prev || String(window.expiration_date || ''))
      })
      .catch(() => null)
  }, [load])

  const templates = useMemo(() => state.templates ?? [], [state.templates])
  const sheetFormats = state.sheet_formats ?? []
  const printers = state.printers ?? []
  const orders = useMemo(() => state.orders ?? [], [state.orders])
  const aggregationFiles = useMemo(() => state.aggregation_files ?? [], [state.aggregation_files])
  const markingFiles = useMemo(() => state.marking_files ?? [], [state.marking_files])

  const activeSheetFormat = sheetFormat || state.default_sheet_format || '100x180'

  const visibleTemplates = useMemo(
    () =>
      templates.filter(
        (item) => String(item.sheet_format || state.default_sheet_format || '100x180').trim() === activeSheetFormat,
      ),
    [templates, activeSheetFormat, state.default_sheet_format],
  )

  useEffect(() => {
    if (visibleTemplates.length === 0) return
    if (visibleTemplates.some((item) => item.path === templatePath)) return
    setTemplatePath(String(visibleTemplates[0].path || ''))
    setTemplatePage(0)
    setPreview(null)
  }, [visibleTemplates, templatePath])

  const selectedTemplate = visibleTemplates.find((item) => item.path === templatePath) || null
  const isAggregationSource = selectedTemplate?.data_source_kind === 'aggregation'
  const csvPath = isAggregationSource ? aggregationPath : markingPath
  const selectedFile = (isAggregationSource ? aggregationFiles : markingFiles).find((item) => item.path === csvPath) || null
  const totalRecords = Math.max(0, Number(selectedFile?.record_count || 0))

  const sheetFormatLabel =
    sheetFormats.find((item) => item.key === activeSheetFormat)?.label || activeSheetFormat || '100x180'

  const templateTotalPages = Math.max(1, Math.ceil(visibleTemplates.length / TEMPLATE_PAGE_SIZE))
  const templatePageIndex = Math.min(Math.max(0, templatePage), templateTotalPages - 1)
  const templateStart = templatePageIndex * TEMPLATE_PAGE_SIZE
  const templatePageItems = visibleTemplates.slice(templateStart, templateStart + TEMPLATE_PAGE_SIZE)

  const isBusy = Boolean(busy)

  const resetManual = () => {
    setManualEnabled(false)
    setManualPrompt('')
    setManualFields(EMPTY_MANUAL)
  }

  const setManualField = <K extends keyof ManualFields>(key: K, value: string) => {
    setManualFields((prev) => ({ ...prev, [key]: value }))
  }

  const tableConfig = (key: TableKey) => {
    if (key === 'orders') {
      return {
        title: 'Заказы',
        rows: orders as Row[],
        columns: [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'GTIN', key: 'gtin' },
          { label: 'Размер', key: 'size' },
          { label: 'Партия', key: 'batch' },
        ] as Column[],
        rowId: (row: Row) => String(row.document_id ?? ''),
        selectedId: orderId,
        onSelect: (id: string) => {
          setOrderId(id)
          resetManual()
          setPreview(null)
        },
        emptyText: 'Заказы не найдены',
      }
    }

    const isAggregation = key === 'aggregation'
    return {
      title: isAggregation ? 'Агрег коды км' : 'Коды км',
      rows: (isAggregation ? aggregationFiles : markingFiles) as Row[],
      columns: [
        { label: 'Файл', key: 'name' },
        { label: 'Папка', key: 'folder_name' },
        { label: 'Строк', key: 'record_count' },
      ] as Column[],
      rowId: (row: Row) => String(row.path ?? ''),
      selectedId: isAggregation ? aggregationPath : markingPath,
      onSelect: (id: string) => {
        if (isAggregation) setAggregationPath(id)
        else setMarkingPath(id)
        resetManual()
        setPreview(null)
      },
      emptyText: 'Файлы с кодами не найдены',
    }
  }

  const buildPayload = () => ({
    sheet_format: activeSheetFormat,
    document_id: orderId,
    template_path: templatePath,
    csv_path: csvPath,
    printer_name: printer,
    manufacture_date: manufactureDate,
    expiration_date: expirationDate,
    quantity_value: quantityValue,
    print_scope: printScope,
    record_number: printScope === 'single' ? recordNumber : null,
    range_start: printScope === 'range' ? rangeStart : null,
    range_end: printScope === 'range' ? rangeEnd : null,
    manual_override: manualEnabled ? { enabled: true, ...manualFields } : null,
  })

  const handleResult = (result: LabelActionResult): boolean => {
    if (result.needs_manual_input) {
      const form = result.manual_form || {}
      const fields = form.fields || {}
      setManualEnabled(true)
      setManualPrompt(result.prompt || form.prompt || 'Заполните форму вручную и повторите действие.')
      setManualFields({
        gtin: String(fields.gtin || ''),
        size: String(fields.size || ''),
        batch: String(fields.batch || ''),
        color: String(fields.color || ''),
        units_per_pack: String(fields.units_per_pack || ''),
      })
      setPreview(null)
      toast.info(result.prompt || 'Заполните форму вручную.')
      return false
    }
    if (!result.preview?.manual_override_used) resetManual()
    if (result.preview) setPreview(result.preview)
    return true
  }

  const runBusy = async (key: string, action: () => Promise<boolean>, successMessage: string) => {
    setBusy(key)
    try {
      if (await action()) toast.success(successMessage)
    } catch (error) {
      toast.error(getErrorMessage(error))
    } finally {
      setBusy(null)
    }
  }

  const showPreview = () =>
    runBusy(
      'preview',
      async () => handleResult(await apiCall<LabelActionResult>('preview_100x180_label', buildPayload())),
      'Контекст печати собран.',
    )

  const print = () =>
    runBusy(
      'print',
      async () => handleResult(await apiCall<LabelActionResult>('print_100x180_label', buildPayload())),
      'Печать поставлена в очередь.',
    )

  const recordInfoText = () => {
    if (!totalRecords) return 'Сначала выберите файл с кодами для печати.'
    if (printScope === 'all') return `Сейчас на печать пойдёт весь файл: ${totalRecords} этикеток.`
    if (printScope === 'range') {
      return `На печать пойдёт диапазон записей №${rangeStart}-${rangeEnd} из ${totalRecords}. Всего этикеток: ${Math.max(
        0,
        rangeEnd - rangeStart + 1,
      )}.`
    }
    return `Выбрана запись №${recordNumber} из ${totalRecords}. Нажмите «Показать контекст», чтобы проверить код перед печатью.`
  }

  const previewLines = useMemo(() => {
    if (!preview) return []
    const lines = [
      `Заказ: ${preview.order_name || '—'}`,
      `Формат: ${preview.sheet_format_label || preview.sheet_format || '100x180'}`,
      `Шаблон: ${preview.template_category || '—'} / ${preview.data_source_kind || '—'}`,
      `Режим печати: ${preview.print_scope_label || 'Весь файл'}`,
      `Размер: ${preview.size || '—'}`,
      `Партия: ${preview.batch || '—'}`,
      `Цвет: ${preview.color || '—'}`,
      `Дата изготовления: ${preview.manufacture_date || '—'}`,
      `Срок годности: ${preview.expiration_date || '—'}`,
      `Количество: ${preview.quantity_pairs ?? '—'} ${preview.quantity_pairs_word || ''}`.trim(),
      `Упаковка: ${preview.package_text || 'не используется'}`,
      `Этикеток к печати: ${preview.label_count ?? '—'}`,
    ]
    if (preview.total_record_count) lines.push(`Записей в файле: ${preview.total_record_count}`)
    if (
      preview.selected_record_number &&
      preview.selected_record_end_number &&
      preview.selected_record_end_number !== preview.selected_record_number
    ) {
      lines.push(`Выбран диапазон: ${preview.selected_record_number}-${preview.selected_record_end_number}`)
      lines.push(`Записей в диапазоне: ${preview.range_record_count || preview.label_count}`)
    } else if (preview.selected_record_number) {
      lines.push(
        `Выбрана запись: ${preview.selected_record_number} из ${preview.total_record_count || preview.label_count}`,
      )
    }
    if (preview.selected_code_label && preview.selected_code_value_short) {
      lines.push(`${preview.selected_code_label}: ${preview.selected_code_value_short}`)
    }
    if (preview.selected_code_gtin) lines.push(`GTIN выбранной записи: ${preview.selected_code_gtin}`)
    if (preview.selected_code_name) lines.push(`Наименование выбранной записи: ${preview.selected_code_name}`)
    return lines
  }, [preview])

  const renderTableCard = (key: TableKey) => {
    const config = tableConfig(key)
    return (
      <Card>
        <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
          <div>
            <CardTitle>{config.title}</CardTitle>
            <CardDescription>Кликните по строке, чтобы выбрать.</CardDescription>
          </div>
          <Button
            size="icon"
            variant="outline"
            title="На весь экран"
            aria-label="На весь экран"
            onClick={() => setFullscreen(key)}
          >
            <Maximize2 className="h-4 w-4" />
          </Button>
        </CardHeader>
        <CardContent className="space-y-2">
          <TableSearch
            value={search[key]}
            onChange={(value) => setSearch((prev) => ({ ...prev, [key]: value }))}
            placeholder="Поиск по таблице"
          />
          {loading && config.rows.length === 0 ? (
            <TableSkeleton rows={6} />
          ) : (
            <SelectableTable
              ariaLabel={config.title}
              rows={filterRows(config.rows, search[key])}
              columns={config.columns}
              rowId={config.rowId}
              selectedId={config.selectedId}
              onSelect={config.onSelect}
              maxHeight="max-h-[360px]"
              emptyText={config.emptyText}
            />
          )}
        </CardContent>
      </Card>
    )
  }

  const fullscreenConfig = fullscreen ? tableConfig(fullscreen) : null

  return (
    <div className="page-shell">
      <PageHeader
        title="Печать этикеток"
        subtitle="Шаблоны BarTender 100x180 и 100x136, контекст печати и запуск печати."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading || isBusy}>
              <RefreshCw className={loading ? 'h-3.5 w-3.5 animate-spin' : 'h-3.5 w-3.5'} />
              Обновить
            </Button>
            <Button variant="outline" size="sm" onClick={() => void showPreview()} disabled={isBusy}>
              Показать контекст
            </Button>
            <Button size="sm" onClick={() => void print()} disabled={isBusy}>
              Выполнить печать
            </Button>
          </>
        }
      />

      <div className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
        <StatPill label="Шаблоны формата" value={visibleTemplates.length} />
        <StatPill label="Принтеры" value={printers.length} />
        <StatPill label="Записей в файле" value={totalRecords || '—'} />
        <StatPill label="Формат" value={sheetFormatLabel} />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Параметры печати {sheetFormatLabel}</CardTitle>
            <CardDescription>Формат, принтер, даты и количество для контекста BarTender.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-2">
              <div>
                <FieldLabel>Формат этикетки</FieldLabel>
                <SelectNative
                  value={activeSheetFormat}
                  onChange={(e) => {
                    setSheetFormat(e.target.value)
                    setTemplatePage(0)
                    resetManual()
                    setPreview(null)
                  }}
                >
                  {sheetFormats.map((format) => (
                    <option key={String(format.key)} value={String(format.key)}>
                      {format.label || format.key}
                    </option>
                  ))}
                </SelectNative>
              </div>
              <div>
                <FieldLabel>Принтер</FieldLabel>
                <SelectNative value={printer} onChange={(e) => setPrinter(e.target.value)}>
                  <option value="">Выберите принтер</option>
                  {printers.map((item) => (
                    <option key={item} value={item}>
                      {item}
                    </option>
                  ))}
                </SelectNative>
              </div>
              <div>
                <FieldLabel>Дата изготовления</FieldLabel>
                <DatePickerField value={manufactureDate} onChange={setManufactureDate} />
              </div>
              <div>
                <FieldLabel>Срок годности</FieldLabel>
                <DatePickerField value={expirationDate} onChange={setExpirationDate} />
              </div>
              <div>
                <FieldLabel>Количество</FieldLabel>
                <TextInput
                  type="number"
                  min={1}
                  step={1}
                  value={quantityValue}
                  onChange={(e) => setQuantityValue(e.target.value)}
                  placeholder="Для шаблонов Латекс, Нитрил, HR"
                />
              </div>
            </div>

            <div className="rounded-md border border-border bg-muted/30 p-3">
              <div className="grid gap-2 sm:grid-cols-2">
                <div>
                  <FieldLabel>Что печатаем</FieldLabel>
                  <SelectNative
                    value={printScope}
                    onChange={(e) => {
                      const next = e.target.value
                      setPrintScope(next === 'single' || next === 'range' ? next : 'all')
                      setPreview(null)
                    }}
                  >
                    <option value="all">Весь файл</option>
                    <option value="single">Одна этикетка</option>
                    <option value="range">Диапазон этикеток</option>
                  </SelectNative>
                </div>
                <div>
                  <FieldLabel>Номер этикетки</FieldLabel>
                  <div className="flex items-center gap-1.5">
                    <Button
                      size="icon"
                      variant="outline"
                      aria-label="Предыдущая запись"
                      disabled={printScope !== 'single' || totalRecords <= 0 || recordNumber <= 1}
                      onClick={() => {
                        setRecordNumber((prev) => Math.max(1, prev - 1))
                        setPreview(null)
                      }}
                    >
                      <ChevronLeft className="h-4 w-4" />
                    </Button>
                    <TextInput
                      type="number"
                      min={totalRecords > 0 ? 1 : 0}
                      max={totalRecords > 0 ? totalRecords : undefined}
                      value={recordNumber}
                      disabled={printScope !== 'single' || totalRecords <= 0}
                      onChange={(e) => {
                        setRecordNumber(Number.parseInt(e.target.value || '1', 10) || 1)
                        setPreview(null)
                      }}
                    />
                    <Button
                      size="icon"
                      variant="outline"
                      aria-label="Следующая запись"
                      disabled={printScope !== 'single' || totalRecords <= 0 || recordNumber >= totalRecords}
                      onClick={() => {
                        setRecordNumber((prev) => (totalRecords > 0 ? Math.min(totalRecords, prev + 1) : prev + 1))
                        setPreview(null)
                      }}
                    >
                      <ChevronRight className="h-4 w-4" />
                    </Button>
                  </div>
                </div>
                <div>
                  <FieldLabel>Начало диапазона</FieldLabel>
                  <TextInput
                    type="number"
                    min={totalRecords > 0 ? 1 : 0}
                    max={totalRecords > 0 ? totalRecords : undefined}
                    value={rangeStart}
                    disabled={printScope !== 'range' || totalRecords <= 0}
                    onChange={(e) => {
                      setRangeStart(Number.parseInt(e.target.value || '1', 10) || 1)
                      setPreview(null)
                    }}
                  />
                </div>
                <div>
                  <FieldLabel>Конец диапазона</FieldLabel>
                  <TextInput
                    type="number"
                    min={totalRecords > 0 ? 1 : 0}
                    max={totalRecords > 0 ? totalRecords : undefined}
                    value={rangeEnd}
                    disabled={printScope !== 'range' || totalRecords <= 0}
                    onChange={(e) => {
                      setRangeEnd(Number.parseInt(e.target.value || '1', 10) || 1)
                      setPreview(null)
                    }}
                  />
                </div>
              </div>
              <p className="mt-2 text-xs text-muted-foreground">{recordInfoText()}</p>
            </div>

            <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-xs">
              {previewLines.length === 0 ? (
                <span className="text-muted-foreground">
                  Выберите шаблон, файл и заказ, затем нажмите «Показать контекст».
                </span>
              ) : (
                <ul className="space-y-0.5">
                  {previewLines.map((line, index) => (
                    <li key={index}>{line}</li>
                  ))}
                </ul>
              )}
            </div>

            {manualEnabled ? (
              <div className="space-y-2 rounded-md border border-warning/40 bg-muted/30 p-3">
                <div className="text-sm font-medium">Ручное заполнение</div>
                <p className="text-xs text-muted-foreground">
                  {manualPrompt ||
                    'Если автоподбор не сработал, заполните поля вручную и повторите предпросмотр или печать.'}
                </p>
                <div className="grid gap-2 sm:grid-cols-2">
                  <div>
                    <FieldLabel>GTIN</FieldLabel>
                    <TextInput
                      value={manualFields.gtin}
                      onChange={(e) => setManualField('gtin', e.target.value)}
                      placeholder="GTIN"
                    />
                  </div>
                  <div>
                    <FieldLabel>Размер</FieldLabel>
                    <TextInput
                      value={manualFields.size}
                      onChange={(e) => setManualField('size', e.target.value)}
                      placeholder="Например, M или 7,5"
                    />
                  </div>
                  <div>
                    <FieldLabel>Партия</FieldLabel>
                    <TextInput
                      value={manualFields.batch}
                      onChange={(e) => setManualField('batch', e.target.value)}
                      placeholder="Номер партии"
                    />
                  </div>
                  <div>
                    <FieldLabel>Цвет</FieldLabel>
                    <TextInput
                      value={manualFields.color}
                      onChange={(e) => setManualField('color', e.target.value)}
                      placeholder="Можно оставить пустым"
                    />
                  </div>
                  <div>
                    <FieldLabel>Единиц в упаковке</FieldLabel>
                    <TextInput
                      type="number"
                      min={1}
                      step={1}
                      value={manualFields.units_per_pack}
                      onChange={(e) => setManualField('units_per_pack', e.target.value)}
                      placeholder="Например, 10"
                    />
                  </div>
                </div>
              </div>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex-row items-start justify-between gap-3 space-y-0">
            <div>
              <CardTitle>Шаблоны BarTender</CardTitle>
              <CardDescription>
                {visibleTemplates.length
                  ? `${templateStart + 1}-${Math.min(templateStart + templatePageItems.length, visibleTemplates.length)} / ${visibleTemplates.length}`
                  : '0-0 / 0'}
              </CardDescription>
            </div>
            <div className="flex gap-1.5">
              <Button
                size="sm"
                variant="outline"
                onClick={() => setTemplatePage(templatePageIndex - 1)}
                disabled={templatePageIndex <= 0}
              >
                Назад
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setTemplatePage(templatePageIndex + 1)}
                disabled={templatePageIndex >= templateTotalPages - 1}
              >
                Вперед
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {templatePageItems.length === 0 ? (
              <EmptyState>Шаблоны для выбранного формата не найдены</EmptyState>
            ) : (
              <div className="space-y-2">
                {templatePageItems.map((template) => {
                  const selected = template.path === templatePath
                  return (
                    <button
                      key={String(template.path)}
                      type="button"
                      className={cn(
                        'w-full rounded-xl border bg-[var(--field-bg)] px-3.5 py-2.5 text-left transition',
                        selected
                          ? 'border-foreground/40 ring-1 ring-foreground/15'
                          : 'border-border hover:border-[var(--field-border-hover)]',
                      )}
                      onClick={() => {
                        setTemplatePath(String(template.path || ''))
                        resetManual()
                        setPreview(null)
                      }}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-medium">{template.name || '—'}</span>
                        <span className="shrink-0 rounded-full bg-muted px-2 py-0.5 text-[11px] font-medium text-muted-foreground">
                          {template.source_label || template.data_source_kind || '—'}
                        </span>
                      </div>
                      <div className="mt-0.5 text-xs text-muted-foreground">
                        {template.sheet_format_label || template.sheet_format} • {template.category || '—'}
                      </div>
                      <div className="truncate text-[11px] text-muted-foreground">{template.relative_path || ''}</div>
                    </button>
                  )
                })}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Стопкой во всю ширину — в три узкие колонки данные было не видно */}
      <div className="mt-4 space-y-4">
        {renderTableCard('orders')}
        <div className="grid gap-4 xl:grid-cols-2">
          {renderTableCard('aggregation')}
          {renderTableCard('marking')}
        </div>
      </div>

      {fullscreen && fullscreenConfig ? (
        <Dialog
          open
          onOpenChange={(open) => {
            if (!open) setFullscreen('')
          }}
        >
          <DialogContent className="max-w-[96vw]">
            <DialogHeader>
              <DialogTitle>{fullscreenConfig.title}</DialogTitle>
            </DialogHeader>
            <div className="space-y-2">
              <TableSearch
                value={search[fullscreen]}
                onChange={(value) => setSearch((prev) => ({ ...prev, [fullscreen]: value }))}
                placeholder="Поиск по таблице"
              />
              <SelectableTable
                ariaLabel={fullscreenConfig.title}
                rows={filterRows(fullscreenConfig.rows, search[fullscreen])}
                columns={fullscreenConfig.columns}
                rowId={fullscreenConfig.rowId}
                selectedId={fullscreenConfig.selectedId}
                onSelect={fullscreenConfig.onSelect}
                maxHeight="max-h-[72vh]"
                emptyText={fullscreenConfig.emptyText}
              />
            </div>
          </DialogContent>
        </Dialog>
      ) : null}
    </div>
  )
}

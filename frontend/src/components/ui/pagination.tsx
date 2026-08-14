import { useEffect, useMemo, useState } from 'react'
import { Pagination } from '@heroui/react'
import { cn } from '@/lib/utils'

export const DEFAULT_PAGE_SIZE = 30

/** Пагинация массива строк: возвращает строки текущей страницы и метаданные. */
export function usePagination<T>(rows: T[], pageSize = DEFAULT_PAGE_SIZE) {
  const [page, setPage] = useState(0)
  const pageCount = Math.max(1, Math.ceil(rows.length / pageSize))
  const safePage = Math.min(page, pageCount - 1)

  // При изменении фильтра/данных не оставляем пользователя на несуществующей странице
  useEffect(() => {
    if (page > pageCount - 1) setPage(pageCount - 1)
  }, [page, pageCount])

  const pageRows = useMemo(
    () => rows.slice(safePage * pageSize, (safePage + 1) * pageSize),
    [rows, safePage, pageSize],
  )

  return { pageRows, page: safePage, pageCount, setPage, total: rows.length, pageSize }
}

function pageWindow(page: number, pageCount: number): Array<number | 'ellipsis'> {
  if (pageCount <= 7) return Array.from({ length: pageCount }, (_, i) => i)
  const pages = new Set<number>([0, pageCount - 1, page - 1, page, page + 1])
  const sorted = [...pages].filter((p) => p >= 0 && p < pageCount).sort((a, b) => a - b)
  const result: Array<number | 'ellipsis'> = []
  let prev = -1
  for (const p of sorted) {
    if (prev >= 0 && p - prev > 1) result.push('ellipsis')
    result.push(p)
    prev = p
  }
  return result
}

export function TablePagination({
  page,
  pageCount,
  onPageChange,
  total,
  pageSize = DEFAULT_PAGE_SIZE,
  className,
}: {
  page: number
  pageCount: number
  onPageChange: (page: number) => void
  total: number
  pageSize?: number
  className?: string
}) {
  if (pageCount <= 1) return null

  const from = page * pageSize + 1
  const to = Math.min((page + 1) * pageSize, total)

  return (
    <div className={cn('flex flex-wrap items-center justify-between gap-2 pt-2', className)}>
      <Pagination.Summary className="text-xs text-muted-foreground">
        {from}–{to} из {total}
      </Pagination.Summary>
      <Pagination aria-label="Страницы таблицы" size="sm">
        <Pagination.Content>
          <Pagination.Item>
            <Pagination.Previous isDisabled={page <= 0} onPress={() => onPageChange(page - 1)}>
              <Pagination.PreviousIcon />
            </Pagination.Previous>
          </Pagination.Item>
          {pageWindow(page, pageCount).map((item, index) =>
            item === 'ellipsis' ? (
              <Pagination.Item key={`e-${index}`}>
                <Pagination.Ellipsis />
              </Pagination.Item>
            ) : (
              <Pagination.Item key={item}>
                <Pagination.Link isActive={item === page} onPress={() => onPageChange(item)}>
                  {item + 1}
                </Pagination.Link>
              </Pagination.Item>
            ),
          )}
          <Pagination.Item>
            <Pagination.Next isDisabled={page >= pageCount - 1} onPress={() => onPageChange(page + 1)}>
              <Pagination.NextIcon />
            </Pagination.Next>
          </Pagination.Item>
        </Pagination.Content>
      </Pagination>
    </div>
  )
}

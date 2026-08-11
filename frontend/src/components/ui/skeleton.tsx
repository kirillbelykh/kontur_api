import type { HTMLAttributes } from 'react'
import { cn } from '@/lib/utils'
import { Shimmer } from '@/components/ui/shimmer'

export function Skeleton({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('animate-pulse rounded bg-muted', className)} {...props} />
}

/** Плейсхолдер таблицы на время загрузки данных */
export function TableSkeleton({ rows = 6, label = 'Загружаем данные…' }: { rows?: number; label?: string }) {
  return (
    <div className="space-y-2.5 py-2" aria-label="Загрузка данных" role="status">
      <Shimmer>{label}</Shimmer>
      {Array.from({ length: rows }).map((_, index) => (
        <div key={index} className="flex items-center gap-3">
          <Skeleton className="h-4 w-4 shrink-0 rounded-sm" />
          <Skeleton className="h-4 flex-1" />
          <Skeleton className="h-4 w-24 shrink-0" />
          <Skeleton className="h-4 w-16 shrink-0" />
        </div>
      ))}
    </div>
  )
}

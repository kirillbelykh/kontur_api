import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'
import { AnimatedNumber } from '@/components/ui/animated-number'

export function PageHeader({
  title,
  actions,
  className,
  refreshing = false,
}: {
  title: string
  actions?: ReactNode
  className?: string
  /** Тонкая полоска, пока список уже на экране и идёт фоновый refresh. */
  refreshing?: boolean
}) {
  return (
    <div className={cn('relative mb-3', className)}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h1 className="min-w-0 truncate text-lg font-semibold tracking-tight text-foreground">{title}</h1>
        {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
      </div>
      {refreshing ? <span className="page-refresh-bar" aria-hidden /> : null}
    </div>
  )
}

/** Компактная стат-пилюля: подпись и значение в одну строку. */
export function StatPill({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="inline-flex h-8 items-center gap-2 rounded-md border border-border bg-card px-3">
      <span className="whitespace-nowrap text-xs text-muted-foreground">{label}</span>
      <span className="whitespace-nowrap text-sm font-semibold tabular-nums text-foreground">
        <AnimatedNumber value={value} />
      </span>
    </div>
  )
}

/** Одна компактная строка статистики над контентом страницы. */
export function StatRow({
  items,
  className,
}: {
  items: Array<{ label: string; value: string | number }>
  className?: string
}) {
  return (
    <div className={cn('mb-3 flex flex-wrap items-center gap-2', className)}>
      {items.map((item) => (
        <StatPill key={item.label} label={item.label} value={item.value} />
      ))}
    </div>
  )
}

export function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-lg border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
      {children}
    </div>
  )
}

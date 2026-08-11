import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'
import { AnimatedNumber } from '@/components/ui/animated-number'

export function PageHeader({
  title,
  subtitle,
  actions,
  className,
}: {
  title: string
  subtitle?: string
  actions?: ReactNode
  className?: string
}) {
  return (
    <div className={cn('mb-3 flex flex-wrap items-center justify-between gap-2', className)}>
      <div className="min-w-0">
        <h1 className="text-lg font-semibold tracking-tight text-foreground">{title}</h1>
        {subtitle ? <p className="max-w-3xl truncate text-xs text-muted-foreground">{subtitle}</p> : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
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

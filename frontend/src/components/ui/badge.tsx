import type { HTMLAttributes } from 'react'
import { cn } from '@/lib/utils'
import { statusMeta } from '@/lib/status'

type BadgeTone =
  | 'neutral'
  | 'success'
  | 'warning'
  | 'danger'
  | 'primary'
  | 'secondary'
  | 'info'
  | 'violet'
  | 'teal'

/** Цвет кружка статуса — токены темы (Графит приглушает палитру). */
const dotClass: Record<BadgeTone, string> = {
  neutral: 'bg-[var(--status-neutral)]',
  secondary: 'bg-[var(--status-neutral)]',
  success: 'bg-[var(--status-success)]',
  warning: 'bg-[var(--status-warning)]',
  danger: 'bg-[var(--status-danger)]',
  primary: 'bg-[var(--status-primary)]',
  info: 'bg-[var(--status-info)]',
  violet: 'bg-[var(--status-violet)]',
  teal: 'bg-[var(--status-teal)]',
}

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone
  /** Перелив текста для статусов «в процессе». */
  shimmer?: boolean
}

export function Badge({ className, tone = 'neutral', shimmer = false, children, ...props }: BadgeProps) {
  const text = typeof children === 'string' ? children : undefined
  return (
    <span
      className={cn('inline-flex items-center gap-1.5 text-xs font-medium text-foreground', className)}
      {...props}
    >
      <span aria-hidden="true" className={cn('h-2 w-2 shrink-0 rounded-full', dotClass[tone])} />
      {shimmer && text ? (
        <span className="t-shimmer" data-text={text}>
          {text}
        </span>
      ) : (
        children
      )}
    </span>
  )
}

/** Бейдж статуса с единой раскраской и shimmer для «в процессе». */
export function StatusBadge({ status, className }: { status?: string; className?: string }) {
  const meta = statusMeta(status)
  return (
    <Badge tone={meta.tone} shimmer={meta.pending} className={className}>
      {status || '—'}
    </Badge>
  )
}

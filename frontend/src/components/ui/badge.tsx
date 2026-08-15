import type { HTMLAttributes } from 'react'
import { cn } from '@/lib/utils'
import { statusMeta, statusShowsSpinner } from '@/lib/status'

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
  neutral: 'text-[var(--status-neutral)] bg-[var(--status-neutral)]',
  secondary: 'text-[var(--status-neutral)] bg-[var(--status-neutral)]',
  success: 'text-[var(--status-success)] bg-[var(--status-success)]',
  warning: 'text-[var(--status-warning)] bg-[var(--status-warning)]',
  danger: 'text-[var(--status-danger)] bg-[var(--status-danger)]',
  primary: 'text-[var(--status-primary)] bg-[var(--status-primary)]',
  info: 'text-[var(--status-info)] bg-[var(--status-info)]',
  violet: 'text-[var(--status-violet)] bg-[var(--status-violet)]',
  teal: 'text-[var(--status-teal)] bg-[var(--status-teal)]',
}

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone
  /** Перелив текста для статусов «в процессе». */
  shimmer?: boolean
  /** Кольцо-спиннер вместо сплошного кружка — только «Скачивается». */
  spin?: boolean
}

export function Badge({ className, tone = 'neutral', shimmer = false, spin = false, children, ...props }: BadgeProps) {
  const text = typeof children === 'string' ? children : undefined
  return (
    <span
      className={cn('inline-flex items-center gap-1.5 text-xs font-medium text-foreground', className)}
      {...props}
    >
      <span
        aria-hidden="true"
        className={cn('status-dot', spin && 'status-dot--spin', dotClass[tone])}
      />
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
    <Badge tone={meta.tone} shimmer={meta.pending} spin={statusShowsSpinner(status)} className={className}>
      {status || '—'}
    </Badge>
  )
}

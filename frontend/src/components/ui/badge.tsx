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

/** Цвет кружка статуса — у каждого тона свой (marzban-style: точка + текст). */
const dotClass: Record<BadgeTone, string> = {
  neutral: 'bg-slate-400',
  secondary: 'bg-slate-400',
  success: 'bg-emerald-500',
  warning: 'bg-amber-500',
  danger: 'bg-rose-500',
  primary: 'bg-indigo-500',
  info: 'bg-sky-500',
  violet: 'bg-violet-500',
  teal: 'bg-teal-500',
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

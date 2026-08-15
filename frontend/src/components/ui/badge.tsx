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
const toneText: Record<BadgeTone, string> = {
  neutral: 'text-[var(--status-neutral)]',
  secondary: 'text-[var(--status-neutral)]',
  success: 'text-[var(--status-success)]',
  warning: 'text-[var(--status-warning)]',
  danger: 'text-[var(--status-danger)]',
  primary: 'text-[var(--status-primary)]',
  info: 'text-[var(--status-info)]',
  violet: 'text-[var(--status-violet)]',
  teal: 'text-[var(--status-teal)]',
}

const dotClass: Record<BadgeTone, string> = {
  neutral: `${toneText.neutral} bg-[var(--status-neutral)]`,
  secondary: `${toneText.secondary} bg-[var(--status-neutral)]`,
  success: `${toneText.success} bg-[var(--status-success)]`,
  warning: `${toneText.warning} bg-[var(--status-warning)]`,
  danger: `${toneText.danger} bg-[var(--status-danger)]`,
  primary: `${toneText.primary} bg-[var(--status-primary)]`,
  info: `${toneText.info} bg-[var(--status-info)]`,
  violet: `${toneText.violet} bg-[var(--status-violet)]`,
  teal: `${toneText.teal} bg-[var(--status-teal)]`,
}

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone
  /** Перелив текста для статусов «в процессе». */
  shimmer?: boolean
  /** Кольцо вместо сплошного кружка — только «Скачивается». */
  spin?: boolean
  /** 0…1 заполнение кольца; без значения кольцо крутится. */
  progress?: number
}

const RING_R = 5
const RING_C = 2 * Math.PI * RING_R

function ProgressRing({ progress }: { progress?: number }) {
  const known = progress != null && Number.isFinite(progress)
  const offset = known ? RING_C * (1 - Math.min(1, Math.max(0, progress))) : RING_C
  return (
    <svg aria-hidden="true" viewBox="0 0 12 12" className="status-download-ring">
      <circle className="status-download-core" cx="6" cy="6" r="4" />
      <circle
        className="status-download-track"
        cx="6"
        cy="6"
        r={RING_R}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
      />
      <circle
        className="status-download-arc"
        cx="6"
        cy="6"
        r={RING_R}
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeDasharray={RING_C}
        strokeDashoffset={offset}
      />
    </svg>
  )
}

export function Badge({
  className,
  tone = 'neutral',
  shimmer = false,
  spin = false,
  progress,
  children,
  ...props
}: BadgeProps) {
  const text = typeof children === 'string' ? children : undefined
  return (
    <span
      className={cn('inline-flex items-center gap-1.5 text-xs font-medium text-foreground', className)}
      {...props}
    >
      {spin ? (
        <span className={cn('inline-flex', toneText[tone])}>
          <ProgressRing progress={progress} />
        </span>
      ) : (
        <span aria-hidden="true" className={cn('status-dot', dotClass[tone])} />
      )}
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
export function StatusBadge({
  status,
  className,
  progress,
}: {
  status?: string
  className?: string
  progress?: number
}) {
  const meta = statusMeta(status)
  return (
    <Badge
      tone={meta.tone}
      shimmer={meta.pending}
      spin={statusShowsSpinner(status)}
      progress={progress}
      className={className}
    >
      {status || '—'}
    </Badge>
  )
}

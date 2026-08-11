import type { HTMLAttributes } from 'react'
import { cn } from '@/lib/utils'

type BadgeTone = 'neutral' | 'success' | 'warning' | 'danger' | 'primary' | 'secondary' | 'info'

/** Статусы в стиле marzban-custom: цветная точка + текст, без заливки. */
const toneClass: Record<BadgeTone, string> = {
  neutral: 'text-muted-foreground [--status-dot:theme(colors.slate.400)]',
  success: 'text-emerald-700 dark:text-emerald-300 [--status-dot:theme(colors.emerald.500)]',
  warning: 'text-amber-700 dark:text-amber-300 [--status-dot:theme(colors.amber.500)]',
  danger: 'text-rose-700 dark:text-rose-300 [--status-dot:theme(colors.rose.500)]',
  primary: 'text-indigo-700 dark:text-indigo-300 [--status-dot:theme(colors.indigo.500)]',
  secondary: 'text-muted-foreground [--status-dot:theme(colors.slate.400)]',
  info: 'text-sky-700 dark:text-sky-300 [--status-dot:theme(colors.sky.500)]',
}

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone
}

export function Badge({ className, tone = 'neutral', children, ...props }: BadgeProps) {
  return (
    <span
      className={cn('inline-flex items-center gap-1.5 text-xs font-medium', toneClass[tone], className)}
      {...props}
    >
      <span aria-hidden="true" className="h-2 w-2 shrink-0 rounded-full bg-[var(--status-dot)]" />
      {children}
    </span>
  )
}

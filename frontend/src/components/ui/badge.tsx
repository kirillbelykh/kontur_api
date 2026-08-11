import type { HTMLAttributes } from 'react'
import { cn } from '@/lib/utils'

type BadgeTone = 'neutral' | 'success' | 'warning' | 'danger' | 'primary' | 'secondary' | 'info'

const toneClass: Record<BadgeTone, string> = {
  neutral: 'bg-muted text-muted-foreground',
  success: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-200',
  warning: 'bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200',
  danger: 'bg-rose-100 text-rose-800 dark:bg-rose-950 dark:text-rose-200',
  primary: 'bg-slate-200/80 text-slate-800 dark:bg-slate-800 dark:text-slate-200',
  secondary: 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-200',
  info: 'bg-blue-100 text-blue-800 dark:bg-blue-950 dark:text-blue-200',
}

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone
}

export function Badge({ className, tone = 'neutral', ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        'inline-flex items-center rounded px-2 py-1 text-xs font-semibold',
        toneClass[tone],
        className,
      )}
      {...props}
    />
  )
}

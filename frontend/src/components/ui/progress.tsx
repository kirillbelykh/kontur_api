import { Label, Meter } from '@heroui/react'
import type { ReactNode } from 'react'

import { cn } from '@/lib/utils'

type ProgressProps = {
  value: number
  label?: ReactNode
  valueLabel?: ReactNode
  className?: string
  size?: 'sm' | 'md' | 'lg'
  color?: 'default' | 'accent' | 'success' | 'warning' | 'danger'
  'aria-label'?: string
}

export function Progress({
  value,
  label,
  valueLabel,
  className,
  size = 'sm',
  color = 'accent',
  'aria-label': ariaLabel,
}: ProgressProps) {
  const normalized = Math.max(0, Math.min(100, value))
  const showHeader = label != null || valueLabel != null

  return (
    <Meter
      aria-label={label != null ? undefined : (ariaLabel ?? 'Прогресс')}
      className={cn('w-full', className)}
      color={color}
      size={size}
      value={normalized}
      valueLabel={valueLabel}
    >
      {label != null ? <Label>{label}</Label> : null}
      {showHeader ? <Meter.Output /> : null}
      <Meter.Track>
        <Meter.Fill />
      </Meter.Track>
    </Meter>
  )
}

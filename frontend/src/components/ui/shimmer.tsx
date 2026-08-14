import type { ReactNode } from 'react'
import { cn } from '@/lib/utils'

/** Shimmer-текст (transitions.dev) для состояний загрузки/ожидания. */
export function Shimmer({ children, className }: { children: string; className?: string }) {
  return (
    <span className={cn('t-shimmer text-sm', className)} data-text={children}>
      {children}
    </span>
  )
}

/** Мгновенный отклик на кнопке: пока операция идёт, текст переливается. */
export function BusyLabel({
  busy,
  pending,
  children,
}: {
  busy: boolean
  pending: string
  children: ReactNode
}) {
  if (busy) return <Shimmer>{pending}</Shimmer>
  return children
}

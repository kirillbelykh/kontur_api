import { cn } from '@/lib/utils'

/** Shimmer-текст (transitions.dev) для состояний загрузки/ожидания. */
export function Shimmer({ children, className }: { children: string; className?: string }) {
  return (
    <span className={cn('t-shimmer text-sm', className)} data-text={children}>
      {children}
    </span>
  )
}

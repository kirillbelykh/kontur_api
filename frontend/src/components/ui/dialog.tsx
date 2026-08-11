import type { ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { X } from 'lucide-react'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'

export function Dialog({
  open,
  onOpenChange,
  children,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  children: ReactNode
}) {
  if (!open || typeof document === 'undefined') return null

  return createPortal(
    <div className="fixed inset-0 z-[200] flex items-center justify-center p-4">
      <button
        type="button"
        className="modal-overlay absolute inset-0 bg-black/40"
        data-state="open"
        aria-label="Закрыть"
        onClick={() => onOpenChange(false)}
      />
      <div
        role="dialog"
        aria-modal="true"
        data-state="open"
        className="modal-content relative z-10 w-full max-w-lg rounded-2xl border border-border bg-card p-5 shadow-panel"
        onClick={(event) => event.stopPropagation()}
      >
        {children}
      </div>
    </div>,
    document.body,
  )
}

export function DialogContent({
  title,
  children,
  onClose,
  className,
}: {
  title?: string
  children: ReactNode
  onClose?: () => void
  className?: string
}) {
  return (
    <div className={cn('space-y-4', className)}>
      {(title || onClose) && (
        <div className="flex items-start justify-between gap-3">
          {title ? <h2 className="text-base font-semibold tracking-tight">{title}</h2> : <span />}
          {onClose ? (
            <Button variant="ghost" size="icon" onClick={onClose} aria-label="Закрыть">
              <X className="h-4 w-4" />
            </Button>
          ) : null}
        </div>
      )}
      {children}
    </div>
  )
}

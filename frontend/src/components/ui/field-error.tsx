import { useEffect, useRef, useState, type ReactNode } from 'react'
import { cn } from '@/lib/utils'

type FieldErrorWrapProps = {
  error?: string | null
  /** Подсказка под полем, когда ошибки нет */
  hint?: ReactNode
  children: ReactNode
  className?: string
  messageClassName?: string
}

/**
 * Transitions.dev error-state: shake + сообщение.
 * Обёртка вокруг Input / SelectNative / Textarea.
 */
export function FieldErrorWrap({
  error,
  hint,
  children,
  className,
  messageClassName,
}: FieldErrorWrapProps) {
  const inputRef = useRef<HTMLDivElement>(null)
  const [isShaking, setIsShaking] = useState(false)
  const hasError = Boolean(error)

  useEffect(() => {
    if (!hasError) {
      setIsShaking(false)
      return
    }

    setIsShaking(false)
    const frame = requestAnimationFrame(() => {
      void inputRef.current?.offsetWidth
      setIsShaking(true)
    })
    return () => cancelAnimationFrame(frame)
  }, [hasError, error])

  return (
    <div className={cn('t-input-wrap', hasError && 'is-error', className)}>
      <div
        ref={inputRef}
        className={cn('t-input', hasError && 'is-error', isShaking && 'is-shaking')}
        onAnimationEnd={() => setIsShaking(false)}
      >
        {children}
      </div>
      {hasError ? (
        <p className={cn('t-error-msg text-sm text-red-500', messageClassName)}>{error}</p>
      ) : hint ? (
        <div className="text-xs text-muted-foreground">{hint}</div>
      ) : null}
    </div>
  )
}

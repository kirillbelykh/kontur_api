import { createElement } from 'react'
import { Loader2 } from 'lucide-react'
import { toast } from 'sonner'
import { getErrorMessage } from '@/lib/utils'

type JobMessages<T> = {
  id?: string
  pending?: string
  success?: string
  succeeded?: (result: T) => boolean
}

const silencedJobs = new Set<string>()

/**
 * Долгая операция с тостом, который живёт вне страницы:
 * уход в другой раздел не отменяет уведомление о завершении.
 * Sonner прячет крестик у type=loading — поэтому pending это обычный тост с duration: Infinity.
 */
export async function notifyJob<T>(
  id: string,
  work: () => Promise<T>,
  messages: JobMessages<T> = {},
): Promise<T> {
  const toastId = `job:${id}`
  silencedJobs.delete(toastId)
  if (messages.pending) {
    toast(messages.pending, {
      id: toastId,
      duration: Infinity,
      closeButton: true,
      icon: createElement(Loader2, {
        className: 'h-4.5 w-4.5 animate-spin text-muted-foreground',
      }),
      onDismiss: () => {
        silencedJobs.add(toastId)
      },
    })
  }
  try {
    const result = await work()
    if (silencedJobs.has(toastId)) {
      silencedJobs.delete(toastId)
      return result
    }
    const ok = messages.succeeded?.(result) ?? true
    if (ok && messages.success) toast.success(messages.success, { id: toastId, duration: 8000, closeButton: true })
    else toast.dismiss(toastId)
    return result
  } catch (error) {
    silencedJobs.delete(toastId)
    toast.error(getErrorMessage(error), { id: toastId, duration: 8000, closeButton: true })
    throw error
  }
}

export async function withPageJob<T>(
  setBusy: (key: string | null) => void,
  key: string,
  work: () => Promise<T>,
  messages: JobMessages<T> = {},
): Promise<T | undefined> {
  setBusy(key)
  try {
    return await notifyJob(messages.id ?? key, work, messages)
  } catch {
    return undefined
  } finally {
    setBusy(null)
  }
}

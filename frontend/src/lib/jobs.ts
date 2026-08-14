import { toast } from 'sonner'
import { getErrorMessage } from '@/lib/utils'

type JobMessages<T> = {
  id?: string
  pending?: string
  success?: string
  succeeded?: (result: T) => boolean
}

/**
 * Долгая операция с тостом, который живёт вне страницы:
 * уход в другой раздел не отменяет уведомление о завершении.
 */
export async function notifyJob<T>(
  id: string,
  work: () => Promise<T>,
  messages: JobMessages<T> = {},
): Promise<T> {
  const toastId = `job:${id}`
  if (messages.pending) toast.loading(messages.pending, { id: toastId })
  try {
    const result = await work()
    const ok = messages.succeeded?.(result) ?? true
    if (ok && messages.success) toast.success(messages.success, { id: toastId, duration: 8000 })
    else toast.dismiss(toastId)
    return result
  } catch (error) {
    toast.error(getErrorMessage(error), { id: toastId, duration: 8000 })
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

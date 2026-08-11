declare global {
  interface Window {
    pywebview?: {
      api: Record<string, (...args: any[]) => Promise<any>>
    }
  }
}

const READY_POLL_MS = 50
const READY_TIMEOUT_MS = 15_000

let readyPromise: Promise<void> | null = null

function hasApi(): boolean {
  return Boolean(window.pywebview?.api && typeof window.pywebview.api === 'object')
}

export function waitForPywebview(timeoutMs = READY_TIMEOUT_MS): Promise<void> {
  if (hasApi()) return Promise.resolve()
  if (readyPromise) return readyPromise

  readyPromise = new Promise<void>((resolve, reject) => {
    const started = Date.now()

    const tick = () => {
      if (hasApi()) {
        resolve()
        return
      }
      if (Date.now() - started >= timeoutMs) {
        readyPromise = null
        reject(new Error('PyWebView API недоступен. Запустите приложение через desktop shell.'))
        return
      }
      window.setTimeout(tick, READY_POLL_MS)
    }

    tick()
  })

  return readyPromise
}

export async function apiCall<T = any>(method: string, ...args: any[]): Promise<T> {
  await waitForPywebview()
  const api = window.pywebview?.api
  const fn = api?.[method]
  if (typeof fn !== 'function') {
    throw new Error(`Метод ApiBridge недоступен: ${method}`)
  }

  const result = await fn(...args)
  if (result && typeof result === 'object' && 'error' in result && (result as { error?: unknown }).error) {
    throw new Error(String((result as { error: unknown }).error))
  }
  return result as T
}

export type SessionInfo = {
  has_session?: boolean
  age_seconds?: number
  minutes_until_update?: number
  prolongation?: Record<string, unknown>
  error?: string
}

export type AuthState = {
  state?: string
  message?: string
  error?: string | null
  has_session?: boolean
  ready?: boolean
  minutes_until_update?: number
}

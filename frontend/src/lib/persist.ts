import { useCallback, useState, type Dispatch, type SetStateAction } from 'react'

export type OpsDates = {
  production: string
  expiration: string
  batch: string
}

export const OPS_DATES_KEY = 'kontur_ops_dates_v1'
export const OPS_PRINTER_KEY = 'kontur_ops_printer_v1'
export const OPS_AUTO_DOWNLOAD_KEY = 'kontur_ops_autodownload_v1'

const EMPTY_DATES: OpsDates = { production: '', expiration: '', batch: '' }

export function readPersisted<T>(key: string, fallback: T): T {
  if (typeof window === 'undefined') return fallback
  try {
    const raw = window.localStorage.getItem(key)
    if (raw == null) return fallback
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}

export function writePersisted<T>(key: string, value: T): void {
  if (typeof window === 'undefined') return
  window.localStorage.setItem(key, JSON.stringify(value))
}

export function usePersistedState<T>(key: string, initial: T): [T, Dispatch<SetStateAction<T>>] {
  const [value, setValue] = useState<T>(() => readPersisted(key, initial))

  const setPersisted = useCallback<Dispatch<SetStateAction<T>>>(
    (next) => {
      setValue((prev) => {
        const resolved = typeof next === 'function' ? (next as (current: T) => T)(prev) : next
        writePersisted(key, resolved)
        return resolved
      })
    },
    [key],
  )

  return [value, setPersisted]
}

export function useOpsDates(): [OpsDates, Dispatch<SetStateAction<OpsDates>>] {
  return usePersistedState<OpsDates>(OPS_DATES_KEY, EMPTY_DATES)
}

export function useOpsPrinter(): [string, Dispatch<SetStateAction<string>>] {
  return usePersistedState(OPS_PRINTER_KEY, '')
}

import { useCallback, useState, type Dispatch, type SetStateAction } from 'react'

/**
 * Кэш состояния страниц на время жизни приложения: при переключении разделов
 * данные таблиц не теряются — компонент монтируется сразу с прошлыми данными,
 * а свежие подтягиваются фоном.
 */
const cache = new Map<string, unknown>()

export function useCachedState<T>(key: string, initial: T): [T, Dispatch<SetStateAction<T>>] {
  const [value, setValue] = useState<T>(() => (cache.has(key) ? (cache.get(key) as T) : initial))

  const setCached = useCallback<Dispatch<SetStateAction<T>>>(
    (next) => {
      setValue((prev) => {
        const resolved = typeof next === 'function' ? (next as (p: T) => T)(prev) : next
        cache.set(key, resolved)
        return resolved
      })
    },
    [key],
  )

  return [value, setCached]
}

export function hasCachedState(key: string): boolean {
  return cache.has(key)
}

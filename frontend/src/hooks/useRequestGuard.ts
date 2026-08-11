import { useCallback, useEffect, useRef } from 'react'

/**
 * Защита от гонок async-запросов: «свежим» считается только последний begin.
 * Размонтирование тоже инвалидирует начатые запросы, чтобы устаревшие ответы
 * не писали state (и кэш страниц) после ухода с экрана.
 *
 * const guard = useRequestGuard()
 * const load = async () => {
 *   const fresh = guard()
 *   const data = await apiCall(...)
 *   if (!fresh()) return
 *   setState(data)
 * }
 */
export function useRequestGuard(): () => () => boolean {
  const genRef = useRef(0)

  useEffect(
    () => () => {
      genRef.current += 1
    },
    [],
  )

  return useCallback(() => {
    const gen = ++genRef.current
    return () => gen === genRef.current
  }, [])
}

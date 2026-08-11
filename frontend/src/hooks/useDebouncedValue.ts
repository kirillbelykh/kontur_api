import { useEffect, useState } from 'react'

/**
 * Отложенное значение для фильтрации больших таблиц: инпут обновляется
 * мгновенно, а дорогой фильтр пересчитывается после паузы ввода.
 */
export function useDebouncedValue<T>(value: T, delayMs = 150): T {
  const [debounced, setDebounced] = useState(value)

  useEffect(() => {
    const id = window.setTimeout(() => setDebounced(value), delayMs)
    return () => window.clearTimeout(id)
  }, [value, delayMs])

  return debounced
}

import { useCallback, useEffect, useState } from 'react'

const STORAGE_KEY = 'kontur_theme'

export type ThemeMode = 'light' | 'dark' | 'system'

function readTheme(): ThemeMode {
  const saved = window.localStorage.getItem(STORAGE_KEY)
  return saved === 'light' || saved === 'dark' || saved === 'system' ? saved : 'system'
}

export function useTheme() {
  const [theme, setThemeState] = useState<ThemeMode>(readTheme)
  const [isDark, setIsDark] = useState(false)

  useEffect(() => {
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const apply = () => {
      const shouldUseDark = theme === 'dark' || (theme === 'system' && media.matches)
      document.documentElement.classList.toggle('dark', shouldUseDark)
      setIsDark(shouldUseDark)
    }

    apply()
    media.addEventListener('change', apply)
    return () => media.removeEventListener('change', apply)
  }, [theme])

  const setTheme = useCallback((next: ThemeMode) => {
    window.localStorage.setItem(STORAGE_KEY, next)
    setThemeState(next)
  }, [])

  const toggleTheme = useCallback(() => {
    setTheme(isDark ? 'light' : 'dark')
  }, [isDark, setTheme])

  return { theme, isDark, setTheme, toggleTheme }
}

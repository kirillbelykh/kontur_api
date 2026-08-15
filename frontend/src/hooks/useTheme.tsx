import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react'
import { apiCall } from '@/lib/bridge'

const STORAGE_KEY = 'kontur_theme'

export type ThemeId = 'light' | 'ivory' | 'dark' | 'graphite' | 'ocean' | 'system'
export type ResolvedTheme = Exclude<ThemeId, 'system'>

export type ThemeOption = {
  id: ResolvedTheme
  label: string
  dark: boolean
  /** Цвета мини-превью в настройках (как карточки тем в VS Code) */
  preview: { bg: string; card: string; border: string; accent: string; text: string }
}

export const THEME_OPTIONS: ThemeOption[] = [
  {
    id: 'light',
    label: 'Светлая',
    dark: false,
    preview: { bg: '#f7f7f8', card: '#ffffff', border: '#d4d4d8', accent: '#3b82f6', text: '#18181b' },
  },
  {
    id: 'ivory',
    label: 'Слоновая кость',
    dark: false,
    preview: { bg: '#f4efe4', card: '#fdfbf5', border: '#d6cdb8', accent: '#a5652f', text: '#2b2218' },
  },
  {
    id: 'dark',
    label: 'Тёмная',
    dark: true,
    preview: { bg: '#18181b', card: '#202024', border: '#4b4b52', accent: '#3b82f6', text: '#dededf' },
  },
  {
    id: 'graphite',
    label: 'Графит',
    dark: true,
    preview: { bg: '#121212', card: '#1a1a1a', border: '#3d3d3d', accent: '#84a3c2', text: '#d6d6d6' },
  },
  {
    id: 'ocean',
    label: 'Океан',
    dark: true,
    preview: { bg: '#0d1723', card: '#16222f', border: '#2e4258', accent: '#3d9be0', text: '#d7dee6' },
  },
]

const VALID_IDS = new Set<string>([...THEME_OPTIONS.map((option) => option.id), 'system'])
const DARK_IDS = new Set<string>(THEME_OPTIONS.filter((option) => option.dark).map((option) => option.id))

function readTheme(): ThemeId {
  const saved = window.localStorage.getItem(STORAGE_KEY)
  return saved && VALID_IDS.has(saved) ? (saved as ThemeId) : 'system'
}

function resolveTheme(themeId: ThemeId): ResolvedTheme {
  if (themeId !== 'system') return themeId
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

/** Токены тем живут на html[data-theme]; dark-темы дополнительно ставят .dark для dark: утилит */
function applyTheme(resolved: ResolvedTheme) {
  const root = document.documentElement
  root.dataset.theme = resolved
  root.classList.toggle('dark', DARK_IDS.has(resolved))
  syncWindowChrome(resolved)
}

function syncWindowChrome(resolved: ResolvedTheme) {
  const option = THEME_OPTIONS.find((item) => item.id === resolved)
  if (!option) return
  void apiCall('set_window_chrome', {
    dark: option.dark,
    caption: option.preview.bg,
    text: option.preview.text,
  }).catch(() => {})
}

type ThemeContextValue = {
  themeId: ThemeId
  resolvedTheme: ResolvedTheme
  resolvedDark: boolean
  setTheme: (next: ThemeId) => void
}

const ThemeContext = createContext<ThemeContextValue | null>(null)

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [themeId, setThemeId] = useState<ThemeId>(() => {
    const initial = readTheme()
    // Синхронно до первого кадра — иначе тёмные темы стартуют белой вспышкой
    applyTheme(resolveTheme(initial))
    return initial
  })
  const [resolvedTheme, setResolvedTheme] = useState<ResolvedTheme>(() => resolveTheme(themeId))

  useEffect(() => {
    const media = window.matchMedia('(prefers-color-scheme: dark)')
    const apply = () => {
      const next = resolveTheme(themeId)
      setResolvedTheme(next)
      applyTheme(next)
    }
    apply()
    media.addEventListener('change', apply)
    return () => media.removeEventListener('change', apply)
  }, [themeId])

  const setTheme = useCallback((next: ThemeId) => {
    window.localStorage.setItem(STORAGE_KEY, next)
    setThemeId(next)
  }, [])

  const value = useMemo<ThemeContextValue>(
    () => ({ themeId, resolvedTheme, resolvedDark: DARK_IDS.has(resolvedTheme), setTheme }),
    [themeId, resolvedTheme, setTheme],
  )

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}

export function useTheme() {
  const context = useContext(ThemeContext)
  if (!context) throw new Error('useTheme must be used within ThemeProvider')
  return context
}

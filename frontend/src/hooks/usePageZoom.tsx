import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react'

const STORAGE_KEY = 'kontur_page_zoom_v1'
export const ZOOM_STEPS = [0.8, 0.9, 1, 1.1, 1.25, 1.5, 1.75, 2]
const DEFAULT_ZOOM = 1

function clampToStep(value: number) {
  return ZOOM_STEPS.reduce((best, step) =>
    Math.abs(step - value) < Math.abs(best - value) ? step : best,
  )
}

function readZoom(): number {
  if (typeof window === 'undefined') return DEFAULT_ZOOM
  const raw = Number(window.localStorage.getItem(STORAGE_KEY))
  return Number.isFinite(raw) && raw > 0 ? clampToStep(raw) : DEFAULT_ZOOM
}

type PageZoomValue = {
  zoom: number
  setZoom: (value: number) => void
  zoomIn: () => void
  zoomOut: () => void
  resetZoom: () => void
}

const PageZoomContext = createContext<PageZoomValue | null>(null)

export function PageZoomProvider({ children }: { children: ReactNode }) {
  const [zoom, setZoomState] = useState<number>(readZoom)

  const setZoom = useCallback((value: number) => {
    const next = clampToStep(value)
    setZoomState(next)
    window.localStorage.setItem(STORAGE_KEY, String(next))
  }, [])

  const shiftZoom = useCallback(
    (direction: 1 | -1) => {
      setZoomState((current) => {
        const index = ZOOM_STEPS.indexOf(clampToStep(current))
        const nextIndex = Math.min(ZOOM_STEPS.length - 1, Math.max(0, index + direction))
        const next = ZOOM_STEPS[nextIndex]
        window.localStorage.setItem(STORAGE_KEY, String(next))
        return next
      })
    },
    [],
  )

  const zoomIn = useCallback(() => shiftZoom(1), [shiftZoom])
  const zoomOut = useCallback(() => shiftZoom(-1), [shiftZoom])
  const resetZoom = useCallback(() => setZoom(DEFAULT_ZOOM), [setZoom])

  // `zoom` keeps layout math intact (unlike transform: scale) and Edge WebView2 supports it.
  useEffect(() => {
    document.documentElement.style.setProperty('zoom', String(zoom))
    return () => {
      document.documentElement.style.removeProperty('zoom')
    }
  }, [zoom])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (!event.ctrlKey && !event.metaKey) return
      if (event.key === '+' || event.key === '=' || event.code === 'NumpadAdd') {
        event.preventDefault()
        zoomIn()
      } else if (event.key === '-' || event.key === '_' || event.code === 'NumpadSubtract') {
        event.preventDefault()
        zoomOut()
      } else if (event.key === '0' || event.code === 'Numpad0') {
        event.preventDefault()
        resetZoom()
      }
    }

    const onWheel = (event: WheelEvent) => {
      if (!event.ctrlKey && !event.metaKey) return
      event.preventDefault()
      if (event.deltaY < 0) zoomIn()
      else if (event.deltaY > 0) zoomOut()
    }

    window.addEventListener('keydown', onKeyDown)
    window.addEventListener('wheel', onWheel, { passive: false })
    return () => {
      window.removeEventListener('keydown', onKeyDown)
      window.removeEventListener('wheel', onWheel)
    }
  }, [resetZoom, zoomIn, zoomOut])

  const value = useMemo(
    () => ({ zoom, setZoom, zoomIn, zoomOut, resetZoom }),
    [resetZoom, setZoom, zoom, zoomIn, zoomOut],
  )

  return <PageZoomContext.Provider value={value}>{children}</PageZoomContext.Provider>
}

export function usePageZoom() {
  const context = useContext(PageZoomContext)
  if (!context) throw new Error('usePageZoom must be used inside PageZoomProvider')
  return context
}

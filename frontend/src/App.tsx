import { Suspense, lazy, useEffect } from 'react'
import { HashRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Toaster } from 'sonner'
import { AlertTriangle, CheckCircle2, Info, Loader2, X, XCircle } from 'lucide-react'
import { ErrorBoundary } from '@/components/ErrorBoundary'
import { AppLayout } from '@/components/layout/AppLayout'
import { AppUpdateProvider } from '@/hooks/useAppUpdate'
import { PageZoomProvider } from '@/hooks/usePageZoom'
import { ThemeProvider } from '@/hooks/useTheme'
import { WelcomePage } from '@/pages/WelcomePage'

// Стартовый экран и каркас — сразу; рабочие страницы — отдельными чанками по требованию
const pageLoaders = [
  () => import('@/pages/OrdersPage'),
  () => import('@/pages/DownloadPage'),
  () => import('@/pages/IntroPage'),
  () => import('@/pages/TsdPage'),
  () => import('@/pages/AggregationPage'),
  () => import('@/pages/LabelsPage'),
] as const

const OrdersPage = lazy(() => pageLoaders[0]().then((m) => ({ default: m.OrdersPage })))
const DownloadPage = lazy(() => pageLoaders[1]().then((m) => ({ default: m.DownloadPage })))
const IntroPage = lazy(() => pageLoaders[2]().then((m) => ({ default: m.IntroPage })))
const TsdPage = lazy(() => pageLoaders[3]().then((m) => ({ default: m.TsdPage })))
const AggregationPage = lazy(() => pageLoaders[4]().then((m) => ({ default: m.AggregationPage })))
const LabelsPage = lazy(() => pageLoaders[5]().then((m) => ({ default: m.LabelsPage })))

/** После первой отрисовки греем ленивые чанки в простое — переходы мгновенные */
function usePreloadPages() {
  useEffect(() => {
    const warm = () => pageLoaders.forEach((load) => void load().catch(() => undefined))
    if (typeof window.requestIdleCallback === 'function') {
      const id = window.requestIdleCallback(warm, { timeout: 3000 })
      return () => window.cancelIdleCallback(id)
    }
    const id = window.setTimeout(warm, 1200)
    return () => window.clearTimeout(id)
  }, [])
}

export default function App() {
  usePreloadPages()
  return (
    <HashRouter>
      <ThemeProvider>
      <PageZoomProvider>
      <AppUpdateProvider>
        <ErrorBoundary>
          <Suspense fallback={null}>
            <Routes>
              <Route element={<AppLayout />}>
                <Route path="/" element={<Navigate to="/welcome" replace />} />
                <Route path="/welcome" element={<WelcomePage />} />
                <Route path="/orders" element={<OrdersPage />} />
                <Route path="/download" element={<DownloadPage />} />
                <Route path="/intro" element={<IntroPage />} />
                <Route path="/tsd" element={<TsdPage />} />
                <Route path="/aggregation" element={<AggregationPage />} />
                <Route path="/labels" element={<LabelsPage />} />
                <Route path="*" element={<Navigate to="/welcome" replace />} />
              </Route>
            </Routes>
          </Suspense>
        </ErrorBoundary>
        <Toaster
          position="top-right"
          closeButton
          expand
          gap={12}
          icons={{
            success: <CheckCircle2 className="h-4.5 w-4.5 text-emerald-600" />,
            error: <XCircle className="h-4.5 w-4.5 text-rose-600" />,
            warning: <AlertTriangle className="h-4.5 w-4.5 text-amber-600" />,
            info: <Info className="h-4.5 w-4.5 text-sky-600" />,
            loading: <Loader2 className="h-4.5 w-4.5 animate-spin text-muted-foreground" />,
            close: <X className="h-3.5 w-3.5" />,
          }}
          toastOptions={{
            unstyled: true,
            classNames: {
              toast:
                'kontur-toast group relative flex w-full items-center gap-3 rounded-lg border border-border bg-card py-3 pl-3 pr-10 text-sm text-foreground shadow-panel',
              title: 'font-medium leading-snug',
              description: 'mt-0.5 text-xs text-muted-foreground',
              icon: 'kontur-toast-icon',
              closeButton: 'kontur-toast-close',
            },
          }}
        />
      </AppUpdateProvider>
      </PageZoomProvider>
      </ThemeProvider>
    </HashRouter>
  )
}

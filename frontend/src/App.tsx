import { Suspense, lazy } from 'react'
import { HashRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Toaster } from 'sonner'
import { AlertTriangle, CheckCircle2, Info, Loader2, XCircle } from 'lucide-react'
import { ErrorBoundary } from '@/components/ErrorBoundary'
import { AppLayout } from '@/components/layout/AppLayout'
import { AppUpdateProvider } from '@/hooks/useAppUpdate'
import { PageZoomProvider } from '@/hooks/usePageZoom'
import { WelcomePage } from '@/pages/WelcomePage'

// Стартовый экран и каркас — сразу; рабочие страницы — отдельными чанками по требованию
const OrdersPage = lazy(() => import('@/pages/OrdersPage').then((m) => ({ default: m.OrdersPage })))
const DownloadPage = lazy(() => import('@/pages/DownloadPage').then((m) => ({ default: m.DownloadPage })))
const IntroPage = lazy(() => import('@/pages/IntroPage').then((m) => ({ default: m.IntroPage })))
const TsdPage = lazy(() => import('@/pages/TsdPage').then((m) => ({ default: m.TsdPage })))
const AggregationPage = lazy(() => import('@/pages/AggregationPage').then((m) => ({ default: m.AggregationPage })))
const LabelsPage = lazy(() => import('@/pages/LabelsPage').then((m) => ({ default: m.LabelsPage })))

export default function App() {
  return (
    <HashRouter>
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
    </HashRouter>
  )
}

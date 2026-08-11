import { HashRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Toaster } from 'sonner'
import { AlertTriangle, CheckCircle2, Info, Loader2, XCircle } from 'lucide-react'
import { AppLayout } from '@/components/layout/AppLayout'
import { AppUpdateProvider } from '@/hooks/useAppUpdate'
import { PageZoomProvider } from '@/hooks/usePageZoom'
import { AggregationPage } from '@/pages/AggregationPage'
import { DownloadPage } from '@/pages/DownloadPage'
import { IntroPage } from '@/pages/IntroPage'
import { LabelsPage } from '@/pages/LabelsPage'
import { OrdersPage } from '@/pages/OrdersPage'
import { TsdPage } from '@/pages/TsdPage'
import { WelcomePage } from '@/pages/WelcomePage'

export default function App() {
  return (
    <HashRouter>
      <PageZoomProvider>
      <AppUpdateProvider>
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
        <Toaster
          position="top-right"
          closeButton
          gap={10}
          icons={{
            success: <CheckCircle2 className="h-4 w-4 text-emerald-500" />,
            error: <XCircle className="h-4 w-4 text-rose-500" />,
            warning: <AlertTriangle className="h-4 w-4 text-amber-500" />,
            info: <Info className="h-4 w-4 text-sky-500" />,
            loading: <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />,
          }}
          toastOptions={{
            unstyled: true,
            classNames: {
              toast:
                'kontur-toast group flex w-full items-center gap-3 rounded-xl border border-border bg-card px-3.5 py-3 text-sm text-foreground shadow-panel',
              title: 'font-medium leading-tight',
              description: 'text-muted-foreground',
              icon: 'flex shrink-0 items-center',
              closeButton:
                'text-muted-foreground hover:text-foreground rounded-md border border-border bg-card',
            },
          }}
        />
      </AppUpdateProvider>
      </PageZoomProvider>
    </HashRouter>
  )
}

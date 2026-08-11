import { HashRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Toaster } from 'sonner'
import { AppLayout } from '@/components/layout/AppLayout'
import { AppUpdateProvider } from '@/hooks/useAppUpdate'
import { PageZoomProvider } from '@/hooks/usePageZoom'
import { AggregationPage } from '@/pages/AggregationPage'
import { ChzPage } from '@/pages/ChzPage'
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
            <Route path="/chz" element={<ChzPage />} />
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
          richColors
          closeButton
          toastOptions={{
            className: 'text-sm',
          }}
        />
      </AppUpdateProvider>
      </PageZoomProvider>
    </HashRouter>
  )
}

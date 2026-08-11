import { HashRouter, Navigate, Route, Routes } from 'react-router-dom'
import { Toaster } from 'sonner'
import { AppLayout } from '@/components/layout/AppLayout'
import { AggregationPage } from '@/pages/AggregationPage'
import { ChzPage } from '@/pages/ChzPage'
import { DownloadPage } from '@/pages/DownloadPage'
import { IntroPage } from '@/pages/IntroPage'
import { LabelsPage } from '@/pages/LabelsPage'
import { OrdersPage } from '@/pages/OrdersPage'
import { TsdPage } from '@/pages/TsdPage'

export default function App() {
  return (
    <HashRouter>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Navigate to="/orders" replace />} />
          <Route path="/orders" element={<OrdersPage />} />
          <Route path="/chz" element={<ChzPage />} />
          <Route path="/download" element={<DownloadPage />} />
          <Route path="/intro" element={<IntroPage />} />
          <Route path="/tsd" element={<TsdPage />} />
          <Route path="/aggregation" element={<AggregationPage />} />
          <Route path="/labels" element={<LabelsPage />} />
          <Route path="*" element={<Navigate to="/orders" replace />} />
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
    </HashRouter>
  )
}

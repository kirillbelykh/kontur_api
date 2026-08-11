import { useCallback, useEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { ArrowRight, Download, ScanLine } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { DiaTextReveal } from '@/components/ui/dia-text-reveal'
import { useAppUpdate } from '@/hooks/useAppUpdate'

const BRAND_COLORS = ['#22d3ee', '#818cf8', '#f472b6', '#34d399']
const REVEAL_DURATION_SEC = 1.8
const GREETING_HOLD_MS = 900

export function WelcomePage() {
  const navigate = useNavigate()
  const { updateAvailable, applying, applyUpdate, remoteShort } = useAppUpdate()
  const [showGreeting, setShowGreeting] = useState(false)
  const [greetingKey, setGreetingKey] = useState(0)

  const finishGreeting = useCallback(() => {
    navigate('/orders', { replace: true })
  }, [navigate])

  const start = () => {
    setGreetingKey((key) => key + 1)
    setShowGreeting(true)
  }

  useEffect(() => {
    window.sessionStorage.setItem('kontur_welcome_seen_v1', '1')
  }, [])

  return (
    <main className="relative flex min-h-[calc(100vh-4rem)] items-center justify-center bg-background px-4 py-8">
      <Card className="w-full max-w-md shadow-panel">
        <CardHeader className="items-center text-center">
          <div className="mb-2 flex h-12 w-12 items-center justify-center rounded-2xl bg-muted">
            <ScanLine className="h-6 w-6 text-primary" aria-hidden />
          </div>
          <CardTitle className="text-xl">Контур Маркировка</CardTitle>
          <CardDescription>
            Desktop-приложение Grundlage для заказов кодов, ввода в оборот и печати этикеток
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="flex justify-center pt-1">
            <Button className="min-w-[11rem] px-8" onClick={start} disabled={showGreeting}>
              Начать работу
              <ArrowRight className="h-4 w-4" />
            </Button>
          </div>
          {updateAvailable ? (
            <div className="flex justify-center">
              <Button
                variant="warning"
                className="min-w-[11rem] px-8"
                disabled={applying}
                onClick={() => void applyUpdate()}
              >
                <Download className="h-4 w-4" />
                {applying ? 'Обновляем…' : `Обновить${remoteShort ? ` · ${remoteShort}` : ''}`}
              </Button>
            </div>
          ) : null}
          <p className="text-center text-xs text-muted-foreground">
            Или откройте раздел из меню слева
          </p>
        </CardContent>
      </Card>

      {typeof document !== 'undefined'
        ? createPortal(
            <AnimatePresence>
              {showGreeting ? (
                <motion.div
                  key={`welcome-greeting-${greetingKey}`}
                  className="fixed inset-0 z-[10000] flex items-center justify-center bg-background px-6"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
                >
                  <DiaTextReveal
                    key={greetingKey}
                    className="text-center text-4xl font-bold tracking-tight sm:text-5xl"
                    colors={BRAND_COLORS}
                    text="КОНТУР МАРКИРОВКА"
                    textColor="hsl(var(--foreground))"
                    startOnView={false}
                    once={false}
                    duration={REVEAL_DURATION_SEC}
                    onComplete={() => {
                      window.setTimeout(finishGreeting, GREETING_HOLD_MS)
                    }}
                  />
                </motion.div>
              ) : null}
            </AnimatePresence>,
            document.body,
          )
        : null}
    </main>
  )
}

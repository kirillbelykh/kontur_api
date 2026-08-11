import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { DiaTextReveal } from '@/components/ui/dia-text-reveal'

const BRAND_COLORS = ['#22d3ee', '#818cf8', '#f472b6', '#34d399']
const REVEAL_DURATION_SEC = 2.2
const REVEAL_DELAY_SEC = 0.25
const GREETING_HOLD_MS = 1300

export function WelcomePage() {
  const navigate = useNavigate()
  const [visible, setVisible] = useState(true)
  // Первый кадр WebView может ещё рисовать белый экран — не запускаем sweep,
  // пока страница реально не отрисована, иначе видно только хвост анимации.
  const [painted, setPainted] = useState(false)

  useEffect(() => {
    let raf2 = 0
    const raf1 = requestAnimationFrame(() => {
      raf2 = requestAnimationFrame(() => setPainted(true))
    })
    return () => {
      cancelAnimationFrame(raf1)
      cancelAnimationFrame(raf2)
    }
  }, [])

  const finishGreeting = useCallback(() => {
    navigate('/orders', { replace: true })
  }, [navigate])

  return (
    <main className="relative min-h-[calc(100vh-3rem)] bg-background">
      <AnimatePresence onExitComplete={finishGreeting}>
        {visible ? (
          <motion.div
            className="fixed inset-0 z-[10000] flex items-center justify-center bg-background px-6"
            initial={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
          >
            {painted ? (
              <DiaTextReveal
                className="text-center text-4xl font-bold tracking-tight sm:text-5xl"
                colors={BRAND_COLORS}
                text="КОНТУР МАРКИРОВКА"
                textColor="var(--foreground)"
                startOnView={false}
                once={false}
                duration={REVEAL_DURATION_SEC}
                delay={REVEAL_DELAY_SEC}
                onComplete={() => {
                  window.setTimeout(() => setVisible(false), GREETING_HOLD_MS)
                }}
              />
            ) : null}
          </motion.div>
        ) : null}
      </AnimatePresence>
    </main>
  )
}

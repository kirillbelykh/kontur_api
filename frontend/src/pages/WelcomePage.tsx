import { useCallback, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { DiaTextReveal } from '@/components/ui/dia-text-reveal'

const BRAND_COLORS = ['#22d3ee', '#818cf8', '#f472b6', '#34d399']
const REVEAL_DURATION_SEC = 1.8
const GREETING_HOLD_MS = 900

export function WelcomePage() {
  const navigate = useNavigate()
  const [visible, setVisible] = useState(true)

  const finishGreeting = useCallback(() => {
    navigate('/orders', { replace: true })
  }, [navigate])

  return (
    <main className="relative min-h-[calc(100vh-4rem)] bg-background">
      <AnimatePresence onExitComplete={finishGreeting}>
        {visible ? (
          <motion.div
            className="fixed inset-0 z-[10000] flex items-center justify-center bg-background px-6"
            initial={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.35, ease: [0.22, 1, 0.36, 1] }}
          >
            <DiaTextReveal
              className="text-center text-4xl font-bold tracking-tight sm:text-5xl"
              colors={BRAND_COLORS}
              text="КОНТУР МАРКИРОВКА"
              textColor="hsl(var(--foreground))"
              startOnView={false}
              once={false}
              duration={REVEAL_DURATION_SEC}
              onComplete={() => {
                window.setTimeout(() => setVisible(false), GREETING_HOLD_MS)
              }}
            />
          </motion.div>
        ) : null}
      </AnimatePresence>
    </main>
  )
}

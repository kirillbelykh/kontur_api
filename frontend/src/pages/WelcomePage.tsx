import { useCallback, useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import { DiaTextReveal } from '@/components/ui/dia-text-reveal'
import { Meteors } from '@/components/ui/meteors'

const BRAND_COLORS = ['#22d3ee', '#818cf8', '#f472b6', '#34d399']
const REVEAL_DURATION_SEC = 2.2
const REVEAL_DELAY_SEC = 0.25
const GREETING_HOLD_MS = 1300

export function WelcomePage() {
  const navigate = useNavigate()
  const [visible, setVisible] = useState(true)
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

  const skipGreeting = useCallback(() => setVisible(false), [])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' || event.key === 'Enter') {
        event.preventDefault()
        skipGreeting()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [skipGreeting])

  return (
    <main className="fixed inset-0 z-[10000] bg-background">
      <AnimatePresence onExitComplete={finishGreeting}>
        {visible ? (
          <motion.div
            onClick={skipGreeting}
            className="absolute inset-0 overflow-hidden bg-background"
            initial={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
          >
            <div className="pointer-events-none absolute inset-0 overflow-hidden">
              <Meteors number={22} minDelay={0} maxDelay={0.35} minDuration={7} maxDuration={13} />
            </div>
            <div className="relative z-10 flex h-full items-center justify-center px-6">
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
            </div>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </main>
  )
}

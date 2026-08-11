import { useEffect, useRef, useState } from 'react'
import {
  animate,
  motion,
  useInView,
  useMotionValue,
  useReducedMotion,
  useTransform,
  type HTMLMotionProps,
} from 'framer-motion'

import { cn } from '@/lib/utils'

const DEFAULT_COLORS = ['#c679c4', '#fa3d1d', '#ffb005', '#e1e1fe', '#0358f7']
const BAND_HALF = 17
const SWEEP_START = -BAND_HALF
const SWEEP_END = 100 + BAND_HALF

const sweepEase = (t: number) => (t < 0.5 ? 4 * t ** 3 : 1 - (-2 * t + 2) ** 3 / 2)

function buildGradient(pos: number, colors: string[], textColor: string) {
  const bandStart = pos - BAND_HALF
  const bandEnd = pos + BAND_HALF

  if (bandStart >= 100) {
    return `linear-gradient(90deg, ${textColor}, ${textColor})`
  }
  const n = colors.length
  const parts: string[] = []

  if (bandStart > 0) parts.push(`${textColor} 0%`, `${textColor} ${bandStart.toFixed(2)}%`)

  colors.forEach((c, i) => {
    const pct = n === 1 ? pos : bandStart + (i / (n - 1)) * BAND_HALF * 2
    parts.push(`${c} ${pct.toFixed(2)}%`)
  })

  if (bandEnd < 100) parts.push(`transparent ${bandEnd.toFixed(2)}%`, `transparent 100%`)

  return `linear-gradient(90deg, ${parts.join(', ')})`
}

function measureWidths(el: HTMLElement, texts: string[]) {
  const ghost = el.cloneNode() as HTMLElement
  Object.assign(ghost.style, {
    position: 'absolute',
    visibility: 'hidden',
    pointerEvents: 'none',
    width: 'auto',
    whiteSpace: 'nowrap',
  })
  el.parentElement!.appendChild(ghost)
  const widths = texts.map((t) => {
    ghost.textContent = t
    return ghost.getBoundingClientRect().width
  })
  ghost.remove()
  return widths
}

export interface DiaTextRevealProps
  extends Omit<
    HTMLMotionProps<'span'>,
    'ref' | 'children' | 'style' | 'animate' | 'transition' | 'color'
  > {
  text: string | string[]
  colors?: string[]
  textColor?: string
  duration?: number
  delay?: number
  repeat?: boolean
  repeatDelay?: number
  startOnView?: boolean
  once?: boolean
  className?: string
  fixedWidth?: boolean
  /** Called when the sweep finishes (single-play mode). */
  onComplete?: () => void
}

export function DiaTextReveal({
  text,
  colors = DEFAULT_COLORS,
  textColor = 'var(--foreground)',
  duration = 1.5,
  delay = 0,
  repeat = false,
  repeatDelay = 0.5,
  startOnView = true,
  once = true,
  className,
  fixedWidth = false,
  onComplete,
  ...props
}: DiaTextRevealProps) {
  const texts = Array.isArray(text) ? text : [text]
  const isMulti = texts.length > 1
  const prefersReducedMotion = useReducedMotion()

  const spanRef = useRef<HTMLSpanElement>(null)
  const optsRef = useRef({
    colors,
    textColor,
    duration,
    delay,
    repeat,
    repeatDelay,
    texts,
    onComplete,
  })
  optsRef.current = {
    colors,
    textColor,
    duration,
    delay,
    repeat,
    repeatDelay,
    texts,
    onComplete,
  }

  const indexRef = useRef(0)
  const hasPlayedRef = useRef(false)
  const completedRef = useRef(false)
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined)
  const playRef = useRef<() => void>(() => {})
  const stopRef = useRef<(() => void) | null>(null)

  const [activeIndex, setActiveIndex] = useState(0)
  const [measuredWidths, setMeasuredWidths] = useState<number[]>([])

  const sweepPos = useMotionValue(SWEEP_START)

  const backgroundImage = useTransform(sweepPos, (pos) =>
    buildGradient(pos, optsRef.current.colors, optsRef.current.textColor),
  )

  const isInView = useInView(spanRef, { once, amount: 0.1 })

  useEffect(() => {
    const el = spanRef.current
    if (!el || !isMulti) return
    setMeasuredWidths(measureWidths(el, texts))
    // eslint-disable-next-line react-hooks/exhaustive-deps -- measure when text content changes
  }, [Array.isArray(text) ? text.join('\0') : text, isMulti])

  playRef.current = () => {
    const opts = optsRef.current

    sweepPos.set(SWEEP_START)

    const controls = animate(sweepPos, SWEEP_END, {
      duration: opts.duration,
      delay: opts.delay,
      ease: sweepEase,
      onComplete() {
        if (opts.repeat) {
          timerRef.current = setTimeout(() => {
            const next = (indexRef.current + 1) % opts.texts.length
            indexRef.current = next
            setActiveIndex(next)
            playRef.current()
          }, opts.repeatDelay * 1000)
          return
        }
        if (!completedRef.current) {
          completedRef.current = true
          opts.onComplete?.()
        }
      },
    })

    stopRef.current = () => controls.stop()
  }

  useEffect(() => {
    if (prefersReducedMotion) {
      sweepPos.set(SWEEP_END)
      if (!completedRef.current) {
        completedRef.current = true
        optsRef.current.onComplete?.()
      }
      return
    }
    if (startOnView && !isInView) return
    if (once && hasPlayedRef.current) return

    hasPlayedRef.current = true
    playRef.current()

    return () => {
      stopRef.current?.()
      clearTimeout(timerRef.current)
      // React Strict Mode: cleanup + re-run on same mount — allow replay
      if (!completedRef.current) {
        hasPlayedRef.current = false
      }
    }
  }, [isInView, startOnView, once, prefersReducedMotion, sweepPos])

  const fixedW =
    isMulti && fixedWidth && measuredWidths.length > 0 ? Math.max(...measuredWidths) : undefined

  const animatedW =
    isMulti && !fixedWidth && measuredWidths[activeIndex] != null
      ? measuredWidths[activeIndex]
      : undefined

  return (
    <motion.span
      ref={spanRef}
      className={cn('align-bottom leading-[100%] text-inherit', className)}
      style={{
        transform: 'translateY(-2px)',
        color: 'transparent',
        backgroundClip: 'text',
        WebkitBackgroundClip: 'text',
        backgroundSize: '100% 100%',
        backgroundImage,
        ...(isMulti && {
          display: 'inline-block',
          overflow: 'hidden',
          whiteSpace: 'nowrap',
          verticalAlign: 'text-center' as const,
          ...(fixedW != null && { width: fixedW }),
        }),
      }}
      animate={animatedW != null ? { width: animatedW } : undefined}
      transition={{ duration: 0.4, ease: [0.4, 0, 0.2, 1] }}
      {...props}
    >
      {texts[activeIndex]}
    </motion.span>
  )
}

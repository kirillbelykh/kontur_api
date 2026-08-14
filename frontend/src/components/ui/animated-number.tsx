import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'
import { useAppSetting } from '@/lib/app-settings'
import { cn } from '@/lib/utils'

/**
 * «Number pop-in» с transitions.dev: изменившиеся символы вылетают вверх с blur,
 * новые въезжают снизу со стаггером. Позиции без изменений не анимируются.
 */
export function AnimatedNumber({
  value,
  className,
}: {
  value: string | number
  className?: string
}) {
  const reduceMotion = useReducedMotion()
  const animations = useAppSetting('animations')
  const text = String(value ?? '')

  if (reduceMotion || !animations || text.length > 16) {
    return <span className={cn('tabular-nums', className)}>{text}</span>
  }

  return (
    <span className={cn('inline-flex overflow-hidden align-bottom tabular-nums', className)} aria-label={text} role="text">
      <AnimatePresence mode="popLayout" initial={false}>
        {text.split('').map((char, index) => (
          <motion.span
            key={`${index}-${char}`}
            initial={{ y: '0.7em', opacity: 0, filter: 'blur(4px)' }}
            animate={{ y: 0, opacity: 1, filter: 'blur(0px)' }}
            exit={{ y: '-0.7em', opacity: 0, filter: 'blur(4px)' }}
            transition={{
              type: 'spring',
              stiffness: 460,
              damping: 34,
              mass: 0.7,
              delay: index * 0.022,
            }}
            className="inline-block"
            aria-hidden
          >
            {char === ' ' ? '\u00A0' : char}
          </motion.span>
        ))}
      </AnimatePresence>
    </span>
  )
}

/** «Text states swap»: смена текста целиком — старый вверх с blur, новый снизу. */
export function AnimatedTextSwap({
  text,
  className,
}: {
  text: string
  className?: string
}) {
  const reduceMotion = useReducedMotion()
  const animations = useAppSetting('animations')
  if (reduceMotion || !animations) return <span className={className}>{text}</span>

  return (
    <span className={cn('relative inline-grid overflow-hidden align-bottom', className)}>
      <AnimatePresence initial={false}>
        <motion.span
          key={text}
          initial={{ y: '0.6em', opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: '-0.6em', opacity: 0 }}
          transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
          className="col-start-1 row-start-1 whitespace-nowrap"
        >
          {text}
        </motion.span>
      </AnimatePresence>
    </span>
  )
}

import { useState } from 'react'
import { cn } from '@/lib/utils'

type MeteorsProps = {
  number?: number
  minDelay?: number
  maxDelay?: number
  minDuration?: number
  maxDuration?: number
  className?: string
}

/** Падают сверху за край окна; до старта анимации на экране их нет. */
export function Meteors({
  number = 20,
  minDelay = 0.15,
  maxDelay = 1.3,
  minDuration = 7,
  maxDuration = 13,
  className,
}: MeteorsProps) {
  const [meteorStyles] = useState(() =>
    Array.from({ length: number }, () => ({
      top: `${-12 - Math.random() * 18}%`,
      left: `${8 + Math.random() * 84}%`,
      animationDelay: `${Math.random() * (maxDelay - minDelay) + minDelay}s`,
      animationDuration: `${(Math.random() * (maxDuration - minDuration) + minDuration).toFixed(2)}s`,
    })),
  )

  return (
    <>
      {meteorStyles.map((style, index) => (
        <span
          key={index}
          className={cn('meteor-origin pointer-events-none absolute', className)}
          style={{ top: style.top, left: style.left }}
        >
          <span
            className="meteor-fly"
            style={{
              animationDelay: style.animationDelay,
              animationDuration: style.animationDuration,
            }}
          >
            <span className="meteor-streak">
              <span className="meteor-head" />
              <span className="meteor-tail" />
            </span>
          </span>
        </span>
      ))}
    </>
  )
}

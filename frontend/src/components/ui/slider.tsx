import { Label, Slider as HeroSlider } from '@heroui/react'
import { cn } from '@/lib/utils'

type SliderProps = {
  value?: number
  defaultValue?: number
  onChange?: (value: number) => void
  min?: number
  max?: number
  step?: number
  label?: string
  /** Формат значения в Output, по умолчанию число как есть */
  formatOutput?: (value: number) => string
  className?: string
  isDisabled?: boolean
  'aria-label'?: string
}

function toNumber(next: number | number[]): number {
  return Array.isArray(next) ? (next[0] ?? 0) : next
}

export function Slider({
  value,
  defaultValue,
  onChange,
  min = 0,
  max = 100,
  step = 1,
  label,
  formatOutput,
  className,
  isDisabled,
  'aria-label': ariaLabel,
}: SliderProps) {
  const controlled = value !== undefined

  return (
    <HeroSlider
      aria-label={ariaLabel ?? (label ? undefined : 'Значение')}
      className={cn('w-full', className)}
      defaultValue={controlled ? undefined : defaultValue}
      isDisabled={isDisabled}
      maxValue={max}
      minValue={min}
      step={step}
      value={controlled ? value : undefined}
      onChange={(next) => onChange?.(toNumber(next))}
    >
      {label ? <Label>{label}</Label> : null}
      <HeroSlider.Output>
        {formatOutput
          ? ({ state }) => formatOutput(state.values[0] ?? 0)
          : undefined}
      </HeroSlider.Output>
      <HeroSlider.Track>
        <HeroSlider.Fill />
        <HeroSlider.Thumb />
      </HeroSlider.Track>
    </HeroSlider>
  )
}

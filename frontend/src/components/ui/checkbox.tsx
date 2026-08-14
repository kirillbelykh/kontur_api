import {
  Checkbox as HeroCheckbox,
  CheckboxGroup,
  Description,
  Label,
} from '@heroui/react'
import { useEffect, useRef, useState, type ComponentProps, type MouseEventHandler, type ReactNode } from 'react'
import { cn } from '@/lib/utils'

type HeroCheckboxProps = ComponentProps<typeof HeroCheckbox>

export type CheckboxProps = Omit<HeroCheckboxProps, 'children'> & {
  /** Текст рядом с контролом (кликабельный label) */
  children?: ReactNode
  /** Подпись под label — как Description в HeroUI docs */
  description?: ReactNode
  /** Клик по Content (нужен для shift-select в таблицах) */
  onContentClick?: MouseEventHandler<HTMLElement>
}

/**
 * HeroUI Checkbox с готовой анатомией Content/Control/Indicator.
 * Выглядит как в docs: label + опциональный Description под ним.
 * Controlled-чекбоксы отмечаются на pointerdown, не дожидаясь pointerup/перерисовки родителя.
 */
export function Checkbox({
  children,
  description,
  className,
  onContentClick,
  isSelected,
  onChange,
  isDisabled,
  ...props
}: CheckboxProps) {
  const fromPointer = useRef(false)
  const [optimistic, setOptimistic] = useState(isSelected)
  const controlled = isSelected !== undefined

  useEffect(() => {
    setOptimistic(isSelected)
  }, [isSelected])

  return (
    <HeroCheckbox
      className={cn(className)}
      isDisabled={isDisabled}
      {...(controlled ? { isSelected: optimistic } : {})}
      onChange={(value) => {
        if (fromPointer.current) {
          fromPointer.current = false
          return
        }
        if (controlled) setOptimistic(value)
        onChange?.(value)
      }}
      {...props}
    >
      <HeroCheckbox.Content
        onClick={onContentClick}
        onPointerDown={(event) => {
          if (event.button !== 0 || isDisabled || !controlled) return
          fromPointer.current = true
          const next = !isSelected
          setOptimistic(next)
          onChange?.(next)
        }}
      >
        <HeroCheckbox.Control>
          <HeroCheckbox.Indicator />
        </HeroCheckbox.Control>
        {children}
      </HeroCheckbox.Content>
      {description != null ? <Description>{description}</Description> : null}
    </HeroCheckbox>
  )
}

Checkbox.displayName = 'Checkbox'

export { CheckboxGroup, Description, Label }

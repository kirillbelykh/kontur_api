import {
  Checkbox as HeroCheckbox,
  CheckboxGroup,
  Description,
  Label,
} from '@heroui/react'
import type { ComponentProps, MouseEventHandler, ReactNode } from 'react'
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
 */
export function Checkbox({
  children,
  description,
  className,
  onContentClick,
  ...props
}: CheckboxProps) {
  return (
    <HeroCheckbox className={cn(className)} {...props}>
      <HeroCheckbox.Content onClick={onContentClick}>
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

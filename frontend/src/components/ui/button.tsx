import * as React from 'react'
import { Button as HeroButton } from '@heroui/react'
import { Slot } from '@radix-ui/react-slot'
import { cn } from '@/lib/utils'

type HeroButtonProps = React.ComponentProps<typeof HeroButton>
type HeroVariant = NonNullable<HeroButtonProps['variant']>
type HeroSize = NonNullable<HeroButtonProps['size']>

/** Проектные варианты поверх HeroUI (success/warning/default — совместимость) */
export type ButtonVariant =
  | 'default'
  | 'primary'
  | 'secondary'
  | 'tertiary'
  | 'success'
  | 'warning'
  | 'danger'
  | 'danger-soft'
  | 'ghost'
  | 'outline'

export type ButtonSize = 'sm' | 'md' | 'lg' | 'icon'

const VARIANT_MAP: Record<ButtonVariant, HeroVariant> = {
  default: 'primary',
  primary: 'primary',
  secondary: 'secondary',
  tertiary: 'tertiary',
  outline: 'outline',
  ghost: 'ghost',
  danger: 'danger',
  'danger-soft': 'danger-soft',
  success: 'primary',
  warning: 'secondary',
}

const VARIANT_CLASS: Partial<Record<ButtonVariant, string>> = {
  success:
    '[--button-bg:var(--success)] [--button-bg-hover:var(--success-hover)] [--button-bg-pressed:var(--success-hover)] [--button-fg:var(--success-foreground)]',
  warning:
    '[--button-bg:var(--warning)] [--button-bg-hover:var(--warning-hover)] [--button-bg-pressed:var(--warning-hover)] [--button-fg:var(--warning-foreground)]',
}

export type ButtonProps = Omit<HeroButtonProps, 'variant' | 'size' | 'children'> & {
  variant?: ButtonVariant
  size?: ButtonSize
  asChild?: boolean
  children?: React.ReactNode
  /** Native tooltip — RAC Button его не пробрасывает */
  title?: string
  /** @deprecated используй isDisabled — оставлено для совместимости */
  disabled?: boolean
  /** @deprecated предпочтительно onPress; onClick сохраняем для существующих экранов */
  onClick?: React.MouseEventHandler<HTMLButtonElement>
}

/**
 * HeroUI Button (радиус как у полей — rounded-field / --field-radius).
 * Совместимость: variant="default"|"outline"|…, size="icon", disabled, onClick, asChild.
 */
export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      className,
      variant = 'default',
      size = 'md',
      asChild = false,
      disabled,
      isDisabled,
      isIconOnly,
      onClick,
      onPress,
      title,
      children,
      ...props
    },
    ref,
  ) => {
    const heroVariant = VARIANT_MAP[variant]
    const iconOnly = isIconOnly || size === 'icon'
    const heroSize: HeroSize = size === 'icon' ? 'md' : size

    const handlePress: HeroButtonProps['onPress'] = (event) => {
      onPress?.(event)
      onClick?.({
        stopPropagation: () => undefined,
        preventDefault: () => undefined,
        ...event,
      } as unknown as React.MouseEvent<HTMLButtonElement>)
    }

    const shared = {
      ref,
      className: cn(VARIANT_CLASS[variant], className),
      variant: heroVariant,
      size: heroSize,
      isIconOnly: iconOnly,
      isDisabled: isDisabled ?? disabled,
      onPress: onPress || onClick ? handlePress : undefined,
      title,
      ...props,
    }

    if (asChild) {
      return (
        <HeroButton
          {...shared}
          render={(domProps) => <Slot {...domProps}>{children}</Slot>}
        />
      )
    }

    return <HeroButton {...shared}>{children}</HeroButton>
  },
)
Button.displayName = 'Button'

/** Совместимость со старым cva-экспортом: классы задаёт HeroUI */
export function buttonVariants(_opts?: {
  variant?: ButtonVariant | null
  size?: ButtonSize | null
  className?: string
}) {
  return cn(
    'button',
    _opts?.variant ? VARIANT_CLASS[_opts.variant] : undefined,
    _opts?.className,
  )
}

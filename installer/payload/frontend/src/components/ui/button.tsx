import * as React from 'react'
import { cn } from '@/lib/utils'

type ButtonVariant = 'default' | 'secondary' | 'outline' | 'ghost' | 'success' | 'warning' | 'danger'
type ButtonSize = 'sm' | 'md' | 'lg' | 'icon'

const variantClass: Record<ButtonVariant, string> = {
  default: 'bg-primary text-primary-foreground hover:bg-slate-600',
  secondary: 'bg-secondary text-secondary-foreground hover:bg-slate-600',
  outline: 'border border-border bg-transparent hover:bg-muted',
  ghost: 'bg-transparent hover:bg-muted',
  success: 'bg-success text-white hover:bg-emerald-600',
  warning: 'bg-warning text-slate-950 hover:bg-amber-600',
  danger: 'bg-error text-white hover:bg-rose-600',
}

const sizeClass: Record<ButtonSize, string> = {
  sm: 'h-8 px-3 text-xs',
  md: 'h-9 px-3.5 text-sm',
  lg: 'h-11 px-5 text-sm',
  icon: 'h-9 w-9',
}

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = 'default', size = 'md', type = 'button', ...props }, ref) => {
    return (
      <button
        ref={ref}
        type={type}
        className={cn(
          'focus-ring inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md font-medium transition disabled:pointer-events-none disabled:opacity-50',
          variantClass[variant],
          sizeClass[size],
          className,
        )}
        {...props}
      />
    )
  },
)
Button.displayName = 'Button'

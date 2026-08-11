import * as React from 'react'
import { Input as HeroInput, TextArea as HeroTextArea } from '@heroui/react'
import { cn } from '@/lib/utils'

type HeroInputProps = React.ComponentProps<typeof HeroInput>
type HeroTextAreaProps = React.ComponentProps<typeof HeroTextArea>

export const Input = React.forwardRef<HTMLInputElement, HeroInputProps>(
  ({ className, fullWidth = true, variant = 'primary', ...props }, ref) => (
    <HeroInput
      ref={ref}
      className={className}
      fullWidth={fullWidth}
      variant={variant}
      {...props}
    />
  ),
)
Input.displayName = 'Input'

export const Textarea = React.forwardRef<HTMLTextAreaElement, HeroTextAreaProps>(
  ({ className, fullWidth = true, variant = 'primary', ...props }, ref) => (
    <HeroTextArea
      ref={ref}
      className={cn('min-h-24', className)}
      fullWidth={fullWidth}
      variant={variant}
      {...props}
    />
  ),
)
Textarea.displayName = 'Textarea'

export { SelectNative } from '@/components/ui/select'

import { Input, Label, SearchField } from '@heroui/react'
import type { ComponentProps, ReactNode } from 'react'
import { cn } from '@/lib/utils'

/** Подпись поля — HeroUI Label (как в доке: sentence case, medium). */
export function FieldLabel({ children, className }: { children: ReactNode; className?: string }) {
  return <Label className={cn('mb-1.5 block', className)}>{children}</Label>
}

export type TextInputProps = ComponentProps<typeof Input>

/** Текстовое поле — HeroUI Input на всю ширину. */
export function TextInput({ className, ...props }: TextInputProps) {
  return <Input fullWidth className={cn(className)} {...props} />
}

/** Поиск по таблице — HeroUI SearchField с иконкой и кнопкой очистки. */
export function TableSearch({
  value,
  onChange,
  placeholder = 'Поиск',
  className,
}: {
  value: string
  onChange: (value: string) => void
  placeholder?: string
  className?: string
}) {
  return (
    <SearchField
      aria-label={placeholder}
      value={value}
      onChange={onChange}
      fullWidth
      className={cn(className)}
    >
      <SearchField.Group>
        <SearchField.SearchIcon />
        <SearchField.Input placeholder={placeholder} />
        <SearchField.ClearButton />
      </SearchField.Group>
    </SearchField>
  )
}

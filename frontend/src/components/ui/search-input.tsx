import { Label, SearchField } from '@heroui/react'
import { cn } from '@/lib/utils'

type SearchInputProps = {
  value?: string
  defaultValue?: string
  onChange?: (value: string) => void
  placeholder?: string
  name?: string
  label?: string
  'aria-label'?: string
  className?: string
  groupClassName?: string
  inputClassName?: string
  fullWidth?: boolean
}

export function SearchInput({
  value,
  defaultValue,
  onChange,
  placeholder = 'Поиск...',
  name = 'search',
  label,
  'aria-label': ariaLabel,
  className,
  groupClassName,
  inputClassName,
  fullWidth = true,
}: SearchInputProps) {
  return (
    <SearchField
      aria-label={ariaLabel ?? (label ? undefined : placeholder)}
      className={cn(fullWidth && 'w-full', className)}
      defaultValue={defaultValue}
      fullWidth={fullWidth}
      name={name}
      value={value}
      onChange={onChange}
    >
      {label ? <Label>{label}</Label> : null}
      <SearchField.Group className={cn(fullWidth && 'w-full', groupClassName)}>
        <SearchField.SearchIcon />
        <SearchField.Input className={cn(fullWidth && 'w-full', inputClassName)} placeholder={placeholder} />
        <SearchField.ClearButton />
      </SearchField.Group>
    </SearchField>
  )
}

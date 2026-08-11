import { useEffect, useRef, useState } from 'react'
import { Calendar, DateField, DatePicker as HeroDatePicker, Label } from '@heroui/react'
import { CalendarDate, type DateValue } from '@internationalized/date'
import { cn } from '@/lib/utils'

/** The bridge parses DD-MM-YYYY, so that is the string shape we hand back. */
const DISPLAY = /^(\d{2})-(\d{2})-(\d{4})$/
const ISO = /^(\d{4})-(\d{2})-(\d{2})$/

export function parseDateValue(value?: string): CalendarDate | null {
  const text = String(value || '').trim()
  const display = DISPLAY.exec(text)
  if (display) {
    return new CalendarDate(Number(display[3]), Number(display[2]), Number(display[1]))
  }
  const iso = ISO.exec(text)
  if (iso) {
    return new CalendarDate(Number(iso[1]), Number(iso[2]), Number(iso[3]))
  }
  return null
}

export function formatDateValue(value: DateValue | null): string {
  if (!value) return ''
  const day = String(value.day).padStart(2, '0')
  const month = String(value.month).padStart(2, '0')
  // Year is padded so partially typed years ("2" while entering "2026") still
  // round-trip through parseDateValue instead of wiping the field.
  const year = String(value.year).padStart(4, '0')
  return `${day}-${month}-${year}`
}

export function DatePickerField({
  label,
  value,
  onChange,
  isDisabled,
  className,
}: {
  label?: string
  value?: string
  onChange: (value: string) => void
  isDisabled?: boolean
  className?: string
}) {
  // While the user types, React Aria's own state is authoritative: feeding every
  // intermediate onChange back through the string prop resets the typed segments.
  const [inner, setInner] = useState<DateValue | null>(() => parseDateValue(value))
  const lastEmitted = useRef(value ?? '')

  useEffect(() => {
    if ((value ?? '') !== lastEmitted.current) {
      lastEmitted.current = value ?? ''
      setInner(parseDateValue(value))
    }
  }, [value])

  return (
    <HeroDatePicker
      aria-label={label}
      className={cn('w-full', className)}
      isDisabled={isDisabled}
      value={inner}
      onChange={(next) => {
        setInner(next)
        const formatted = formatDateValue(next)
        lastEmitted.current = formatted
        onChange(formatted)
      }}
    >
      {label ? <Label>{label}</Label> : null}
      <DateField.Group>
        <DateField.Input>{(segment) => <DateField.Segment segment={segment} />}</DateField.Input>
        <DateField.Suffix>
          <HeroDatePicker.Trigger>
            <HeroDatePicker.TriggerIndicator />
          </HeroDatePicker.Trigger>
        </DateField.Suffix>
      </DateField.Group>
      <HeroDatePicker.Popover>
        <Calendar aria-label={label || 'Выбор даты'}>
          <Calendar.Header>
            <Calendar.YearPickerTrigger>
              <Calendar.YearPickerTriggerHeading />
              <Calendar.YearPickerTriggerIndicator />
            </Calendar.YearPickerTrigger>
            <Calendar.NavButton slot="previous" />
            <Calendar.NavButton slot="next" />
          </Calendar.Header>
          <Calendar.Grid>
            <Calendar.GridHeader>
              {(day) => <Calendar.HeaderCell>{day}</Calendar.HeaderCell>}
            </Calendar.GridHeader>
            <Calendar.GridBody>{(date) => <Calendar.Cell date={date} />}</Calendar.GridBody>
          </Calendar.Grid>
          {/* Без этой сетки YearPickerTrigger открывает пустоту — год выбрать нельзя */}
          <Calendar.YearPickerGrid>
            <Calendar.YearPickerGridBody>
              {({ year, formattedYear }) => (
                <Calendar.YearPickerCell key={year} year={year}>
                  {formattedYear}
                </Calendar.YearPickerCell>
              )}
            </Calendar.YearPickerGridBody>
          </Calendar.YearPickerGrid>
        </Calendar>
      </HeroDatePicker.Popover>
    </HeroDatePicker>
  )
}

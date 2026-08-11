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
  return `${day}-${month}-${value.year}`
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
  return (
    <HeroDatePicker
      aria-label={label}
      className={cn('w-full', className)}
      isDisabled={isDisabled}
      value={parseDateValue(value)}
      onChange={(next) => onChange(formatDateValue(next))}
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
        </Calendar>
      </HeroDatePicker.Popover>
    </HeroDatePicker>
  )
}

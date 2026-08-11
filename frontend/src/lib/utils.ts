import { type ClassValue, clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function getErrorMessage(error: unknown, fallback = 'Не удалось выполнить запрос') {
  if (error instanceof Error && error.message) return error.message
  if (typeof error === 'string' && error.trim()) return error
  if (typeof error === 'object' && error && 'error' in error) {
    const value = (error as { error?: unknown }).error
    if (typeof value === 'string' && value.trim()) return value
  }
  return fallback
}

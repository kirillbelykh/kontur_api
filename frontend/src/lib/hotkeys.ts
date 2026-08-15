import { useEffect } from 'react'

export const PAGE_REFRESH_EVENT = 'kontur:refresh-page'

export const SECTION_ROUTES = [
  '/orders',
  '/download',
  '/intro',
  '/tsd',
  '/aggregation',
  '/labels',
] as const

export function isTypingTarget(target: EventTarget | null): boolean {
  if (!target || typeof target !== 'object') return false
  const el = target as { isContentEditable?: boolean; tagName?: string }
  if (el.isContentEditable) return true
  const tag = String(el.tagName || '').toUpperCase()
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT'
}

export function focusTableSearch(): boolean {
  const input = document.querySelector<HTMLInputElement>('[data-kontur-search]')
  if (!input) return false
  input.focus()
  input.select()
  return true
}

export function usePageRefreshHotkey(onRefresh: () => void) {
  useEffect(() => {
    const handler = () => onRefresh()
    window.addEventListener(PAGE_REFRESH_EVENT, handler)
    return () => window.removeEventListener(PAGE_REFRESH_EVENT, handler)
  }, [onRefresh])
}

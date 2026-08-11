import { useSyncExternalStore } from 'react'

/**
 * Локальные настройки поведения (диалог «Настройки»), живут в localStorage.
 * useAppSetting даёт реактивность без контекста — подписка через store.
 */
export type AppSettingKey = 'animations' | 'journalAutoRefresh'

const STORAGE_KEYS: Record<AppSettingKey, string> = {
  animations: 'kontur_animations_v1',
  journalAutoRefresh: 'kontur_journal_autorefresh_v1',
}

const listeners = new Set<() => void>()

export function getAppSetting(key: AppSettingKey): boolean {
  if (typeof window === 'undefined') return true
  return window.localStorage.getItem(STORAGE_KEYS[key]) !== 'false'
}

export function setAppSetting(key: AppSettingKey, value: boolean) {
  window.localStorage.setItem(STORAGE_KEYS[key], String(value))
  listeners.forEach((listener) => listener())
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function useAppSetting(key: AppSettingKey): boolean {
  return useSyncExternalStore(subscribe, () => getAppSetting(key))
}

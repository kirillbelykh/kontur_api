import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react'
import { toast } from 'sonner'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'

export type UpdateCheckResult = {
  update_available?: boolean
  local_commit?: string
  remote_commit?: string
  behind_count?: number
  error?: string
}

export type UpdateApplyResult = {
  success?: boolean
  restarted?: boolean
  message?: string
  error?: string
}

type AppUpdateContextValue = {
  updateAvailable: boolean
  localCommit: string
  remoteCommit: string
  remoteShort: string
  behindCount: number
  checking: boolean
  applying: boolean
  checkForUpdates: () => Promise<UpdateCheckResult | null>
  applyUpdate: () => Promise<UpdateApplyResult>
}

const AppUpdateContext = createContext<AppUpdateContextValue | null>(null)

const POLL_MS = 5 * 60_000

export function AppUpdateProvider({ children }: { children: ReactNode }) {
  const [updateAvailable, setUpdateAvailable] = useState(false)
  const [localCommit, setLocalCommit] = useState('')
  const [remoteCommit, setRemoteCommit] = useState('')
  const [behindCount, setBehindCount] = useState(0)
  const [checking, setChecking] = useState(false)
  const [applying, setApplying] = useState(false)

  const checkForUpdates = useCallback(async () => {
    setChecking(true)
    try {
      const result = await apiCall<UpdateCheckResult>('check_for_updates')
      if (result.error) {
        setUpdateAvailable(false)
        return result
      }
      setUpdateAvailable(Boolean(result.update_available))
      setLocalCommit(String(result.local_commit || ''))
      setRemoteCommit(String(result.remote_commit || ''))
      setBehindCount(Number(result.behind_count || 0))
      return result
    } catch {
      setUpdateAvailable(false)
      return null
    } finally {
      setChecking(false)
    }
  }, [])

  const applyUpdate = useCallback(async () => {
    setApplying(true)
    try {
      const result = await apiCall<UpdateApplyResult>('apply_update')
      if (result.error || result.success === false) {
        throw new Error(result.error || 'Обновление не выполнено')
      }
      toast.success(result.message || 'Обновление установлено. Перезапуск…')
      setUpdateAvailable(false)
      return result
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось обновить'))
      throw error
    } finally {
      setApplying(false)
    }
  }, [])

  useEffect(() => {
    void checkForUpdates()
    const id = window.setInterval(() => void checkForUpdates(), POLL_MS)
    return () => window.clearInterval(id)
  }, [checkForUpdates])

  const value = useMemo<AppUpdateContextValue>(
    () => ({
      updateAvailable,
      localCommit,
      remoteCommit,
      remoteShort: remoteCommit ? remoteCommit.slice(0, 7) : '',
      behindCount,
      checking,
      applying,
      checkForUpdates,
      applyUpdate,
    }),
    [
      updateAvailable,
      localCommit,
      remoteCommit,
      behindCount,
      checking,
      applying,
      checkForUpdates,
      applyUpdate,
    ],
  )

  return <AppUpdateContext.Provider value={value}>{children}</AppUpdateContext.Provider>
}

export function useAppUpdate() {
  const ctx = useContext(AppUpdateContext)
  if (!ctx) {
    throw new Error('useAppUpdate must be used within AppUpdateProvider')
  }
  return ctx
}

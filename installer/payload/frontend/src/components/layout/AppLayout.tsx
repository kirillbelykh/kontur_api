import { useCallback, useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { NavLink, Outlet, useLocation } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import {
  ClipboardList,
  Download,
  Layers3,
  PanelLeftClose,
  PanelLeftOpen,
  Printer,
  RefreshCw,
  Shield,
  Smartphone,
  type LucideIcon,
} from 'lucide-react'
import { toast } from 'sonner'
import { apiCall, type SessionInfo } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'

const DESKTOP_SIDEBAR_OPEN_WIDTH = 240
const DESKTOP_SIDEBAR_COLLAPSED_WIDTH = 64
const SIDEBAR_STORAGE_KEY = 'kontur_desktop_sidebar_open_v1'

const sidebarTransition = {
  duration: 0.28,
  ease: [0.22, 1, 0.36, 1] as [number, number, number, number],
}

const pageTransition = {
  initial: { opacity: 0, y: 8 },
  animate: { opacity: 1, y: 0 },
  exit: { opacity: 0, y: -4 },
  transition: { duration: 0.18, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] },
}

type NavItem = {
  to: string
  label: string
  icon: LucideIcon
}

const navItems: NavItem[] = [
  { to: '/orders', label: 'Заказ кодов', icon: ClipboardList },
  { to: '/chz', label: 'Запросы ЧЗ', icon: Shield },
  { to: '/download', label: 'Загрузка кодов', icon: Download },
  { to: '/intro', label: 'Ввод в оборот', icon: RefreshCw },
  { to: '/tsd', label: 'Задание на ТСД', icon: Smartphone },
  { to: '/aggregation', label: 'Коды агрегации', icon: Layers3 },
  { to: '/labels', label: 'Печать этикеток', icon: Printer },
]

const titles: Record<string, string> = {
  '/orders': 'Заказ кодов',
  '/chz': 'Запросы ЧЗ',
  '/download': 'Загрузка кодов',
  '/intro': 'Ввод в оборот',
  '/tsd': 'Задание на ТСД',
  '/aggregation': 'Коды агрегации',
  '/labels': 'Печать этикеток',
}

type TooltipState = { label: string; top: number; left: number }

function readSidebarOpen() {
  if (typeof window === 'undefined') return true
  const saved = window.localStorage.getItem(SIDEBAR_STORAGE_KEY)
  if (saved === 'true') return true
  if (saved === 'false') return false
  return true
}

function writeSidebarOpen(value: boolean) {
  window.localStorage.setItem(SIDEBAR_STORAGE_KEY, String(value))
}

function SidebarTooltip({ tooltip }: { tooltip: TooltipState | null }) {
  if (!tooltip || typeof document === 'undefined') return null
  return createPortal(
    <motion.div
      initial={{ opacity: 0, x: -4 }}
      animate={{ opacity: 1, x: 0 }}
      exit={{ opacity: 0, x: -4 }}
      transition={{ duration: 0.15, ease: [0.22, 1, 0.36, 1] }}
      style={{
        position: 'fixed',
        top: tooltip.top,
        left: tooltip.left,
        transform: 'translateY(-50%)',
      }}
      className="pointer-events-none z-[9999] whitespace-nowrap rounded-md bg-slate-900 px-2.5 py-1 text-xs font-medium text-white shadow-panel"
    >
      {tooltip.label}
    </motion.div>,
    document.body,
  )
}

function Navigation({ collapsed, onNavigate }: { collapsed: boolean; onNavigate?: () => void }) {
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)

  return (
    <nav className="flex flex-1 flex-col gap-0.5 px-2 py-2">
      {navItems.map((item) => {
        const Icon = item.icon
        return (
          <NavLink
            key={item.to}
            to={item.to}
            onClick={onNavigate}
            onMouseEnter={(event) => {
              if (!collapsed) return
              const rect = event.currentTarget.getBoundingClientRect()
              setTooltip({
                label: item.label,
                top: Math.round(rect.top + rect.height / 2),
                left: Math.round(rect.right + 10),
              })
            }}
            onMouseLeave={() => setTooltip(null)}
            className={({ isActive }) =>
              cn(
                'group flex items-center gap-3 rounded-md px-2.5 py-2 text-sm font-medium transition-colors',
                collapsed && 'justify-center px-0',
                isActive
                  ? 'bg-muted text-foreground'
                  : 'text-muted-foreground hover:bg-muted/70 hover:text-foreground',
              )
            }
          >
            <Icon className="h-4 w-4 shrink-0 opacity-80" />
            <AnimatePresence initial={false}>
              {!collapsed && (
                <motion.span
                  initial={{ opacity: 0, width: 0 }}
                  animate={{ opacity: 1, width: 'auto' }}
                  exit={{ opacity: 0, width: 0 }}
                  transition={{ duration: 0.16, ease: 'easeOut' }}
                  className="overflow-hidden whitespace-nowrap"
                >
                  {item.label}
                </motion.span>
              )}
            </AnimatePresence>
          </NavLink>
        )
      })}
      <AnimatePresence>{tooltip ? <SidebarTooltip tooltip={tooltip} /> : null}</AnimatePresence>
    </nav>
  )
}

export function AppLayout() {
  const location = useLocation()
  const [sidebarOpen, setSidebarOpen] = useState(readSidebarOpen)
  const [session, setSession] = useState<SessionInfo | null>(null)
  const [sessionLoading, setSessionLoading] = useState(false)

  const title = useMemo(() => {
    const exact = titles[location.pathname]
    if (exact) return exact
    const match = navItems.find((item) => location.pathname.startsWith(item.to))
    return match?.label ?? 'Kontur API'
  }, [location.pathname])

  const loadSession = useCallback(async () => {
    try {
      const info = await apiCall<SessionInfo>('get_session_info')
      setSession(info)
    } catch {
      setSession({ has_session: false })
    }
  }, [])

  const refreshSession = useCallback(async () => {
    setSessionLoading(true)
    try {
      const result = await apiCall<{ success?: boolean; session?: SessionInfo }>('refresh_session')
      setSession(result.session ?? (await apiCall<SessionInfo>('get_session_info')))
      toast.success('Сессия обновлена')
    } catch (error) {
      toast.error(getErrorMessage(error, 'Не удалось обновить сессию'))
    } finally {
      setSessionLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadSession()
    const id = window.setInterval(() => void loadSession(), 60_000)
    return () => window.clearInterval(id)
  }, [loadSession])

  const toggleSidebar = () => {
    setSidebarOpen((prev) => {
      const next = !prev
      writeSidebarOpen(next)
      return next
    })
  }

  const sessionTone = session?.has_session ? 'success' : 'warning'
  const sessionLabel = session?.has_session
    ? `Сессия · ${Math.round(Number(session.minutes_until_update ?? 0))} мин`
    : 'Нет сессии'

  return (
    <div className="flex h-screen overflow-hidden bg-background text-foreground">
      <motion.aside
        initial={false}
        animate={{ width: sidebarOpen ? DESKTOP_SIDEBAR_OPEN_WIDTH : DESKTOP_SIDEBAR_COLLAPSED_WIDTH }}
        transition={sidebarTransition}
        className="relative z-20 flex h-full shrink-0 flex-col border-r border-border bg-card shadow-panel"
      >
        <div className={cn('flex h-14 items-center border-b border-border px-3', !sidebarOpen && 'justify-center px-0')}>
          {sidebarOpen ? (
            <div className="min-w-0">
              <div className="truncate text-sm font-semibold tracking-tight">Kontur API</div>
              <div className="truncate text-[11px] text-muted-foreground">Маркировка · desktop</div>
            </div>
          ) : (
            <div className="flex h-8 w-8 items-center justify-center rounded-md bg-muted text-xs font-bold text-foreground">
              K
            </div>
          )}
        </div>
        <Navigation collapsed={!sidebarOpen} />
        <div className="border-t border-border p-2">
          <Button
            variant="ghost"
            size={sidebarOpen ? 'sm' : 'icon'}
            className={cn('w-full', sidebarOpen ? 'justify-start' : 'justify-center')}
            onClick={toggleSidebar}
            aria-label={sidebarOpen ? 'Свернуть меню' : 'Развернуть меню'}
          >
            {sidebarOpen ? <PanelLeftClose className="h-4 w-4" /> : <PanelLeftOpen className="h-4 w-4" />}
            {sidebarOpen ? <span>Свернуть</span> : null}
          </Button>
        </div>
      </motion.aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-10 flex h-14 items-center justify-between gap-3 border-b border-border bg-card/95 px-4 backdrop-blur supports-[backdrop-filter]:bg-card/80">
          <div className="min-w-0">
            <div className="truncate text-sm font-semibold">{title}</div>
            <div className="truncate text-[11px] text-muted-foreground">Операции Контур.Маркировка</div>
          </div>
          <div className="flex items-center gap-2">
            <Badge tone={sessionTone}>{sessionLabel}</Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void refreshSession()}
              disabled={sessionLoading}
            >
              <RefreshCw className={cn('h-3.5 w-3.5', sessionLoading && 'animate-spin')} />
              Обновить сессию
            </Button>
          </div>
        </header>

        <main className="min-h-0 flex-1 overflow-auto">
          <AnimatePresence mode="wait">
            <motion.div key={location.pathname} {...pageTransition} className="min-h-full">
              <Outlet />
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </div>
  )
}

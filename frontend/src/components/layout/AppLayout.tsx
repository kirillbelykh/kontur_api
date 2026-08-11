import { useCallback, useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { AnimatePresence, motion } from 'framer-motion'
import {
  ClipboardList,
  Download,
  Home,
  Layers3,
  PanelLeftOpen,
  Printer,
  RefreshCw,
  Shield,
  Smartphone,
  ZoomIn,
  ZoomOut,
  type LucideIcon,
} from 'lucide-react'
import { toast } from 'sonner'
import menuLogo from '@/assets/menu_logo_3.png'
import logo from '@/assets/logo.png'
import { apiCall, type SessionInfo } from '@/lib/bridge'
import { cn, getErrorMessage } from '@/lib/utils'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { useAppUpdate } from '@/hooks/useAppUpdate'
import { usePageZoom } from '@/hooks/usePageZoom'

const DESKTOP_SIDEBAR_OPEN_WIDTH = 256
const DESKTOP_SIDEBAR_COLLAPSED_WIDTH = 64
const SIDEBAR_STORAGE_KEY = 'kontur_desktop_sidebar_open_v1'

const sidebarTransition = {
  duration: 0.28,
  ease: [0.22, 1, 0.36, 1] as [number, number, number, number],
}

const textTransition = {
  duration: 0.16,
  ease: 'easeOut' as const,
}

const pageTransition = {
  initial: { opacity: 0, y: 8 },
  animate: { opacity: 1, y: 0 },
  transition: { duration: 0.18 },
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
  '/': '',
  '/welcome': '',
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
        transform: 'translateY(calc(-50% - 10px))',
      }}
      className="pointer-events-none z-[9999] whitespace-nowrap rounded-lg bg-[#202123] px-3 py-1.5 text-xs font-semibold leading-none text-white shadow-xl"
    >
      {tooltip.label}
    </motion.div>,
    document.body,
  )
}

function Navigation({ collapsed, onNavigate }: { collapsed: boolean; onNavigate?: () => void }) {
  const [tooltip, setTooltip] = useState<TooltipState | null>(null)

  const showTooltip = (label: string, element: HTMLElement) => {
    if (!collapsed) return
    const rect = element.getBoundingClientRect()
    setTooltip({
      label,
      top: Math.round(rect.top + rect.height / 2),
      left: rect.right + 12,
    })
  }

  return (
    <>
      <div className="flex flex-col gap-1">
        {navItems.map((item) => {
          const Icon = item.icon
          return (
            <NavLink
              key={item.to}
              to={item.to}
              onClick={onNavigate}
              onMouseEnter={(event) => showTooltip(item.label, event.currentTarget)}
              onMouseLeave={() => setTooltip(null)}
              onFocus={(event) => showTooltip(item.label, event.currentTarget)}
              onBlur={() => setTooltip(null)}
              className={({ isActive }) =>
                cn(
                  'focus-ring flex h-10 w-full items-center overflow-hidden rounded-xl text-sm font-medium text-muted-foreground transition-colors duration-200 hover:bg-muted hover:text-foreground',
                  isActive && 'bg-slate-200/80 text-foreground dark:bg-slate-800',
                )
              }
            >
              <span className="flex h-10 w-12 shrink-0 items-center justify-center">
                <Icon className="h-5 w-5 shrink-0" />
              </span>
              <AnimatePresence initial={false}>
                {!collapsed && (
                  <motion.span
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={textTransition}
                    className="min-w-0 flex-1 truncate whitespace-nowrap pr-3"
                  >
                    {item.label}
                  </motion.span>
                )}
              </AnimatePresence>
            </NavLink>
          )
        })}
      </div>
      <AnimatePresence>
        <SidebarTooltip tooltip={tooltip} />
      </AnimatePresence>
    </>
  )
}

function SidebarBrand({
  collapsed,
  onLogoClick,
}: {
  collapsed: boolean
  onLogoClick?: () => void
}) {
  return (
    <div className="mb-4 flex h-12 shrink-0 items-center justify-between">
      <div className="flex min-w-0 items-center">
        <span className="flex h-12 w-12 shrink-0 items-center justify-center">
          <motion.img
            src={menuLogo}
            alt="Grundlage"
            onClick={onLogoClick}
            onMouseDown={(event) => event.preventDefault()}
            whileHover={onLogoClick ? { scale: 1.04 } : undefined}
            whileTap={onLogoClick ? { scale: 0.96 } : undefined}
            transition={{ duration: 0.14, ease: 'easeOut' }}
            className={cn(
              'block h-10 w-10 shrink-0 select-none object-contain border-0 bg-transparent shadow-none',
              'outline-none ring-0 focus:outline-none focus:ring-0 focus-visible:outline-none focus-visible:ring-0',
              'active:outline-none active:ring-0 dark:invert dark:brightness-110',
              onLogoClick && 'cursor-pointer',
            )}
            draggable={false}
            style={{
              WebkitTapHighlightColor: 'transparent',
              outline: 'none',
              border: 'none',
              boxShadow: 'none',
            }}
          />
        </span>
        <AnimatePresence initial={false}>
          {!collapsed && (
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={textTransition}
              className="min-w-0 overflow-hidden pr-2"
            >
              <p className="truncate text-sm font-semibold leading-tight text-foreground">Grundlage</p>
              <p className="truncate text-xs text-muted-foreground">Контур Маркировка</p>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  )
}

export function AppLayout() {
  const location = useLocation()
  const navigate = useNavigate()
  const [sidebarOpen, setSidebarOpen] = useState(readSidebarOpen)
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false)
  const [session, setSession] = useState<SessionInfo | null>(null)
  const [sessionLoading, setSessionLoading] = useState(false)
  const { updateAvailable, applying, applyUpdate, remoteShort } = useAppUpdate()
  const { zoom, zoomIn, zoomOut, resetZoom } = usePageZoom()

  const title = useMemo(() => {
    const exact = titles[location.pathname]
    if (exact !== undefined) return exact
    const match = navItems.find((item) => location.pathname.startsWith(item.to))
    return match?.label ?? 'Контур Маркировка'
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

  useEffect(() => {
    writeSidebarOpen(sidebarOpen)
  }, [sidebarOpen])

  const sessionTone = session?.has_session ? 'success' : 'warning'
  const sessionLabel = session?.has_session
    ? `Сессия · ${Math.round(Number(session.minutes_until_update ?? 0))} мин`
    : 'Нет сессии'

  const isWelcome = location.pathname === '/' || location.pathname === '/welcome'

  return (
    <div
      className="min-h-screen bg-background"
      style={{ touchAction: 'manipulation', overscrollBehaviorY: 'contain' }}
    >
      <motion.aside
        initial={false}
        animate={{
          width: sidebarOpen ? DESKTOP_SIDEBAR_OPEN_WIDTH : DESKTOP_SIDEBAR_COLLAPSED_WIDTH,
        }}
        transition={sidebarTransition}
        className="fixed left-0 top-0 z-20 hidden h-screen overflow-hidden border-r border-border bg-card px-2 py-3 md:flex md:flex-col"
      >
        <SidebarBrand
          collapsed={!sidebarOpen}
          onLogoClick={() => setSidebarOpen((value) => !value)}
        />
        <nav className="sidebar-scroll flex-1 overflow-y-auto overscroll-contain pb-4">
          <Navigation collapsed={!sidebarOpen} />
        </nav>
        <div className="space-y-2 border-t border-border pt-3">
          <Button
            variant="ghost"
            onClick={() => navigate('/welcome')}
            className="h-9 w-full overflow-hidden rounded-xl p-0 text-muted-foreground hover:text-foreground"
            aria-label="На главную"
            title={!sidebarOpen ? 'На главную' : undefined}
          >
            <span className="flex h-9 w-12 shrink-0 items-center justify-center">
              <Home className="h-4 w-4 shrink-0" />
            </span>
            <AnimatePresence initial={false}>
              {sidebarOpen && (
                <motion.span
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={textTransition}
                  className="min-w-0 flex-1 truncate whitespace-nowrap pr-3 text-left text-xs"
                >
                  Главная
                </motion.span>
              )}
            </AnimatePresence>
          </Button>
        </div>
      </motion.aside>

      {mobileSidebarOpen && (
        <div className="fixed inset-0 z-40 md:hidden">
          <button
            type="button"
            className="absolute inset-0 bg-black/40"
            aria-label="Закрыть меню"
            onClick={() => setMobileSidebarOpen(false)}
          />
          <motion.aside
            initial={{ x: -288 }}
            animate={{ x: 0 }}
            transition={sidebarTransition}
            className="relative flex h-full w-72 flex-col border-r border-border bg-card px-3 py-3 shadow-2xl"
          >
            <SidebarBrand collapsed={false} onLogoClick={() => setMobileSidebarOpen(false)} />
            <nav className="scrollbar-hide flex-1 overflow-y-auto pb-4">
              <Navigation collapsed={false} onNavigate={() => setMobileSidebarOpen(false)} />
            </nav>
          </motion.aside>
        </div>
      )}

      <div
        className={cn(
          'min-h-screen transition-[padding-left] duration-[280ms] ease-[cubic-bezier(0.22,1,0.36,1)]',
          sidebarOpen ? 'md:pl-64' : 'md:pl-16',
        )}
      >
        <header className="sticky top-0 z-10 border-b border-border bg-background/90 backdrop-blur">
          <div className="relative flex h-16 items-center justify-between px-4 sm:px-6 lg:px-8">
            <div className="flex min-w-0 items-center gap-3">
              <Button
                variant="ghost"
                size="icon"
                onClick={() => setMobileSidebarOpen(true)}
                className="md:hidden"
                aria-label="Открыть меню"
              >
                <PanelLeftOpen className="h-5 w-5" />
              </Button>
              <div className="min-w-0">
                <h1 className="truncate text-base font-semibold sm:text-lg">{title || 'Контур Маркировка'}</h1>
                <p className="hidden truncate text-sm text-muted-foreground sm:block">
                  Операции Контур.Маркировка
                </p>
              </div>
            </div>

            {!isWelcome ? (
              <div className="pointer-events-none absolute left-1/2 top-1/2 block -translate-x-1/2 -translate-y-1/2">
                <img
                  src={logo}
                  alt="Grundlage"
                  className="h-12 w-auto object-contain opacity-90 transition-opacity hover:opacity-100 dark:invert dark:brightness-110 sm:h-[84px] md:h-[72px]"
                />
              </div>
            ) : null}

            <div className="flex items-center gap-2">
              <div className="hidden items-center rounded-lg thin-border sm:flex">
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={zoomOut}
                  aria-label="Уменьшить масштаб"
                  title="Уменьшить масштаб (Ctrl+−)"
                >
                  <ZoomOut className="h-4 w-4" />
                </Button>
                <button
                  type="button"
                  onClick={resetZoom}
                  title="Сбросить масштаб (Ctrl+0)"
                  className="min-w-[3.25rem] border-0 bg-transparent px-1 py-0 text-xs font-medium tabular-nums text-muted-foreground hover:text-foreground"
                >
                  {Math.round(zoom * 100)}%
                </button>
                <Button
                  variant="ghost"
                  size="icon"
                  onClick={zoomIn}
                  aria-label="Увеличить масштаб"
                  title="Увеличить масштаб (Ctrl++)"
                >
                  <ZoomIn className="h-4 w-4" />
                </Button>
              </div>
              {updateAvailable ? (
                <Button
                  variant="warning"
                  size="sm"
                  disabled={applying}
                  onClick={() => void applyUpdate()}
                >
                  <Download className={cn('h-3.5 w-3.5', applying && 'animate-spin')} />
                  {applying ? 'Обновляем…' : `Обновить${remoteShort ? ` · ${remoteShort}` : ''}`}
                </Button>
              ) : null}
              <Badge tone={sessionTone}>{sessionLabel}</Badge>
              <Button
                variant="outline"
                size="sm"
                onClick={() => void refreshSession()}
                disabled={sessionLoading}
              >
                <RefreshCw className={cn('h-3.5 w-3.5', sessionLoading && 'animate-spin')} />
                <span className="hidden sm:inline">Сессия</span>
              </Button>
            </div>
          </div>
        </header>

        <motion.main
          key={location.pathname}
          {...pageTransition}
          className="pb-8"
        >
          <Outlet />
        </motion.main>
      </div>
    </div>
  )
}

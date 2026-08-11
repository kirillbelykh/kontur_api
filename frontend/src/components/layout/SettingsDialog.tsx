import { useEffect, useState } from 'react'
import { Switch } from '@heroui/react'
import { Check, Monitor, ScrollText } from 'lucide-react'
import { getAppSetting, setAppSetting, type AppSettingKey } from '@/lib/app-settings'
import { apiCall } from '@/lib/bridge'
import { cn } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { useAppUpdate } from '@/hooks/useAppUpdate'
import { usePageZoom, ZOOM_STEPS } from '@/hooks/usePageZoom'
import { THEME_OPTIONS, useTheme, type ThemeOption } from '@/hooks/useTheme'

type AppVersion = { version?: string; commit?: string }

function ThemeCard({
  option,
  active,
  onSelect,
}: {
  option: ThemeOption
  active: boolean
  onSelect: () => void
}) {
  const preview = option.preview
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'w-full rounded-lg border bg-transparent p-1.5 text-left transition',
        active
          ? 'border-[var(--accent)] ring-1 ring-[var(--accent)]'
          : 'border-border hover:border-[var(--field-border-hover)]',
      )}
    >
      <span
        className="block overflow-hidden rounded-md border"
        style={{ background: preview.bg, borderColor: preview.border }}
      >
        <span
          className="m-1.5 block rounded-sm border p-1.5"
          style={{ background: preview.card, borderColor: preview.border }}
        >
          <span className="block h-1.5 w-8 rounded-full" style={{ background: preview.accent }} />
          <span className="mt-1 block h-1 w-12 rounded-full" style={{ background: preview.text, opacity: 0.55 }} />
          <span className="mt-1 block h-1 w-9 rounded-full" style={{ background: preview.text, opacity: 0.3 }} />
        </span>
      </span>
      <span className="mt-1 flex h-4 items-center justify-between px-0.5">
        <span className="truncate text-xs font-medium text-foreground">{option.label}</span>
        {active ? <Check className="h-3.5 w-3.5 shrink-0 text-[var(--accent)]" /> : null}
      </span>
    </button>
  )
}

function SystemThemeCard({ active, onSelect }: { active: boolean; onSelect: () => void }) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={cn(
        'w-full rounded-lg border bg-transparent p-1.5 text-left transition',
        active
          ? 'border-[var(--accent)] ring-1 ring-[var(--accent)]'
          : 'border-border hover:border-[var(--field-border-hover)]',
      )}
    >
      <span
        className="grid h-[52px] place-items-center overflow-hidden rounded-md border border-border"
        style={{ background: 'linear-gradient(90deg, #f7f7f8 49.7%, #18181b 50.3%)' }}
      >
        <Monitor className="h-4 w-4" style={{ color: '#8a8a92', mixBlendMode: 'difference' }} />
      </span>
      <span className="mt-1 flex h-4 items-center justify-between px-0.5">
        <span className="truncate text-xs font-medium text-foreground">Системная</span>
        {active ? <Check className="h-3.5 w-3.5 shrink-0 text-[var(--accent)]" /> : null}
      </span>
    </button>
  )
}

function SettingSwitch({ settingKey, label }: { settingKey: AppSettingKey; label: string }) {
  const [value, setValue] = useState(() => getAppSetting(settingKey))
  return (
    <Switch
      isSelected={value}
      onChange={(next) => {
        setValue(next)
        setAppSetting(settingKey, next)
      }}
      className="flex w-full flex-row items-center justify-between gap-3 rounded-md border border-border px-3 py-2"
    >
      <span className="text-sm text-foreground">{label}</span>
      <Switch.Control>
        <Switch.Thumb />
      </Switch.Control>
    </Switch>
  )
}

export function SettingsDialog({
  open,
  onOpenChange,
  onOpenJournal,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onOpenJournal: () => void
}) {
  const { themeId, setTheme } = useTheme()
  const { zoom, setZoom } = usePageZoom()
  const { updateAvailable, remoteShort, checking, checkForUpdates } = useAppUpdate()
  const [version, setVersion] = useState<AppVersion | null>(null)
  // ?qa-settings=behavior|about — только dev: скриншот-прогон вкладок без кликов
  const [tab, setTab] = useState(() => {
    if (!import.meta.env.DEV) return 'appearance'
    const qa = new URLSearchParams(window.location.search).get('qa-settings')
    return qa === 'behavior' || qa === 'about' ? qa : 'appearance'
  })

  // Версия/коммит из бриджа; метода может не быть — тогда версия сборки из package.json
  useEffect(() => {
    if (!open) return
    apiCall<AppVersion>('get_app_version')
      .then((result) => setVersion(result && typeof result === 'object' ? result : null))
      .catch(() => setVersion(null))
  }, [open])

  const versionLabel = String(version?.version || __APP_VERSION__ || '—')
  const commitLabel = String(version?.commit || '').slice(0, 7)
  const updateStatus = checking
    ? 'Проверяем…'
    : updateAvailable
      ? `Доступно обновление${remoteShort ? ` · ${remoteShort}` : ''}`
      : 'Актуальная версия'

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle>Настройки</DialogTitle>
        </DialogHeader>

        <Tabs value={tab} onValueChange={setTab}>
          <TabsList>
            <TabsTrigger value="appearance">Оформление</TabsTrigger>
            <TabsTrigger value="behavior">Поведение</TabsTrigger>
            <TabsTrigger value="about">О приложении</TabsTrigger>
          </TabsList>

          <TabsContent value="appearance" className="space-y-4">
            <div className="grid grid-cols-3 gap-2">
              {THEME_OPTIONS.map((option) => (
                <ThemeCard
                  key={option.id}
                  option={option}
                  active={themeId === option.id}
                  onSelect={() => setTheme(option.id)}
                />
              ))}
              <SystemThemeCard active={themeId === 'system'} onSelect={() => setTheme('system')} />
            </div>

            <div>
              <div className="mb-1.5 text-xs font-medium text-muted-foreground">Масштаб</div>
              <div className="flex flex-wrap gap-1">
                {ZOOM_STEPS.map((step) => (
                  <Button
                    key={step}
                    size="sm"
                    variant={zoom === step ? 'secondary' : 'ghost'}
                    className="px-2.5 tabular-nums"
                    onClick={() => setZoom(step)}
                  >
                    {Math.round(step * 100)}%
                  </Button>
                ))}
              </div>
            </div>
          </TabsContent>

          <TabsContent value="behavior" className="space-y-2">
            <SettingSwitch settingKey="animations" label="Показывать анимации" />
            <SettingSwitch settingKey="journalAutoRefresh" label="Автообновление журнала" />
          </TabsContent>

          <TabsContent value="about" className="space-y-2">
            <div className="rounded-md border border-border px-3 py-2">
              <div className="text-sm font-medium text-foreground">Контур Маркировка</div>
              <div className="mt-0.5 font-mono text-xs text-muted-foreground">
                v{versionLabel}
                {commitLabel ? ` · ${commitLabel}` : ''}
              </div>
            </div>
            <div className="flex items-center justify-between gap-3 rounded-md border border-border px-3 py-2">
              <span className="text-xs text-muted-foreground">{updateStatus}</span>
              <Button size="sm" variant="outline" disabled={checking} onClick={() => void checkForUpdates()}>
                Проверить обновления
              </Button>
            </div>
            <Button size="sm" variant="ghost" onClick={onOpenJournal}>
              <ScrollText className="h-3.5 w-3.5" />
              Открыть журнал
            </Button>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  )
}

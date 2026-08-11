import { useCallback, useEffect, useRef, useState } from 'react'
import { Trash2, X } from 'lucide-react'
import { apiCall } from '@/lib/bridge'
import { getErrorMessage } from '@/lib/utils'
import { Button } from '@/components/ui/button'
import { SelectNative } from '@/components/ui/select'

export const JOURNAL_WIDTH = 320
const REFRESH_MS = 5_000
const ALL = 'all'

/** Каналы журнала — LOG_CHANNELS из backend/app/api_bridge.py */
const CHANNELS: Array<{ id: string; label: string }> = [
  { id: 'orders', label: 'Заказы' },
  { id: 'chz', label: 'ЧЗ (WMS)' },
  { id: 'download', label: 'Загрузка' },
  { id: 'intro', label: 'Ввод в оборот' },
  { id: 'tsd', label: 'ТСД' },
  { id: 'aggregation', label: 'Агрегация' },
  { id: 'labels', label: 'Этикетки' },
]

type JournalEntry = {
  channel: string
  time: string
  message: string
}

/** Строки бэкенда приходят как `[HH:MM:SS] сообщение` */
function parseLine(channel: string, line: string): JournalEntry {
  const match = /^\[(\d{2}:\d{2}:\d{2})\]\s?(.*)$/s.exec(line)
  return match
    ? { channel, time: match[1], message: match[2] }
    : { channel, time: '', message: line }
}

async function fetchChannel(channel: string): Promise<JournalEntry[]> {
  const lines = await apiCall<string[]>('get_logs', channel)
  return (Array.isArray(lines) ? lines : []).map((line) => parseLine(channel, String(line)))
}

/**
 * Правая панель «Журнал»: логи операций из бриджа (get_logs / clear_logs).
 * Обновляется раз в 5 секунд ТОЛЬКО пока открыта — фонового поллинга нет,
 * поэтому бейдж непрочитанных на тумблере не делаем.
 */
export function JournalPanel({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [channel, setChannel] = useState<string>(ALL)
  const [entries, setEntries] = useState<JournalEntry[]>([])
  const [error, setError] = useState('')
  const [loaded, setLoaded] = useState(false)
  const busyRef = useRef(false)

  const refresh = useCallback(async (selected: string) => {
    if (busyRef.current) return
    busyRef.current = true
    try {
      let next: JournalEntry[]
      if (selected === ALL) {
        const perChannel = await Promise.all(CHANNELS.map((item) => fetchChannel(item.id)))
        next = perChannel.flat()
        // ponytail: сортировка по HH:MM:SS ломается на записях через полночь;
        // логи живут в памяти одной сессии (≤500 строк на канал), для смены суток нужен полный timestamp в бридже
        next.sort((a, b) => a.time.localeCompare(b.time))
      } else {
        next = await fetchChannel(selected)
      }
      next.reverse() // свежие сверху
      setEntries(next)
      setError('')
    } catch (err) {
      setEntries([])
      setError(getErrorMessage(err, 'Не удалось загрузить журнал'))
    } finally {
      busyRef.current = false
      setLoaded(true)
    }
  }, [])

  // Поллинг каждые 5 с — только пока панель открыта
  useEffect(() => {
    if (!open) return
    setLoaded(false)
    void refresh(channel)
    const id = window.setInterval(() => void refresh(channel), REFRESH_MS)
    return () => window.clearInterval(id)
  }, [open, channel, refresh])

  const clear = async () => {
    try {
      const targets = channel === ALL ? CHANNELS.map((item) => item.id) : [channel]
      await Promise.all(targets.map((id) => apiCall('clear_logs', id)))
      await refresh(channel)
    } catch (err) {
      setError(getErrorMessage(err, 'Не удалось очистить журнал'))
    }
  }

  const channelLabel = (id: string) => CHANNELS.find((item) => item.id === id)?.label ?? id

  return (
    <div className="flex h-full flex-col" style={{ width: JOURNAL_WIDTH }}>
      <div className="flex h-12 shrink-0 items-center justify-between gap-2 border-b border-border px-3">
        <span className="text-sm font-semibold text-foreground">Журнал</span>
        <div className="flex items-center gap-1">
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 text-muted-foreground"
            onClick={() => void clear()}
            aria-label="Очистить журнал"
            title="Очистить журнал"
          >
            <Trash2 className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 text-muted-foreground"
            onClick={onClose}
            aria-label="Закрыть журнал"
            title="Закрыть журнал"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="shrink-0 border-b border-border p-2">
        <SelectNative
          aria-label="Канал журнала"
          value={channel}
          onChange={(event) => setChannel(event.target.value)}
        >
          <option value={ALL}>Все каналы</option>
          {CHANNELS.map((item) => (
            <option key={item.id} value={item.id}>
              {item.label}
            </option>
          ))}
        </SelectNative>
      </div>

      <div className="sidebar-scroll min-h-0 flex-1 overflow-y-auto overscroll-contain p-2">
        {error ? (
          <p className="px-1 py-2 text-xs text-muted-foreground">{error}</p>
        ) : entries.length === 0 ? (
          <p className="px-1 py-2 text-xs text-muted-foreground">
            {loaded ? 'Журнал пуст' : 'Загружаем журнал…'}
          </p>
        ) : (
          <ol className="space-y-1">
            {entries.map((entry, index) => (
              <li
                key={`${entry.channel}-${entry.time}-${index}`}
                className="rounded-sm px-1.5 py-1 font-mono text-xs leading-snug hover:bg-muted/60"
              >
                <span className="text-muted-foreground">{entry.time || '—:—:—'}</span>
                {channel === ALL ? (
                  <span className="ml-1.5 text-muted-foreground">[{channelLabel(entry.channel)}]</span>
                ) : null}
                <span className="ml-1.5 break-words text-foreground">{entry.message}</span>
              </li>
            ))}
          </ol>
        )}
      </div>
    </div>
  )
}

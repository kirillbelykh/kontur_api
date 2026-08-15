export type JournalMessageTone = 'neutral' | 'success' | 'warning' | 'danger' | 'info'

/** Тон строки журнала по тексту — без «системного» dump, для обычного оператора. */
export function journalMessageTone(message: string): JournalMessageTone {
  const value = message.toLowerCase()
  if (/ошиб|error|fail|не удалось|exception|traceback|отклон/.test(value)) return 'danger'
  if (/успеш|готов|скачан|создан|отправл|заверш|провед|обновл/.test(value)) return 'success'
  if (/ожид|warn|повтор|retry|таймаут|timeout/.test(value)) return 'warning'
  if (/старт|запуск|начал|подключ|провер/.test(value)) return 'info'
  return 'neutral'
}

export type StatusTone =
  | 'neutral'
  | 'success'
  | 'warning'
  | 'danger'
  | 'primary'
  | 'info'
  | 'violet'
  | 'teal'

export type StatusMeta = {
  tone: StatusTone
  /** Переходный статус — по тексту идёт перелив (shimmer). */
  pending: boolean
}

/**
 * Единая раскраска статусов для всех таблиц: у разных статусов — разный цвет
 * кружка, а «в процессе» статусы помечаются pending для shimmer-анимации.
 */
export function statusMeta(status?: string): StatusMeta {
  const value = (status || '').toLowerCase()
  if (!value) return { tone: 'neutral', pending: false }

  if (value.includes('ошиб') || value.includes('error') || value.includes('reject') || value.includes('отклон')) {
    return { tone: 'danger', pending: false }
  }

  // Переходные (крутится shimmer)
  if (
    value.includes('ожид') ||
    value.includes('обработ') ||
    value.includes('создаёт') ||
    value.includes('создает') ||
    value.includes('закладыва') ||
    value.includes('заклад') ||
    value.includes('закрыва') ||
    value.includes('pending') ||
    value.includes('process') ||
    value.includes('creat')
  ) {
    return { tone: 'warning', pending: true }
  }

  if (value.includes('наполн')) return { tone: 'violet', pending: false }
  if (value.includes('оборот')) return { tone: 'primary', pending: false }
  if (value.includes('скачан') || value.includes('download')) return { tone: 'teal', pending: false }
  if (
    value.includes('получен') ||
    value.includes('готов') ||
    value.includes('ready') ||
    value.includes('released') ||
    value.includes('received') ||
    value.includes('провед') ||
    value.includes('зарегистр') ||
    value.includes('approved')
  ) {
    return { tone: 'success', pending: false }
  }
  if (value.includes('черновик') || value.includes('draft') || value.includes('нов') || value.includes('созд')) {
    return { tone: 'info', pending: false }
  }
  return { tone: 'neutral', pending: false }
}

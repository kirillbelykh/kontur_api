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

  if (value.includes('скачив')) {
    return { tone: 'success', pending: true }
  }
  if (value.includes('не скач')) {
    return { tone: 'info', pending: false }
  }

  // Первые статусы после действия — пока Контур не перевёл заказ дальше
  if (
    value.includes('проверк') ||
    value.includes('создаёт') ||
    value.includes('создает') ||
    value.includes('ожид') ||
    value.includes('вводит') ||
    value.includes('отправляется') ||
    value.includes('подписывается') ||
    value.includes('печата') ||
    value.includes('обработ') ||
    value.includes('заклад') ||
    value.includes('закрыва') ||
    value.includes('наполня') ||
    value.includes('проводится') ||
    value.includes('проводятся') ||
    value.includes('заказыва') ||
    value.includes('sendforrelease')
  ) {
    return { tone: 'warning', pending: true }
  }

  if (value.includes('отправ') || value.includes('подпис')) {
    return { tone: 'warning', pending: false }
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

/** Кольцо-спиннер только у активного скачивания, не у остальных pending-статусов. */
export function statusShowsSpinner(status?: string): boolean {
  const value = (status || '').toLowerCase()
  return statusMeta(status).pending && value.includes('скачив')
}

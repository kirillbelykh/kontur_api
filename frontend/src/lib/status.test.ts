import { describe, expect, it } from 'vitest'
import { statusMeta } from './status'

describe('statusMeta', () => {
  it('ошибочные статусы — danger, без shimmer', () => {
    expect(statusMeta('Ошибка')).toEqual({ tone: 'danger', pending: false })
    expect(statusMeta('Отклонён')).toEqual({ tone: 'danger', pending: false })
  })

  it('перелив только у «На проверке» и «Скачивается»', () => {
    expect(statusMeta('На проверке')).toEqual({ tone: 'warning', pending: true })
    expect(statusMeta('Скачивается')).toEqual({ tone: 'warning', pending: true })
  })

  it('отправка и подпись — без перелива', () => {
    for (const status of ['Отправлен на подпись', 'Отправлено', 'Не отправлено']) {
      const meta = statusMeta(status)
      expect(meta.pending, status).toBe(false)
      expect(meta.tone, status).toBe('warning')
    }
  })

  it('разные статусы получают разные цвета кружка', () => {
    const tones = new Set(
      ['Коды получены', 'Наполнен на ТСД', 'Введён в оборот', 'Скачан', 'Черновик', 'Ошибка'].map(
        (status) => statusMeta(status).tone,
      ),
    )
    expect(tones.size).toBe(6)
  })

  it('пустой статус — нейтральный', () => {
    expect(statusMeta('')).toEqual({ tone: 'neutral', pending: false })
    expect(statusMeta(undefined)).toEqual({ tone: 'neutral', pending: false })
  })
})

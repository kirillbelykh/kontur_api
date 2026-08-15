import { describe, expect, it } from 'vitest'
import { journalMessageTone } from './journal'

describe('journalMessageTone', () => {
  it('красит ошибки, успехи и ожидание по тексту', () => {
    expect(journalMessageTone('Не удалось скачать заказ')).toBe('danger')
    expect(journalMessageTone('Заказы скачаны')).toBe('success')
    expect(journalMessageTone('Ожидаем ответ Контура')).toBe('warning')
    expect(journalMessageTone('Подключаемся к сессии')).toBe('info')
    expect(journalMessageTone('Список АК загружен')).toBe('neutral')
  })
})

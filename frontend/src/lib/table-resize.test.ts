import { describe, expect, it } from 'vitest'
import { resizeMapToWidths, tableStorageKey } from './table-resize'

describe('resizeMapToWidths', () => {
  it('берёт только числовые ширины и ключи колонок', () => {
    const sizes = new Map<unknown, unknown>([
      ['col-0', 120],
      ['col-1', '1fr'],
      ['col-2', 88.6],
      [3, 40],
    ])
    expect(resizeMapToWidths(sizes)).toEqual({
      'col-0': 120,
      'col-2': 89,
      '3': 40,
    })
  })

  it('пустая карта даёт пустые ширины', () => {
    expect(resizeMapToWidths(new Map())).toEqual({})
  })

  it('ключ настроек таблицы не зависит от pathname', () => {
    expect(tableStorageKey('Заказы к загрузке', ['Выбор', 'Заявка'])).toBe(
      'kontur_table_prefs_v4_Заказы к загрузке_Выбор|Заявка',
    )
  })
})

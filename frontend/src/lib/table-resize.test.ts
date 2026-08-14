import { describe, expect, it } from 'vitest'
import { resizeMapToWidths } from './table-resize'

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
})

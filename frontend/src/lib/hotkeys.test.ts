import { describe, expect, it } from 'vitest'
import { isTypingTarget, SECTION_ROUTES } from './hotkeys'

describe('hotkeys', () => {
  it('шесть разделов для Ctrl+1…6', () => {
    expect(SECTION_ROUTES).toHaveLength(6)
  })

  it('не считает кнопки полем ввода', () => {
    expect(isTypingTarget({ tagName: 'BUTTON' } as unknown as EventTarget)).toBe(false)
    expect(isTypingTarget({ tagName: 'INPUT' } as unknown as EventTarget)).toBe(true)
    expect(isTypingTarget({ tagName: 'TEXTAREA' } as unknown as EventTarget)).toBe(true)
  })
})

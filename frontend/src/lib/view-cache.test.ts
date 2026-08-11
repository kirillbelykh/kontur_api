import { describe, expect, it } from 'vitest'
import { renderToString } from 'react-dom/server'
import { createElement } from 'react'
import { hasCachedState, useCachedState } from './view-cache'

function Probe({ cacheKey, next }: { cacheKey: string; next?: string[] }) {
  const [value, setValue] = useCachedState<string[]>(cacheKey, [])
  if (next && value.length === 0) setValue(next)
  return createElement('span', null, JSON.stringify(value))
}

describe('useCachedState', () => {
  it('восстанавливает данные при повторном монтировании (переключение разделов)', () => {
    const key = `test-${Math.random()}`
    expect(hasCachedState(key)).toBe(false)

    renderToString(createElement(Probe, { cacheKey: key, next: ['a', 'b'] }))
    expect(hasCachedState(key)).toBe(true)

    // Новый «монтаж» того же раздела — данные не потеряны
    const html = renderToString(createElement(Probe, { cacheKey: key }))
    expect(html).toContain('a')
    expect(html).toContain('b')
  })
})

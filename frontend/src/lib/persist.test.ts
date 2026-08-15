import { afterEach, describe, expect, it, vi } from 'vitest'
import { readPersisted, writePersisted } from './persist'

describe('persist', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('читает записанное значение и отдаёт fallback при мусоре', () => {
    const store: Record<string, string> = {}
    vi.stubGlobal('window', {
      localStorage: {
        getItem: (key: string) => (key in store ? store[key] : null),
        setItem: (key: string, value: string) => {
          store[key] = value
        },
        removeItem: (key: string) => {
          delete store[key]
        },
      },
    })

    const key = 'kontur-test-persist'
    expect(readPersisted(key, { a: 1 })).toEqual({ a: 1 })
    writePersisted(key, { a: 2 })
    expect(readPersisted(key, { a: 1 })).toEqual({ a: 2 })
    store[key] = '{'
    expect(readPersisted(key, { a: 1 })).toEqual({ a: 1 })
  })
})

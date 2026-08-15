import { describe, expect, it } from 'vitest'
import { parseDownloadProgress } from './progress'

describe('parseDownloadProgress', () => {
  it('читает progress по одному id и по списку', () => {
    expect(parseDownloadProgress({ documentId: 'a', progress: 0.4 })).toEqual({
      ids: ['a'],
      progress: 0.4,
    })
    expect(parseDownloadProgress({ documentIds: ['a', 'b'], progress: 1.4 })).toEqual({
      ids: ['a', 'b'],
      progress: 1,
    })
  })

  it('отбрасывает пустой payload', () => {
    expect(parseDownloadProgress(null)).toBeNull()
    expect(parseDownloadProgress({ progress: 0.2 })).toBeNull()
    expect(parseDownloadProgress({ documentId: 'a', progress: Number.NaN })).toBeNull()
  })
})

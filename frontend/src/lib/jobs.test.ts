import { beforeEach, describe, expect, it, vi } from 'vitest'
import { toast } from 'sonner'
import { notifyJob } from './jobs'

vi.mock('sonner', () => ({
  toast: {
    loading: vi.fn(),
    success: vi.fn(),
    error: vi.fn(),
    dismiss: vi.fn(),
  },
}))

describe('notifyJob', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('меняет loading-тост на success с тем же id', async () => {
    const result = await notifyJob('download:download', async () => 1, {
      pending: 'Скачивание кодов…',
      success: 'Заказы скачаны.',
    })
    expect(result).toBe(1)
    expect(toast.loading).toHaveBeenCalledWith('Скачивание кодов…', { id: 'job:download:download' })
    expect(toast.success).toHaveBeenCalledWith('Заказы скачаны.', {
      id: 'job:download:download',
      duration: 8000,
    })
  })

  it('меняет loading-тост на error, если операция упала', async () => {
    await expect(
      notifyJob(
        'intro:run',
        async () => {
          throw new Error('Укажите номер партии.')
        },
        { pending: 'Ввод в оборот…', success: 'Готово' },
      ),
    ).rejects.toThrow('Укажите номер партии.')
    expect(toast.error).toHaveBeenCalledWith('Укажите номер партии.', {
      id: 'job:intro:run',
      duration: 8000,
    })
  })
})

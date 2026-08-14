import { beforeEach, describe, expect, it, vi } from 'vitest'
import { toast } from 'sonner'
import { notifyJob } from './jobs'

vi.mock('sonner', () => {
  const toast = Object.assign(vi.fn(), {
    success: vi.fn(),
    error: vi.fn(),
    dismiss: vi.fn(),
  })
  return { toast }
})

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
    expect(toast).toHaveBeenCalledWith(
      'Скачивание кодов…',
      expect.objectContaining({
        id: 'job:download:download',
        duration: Infinity,
        closeButton: true,
      }),
    )
    expect(toast.success).toHaveBeenCalledWith('Заказы скачаны.', {
      id: 'job:download:download',
      duration: 8000,
      closeButton: true,
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
      closeButton: true,
    })
  })

  it('без pending показывает только success по завершении', async () => {
    await notifyJob('download:download', async () => 1, {
      success: 'Заказы скачаны.',
    })
    expect(toast).not.toHaveBeenCalled()
    expect(toast.success).toHaveBeenCalledWith('Заказы скачаны.', {
      id: 'job:download:download',
      duration: 8000,
      closeButton: true,
    })
  })

  it('не показывает success, если pending-тост закрыли крестиком', async () => {
    vi.mocked(toast).mockImplementation((_message, options) => {
      options?.onDismiss?.({} as never)
      return 'job:download:download'
    })
    await notifyJob('download:download', async () => 1, {
      pending: 'Скачивание кодов…',
      success: 'Заказы скачаны.',
    })
    expect(toast.success).not.toHaveBeenCalled()
  })
})

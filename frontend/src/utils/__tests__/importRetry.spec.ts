import { afterEach, describe, expect, it, vi } from 'vitest'
import { importWithRetry } from '../importRetry'

afterEach(() => { vi.useRealTimers() })

describe('有界模块重试', () => {
  it('最多四次导入，不清缓存、不改写 URL 或自动刷新', async () => {
    vi.useFakeTimers()
    const url = window.location.href
    const loader = vi.fn().mockRejectedValue(new Error('Failed to fetch dynamically imported module'))
    const result = importWithRetry(loader, 100)
    const assertion = expect(result).rejects.toThrow('Failed to fetch')
    await vi.runAllTimersAsync()
    await assertion
    expect(loader).toHaveBeenCalledTimes(4)
    expect(window.location.href).toBe(url)
  })

  it('语法等非网络错误不重试', async () => {
    const loader = vi.fn().mockRejectedValue(new SyntaxError('bad module'))
    await expect(importWithRetry(loader)).rejects.toThrow('bad module')
    expect(loader).toHaveBeenCalledTimes(1)
  })

  it('相同缓存键合并并发请求，失败后允许手动重试', async () => {
    const failing = vi.fn().mockRejectedValue(new Error('Failed to fetch'))
    const first = importWithRetry(failing, 0, 'test-failed')
    const second = importWithRetry(failing, 0, 'test-failed')
    await expect(first).rejects.toThrow('Failed to fetch')
    await expect(second).rejects.toThrow('Failed to fetch')
    expect(failing).toHaveBeenCalledTimes(1)
    const recovered = vi.fn().mockResolvedValue('ready')
    await expect(importWithRetry(recovered, 0, 'test-failed')).resolves.toBe('ready')
  })
})

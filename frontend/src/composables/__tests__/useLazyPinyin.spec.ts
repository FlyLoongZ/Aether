import { computed } from 'vue'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const loadPinyinMock = vi.hoisted(() => vi.fn())
const isPinyinLoadedMock = vi.hoisted(() => vi.fn(() => false))

vi.mock('@/utils/search', () => ({
  loadPinyin: loadPinyinMock,
  isPinyinLoaded: isPinyinLoadedMock,
}))

import { __resetPinyinLoaderForTests, useLazyPinyin } from '../useLazyPinyin'

async function flushPromises(): Promise<void> {
  await Promise.resolve()
  await Promise.resolve()
}

describe('useLazyPinyin', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    loadPinyinMock.mockReset()
    isPinyinLoadedMock.mockReset()
    isPinyinLoadedMock.mockReturnValue(false)
    __resetPinyinLoaderForTests()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('loads once for concurrent callers and re-runs tracked computeds', async () => {
    loadPinyinMock.mockResolvedValue(true)
    const { ensureLoaded, track } = useLazyPinyin()
    let runs = 0
    const probe = computed(() => {
      track()
      runs += 1
      return runs
    })
    expect(probe.value).toBe(1)

    ensureLoaded()
    ensureLoaded()
    expect(loadPinyinMock).toHaveBeenCalledTimes(1)

    await flushPromises()
    expect(probe.value).toBe(2)

    ensureLoaded()
    expect(loadPinyinMock).toHaveBeenCalledTimes(1)
  })

  it('backs off for 30s after a failed load, then retries', async () => {
    loadPinyinMock.mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const { ensureLoaded } = useLazyPinyin()

    ensureLoaded()
    await flushPromises()
    expect(loadPinyinMock).toHaveBeenCalledTimes(1)

    ensureLoaded()
    await flushPromises()
    expect(loadPinyinMock).toHaveBeenCalledTimes(1)

    vi.advanceTimersByTime(30_000)
    ensureLoaded()
    await flushPromises()
    expect(loadPinyinMock).toHaveBeenCalledTimes(2)
  })
})

import { beforeEach, describe, expect, it, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const { getAllStatusMock, setEnabledMock, getUserStatusMock } = vi.hoisted(() => ({
  getAllStatusMock: vi.fn(),
  setEnabledMock: vi.fn(),
  getUserStatusMock: vi.fn()
}))

vi.mock('@/api/modules', () => ({
  modulesApi: {
    getAllStatus: getAllStatusMock,
    setEnabled: setEnabledMock,
    getUserStatus: getUserStatusMock
  }
}))

import { useModuleStore } from '@/stores/modules'

function status(enabled: boolean) {
  return {
    'chat.pii': {
      name: 'chat.pii',
      available: true,
      enabled,
      active: enabled
    }
  }
}

describe('module store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    getAllStatusMock.mockReset()
    setEnabledMock.mockReset()
    getUserStatusMock.mockReset()
  })

  it('re-reads status after a write instead of reusing the in-flight snapshot', async () => {
    // The refresh that started *before* the write resolves with the old state; reusing
    // it made the switch snap back to its previous position after a successful write.
    let releaseStale: (value: unknown) => void = () => {}
    getAllStatusMock
      .mockImplementationOnce(
        () =>
          new Promise(resolve => {
            releaseStale = resolve
          })
      )
      .mockResolvedValueOnce(status(true))

    const store = useModuleStore()
    const staleFetch = store.fetchModules()
    setEnabledMock.mockResolvedValue(undefined)

    const write = store.setEnabled('chat.pii', true)
    releaseStale(status(false))
    await staleFetch
    await write

    expect(store.isEnabled('chat.pii')).toBe(true)
    expect(getAllStatusMock).toHaveBeenCalledTimes(2)
  })

  it('marks the snapshot stale when a refresh fails but keeps the last known state', async () => {
    getAllStatusMock.mockResolvedValueOnce(status(true))
    const store = useModuleStore()
    await store.fetchModules()
    expect(store.isEnabled('chat.pii')).toBe(true)
    expect(store.stale).toBe(false)

    getAllStatusMock.mockRejectedValueOnce(new Error('offline'))
    await expect(store.fetchModules()).rejects.toThrow()

    // Hidden menus would be worse than a stale one; the flag tells the UI it is old.
    expect(store.isEnabled('chat.pii')).toBe(true)
    expect(store.stale).toBe(true)
  })

  it('clears user modules on a failed read so a disabled feature cannot stay visible', async () => {
    getUserStatusMock.mockResolvedValueOnce({ 'chat.pii': { active: true } })
    const store = useModuleStore()
    await store.fetchUserModules()
    expect(store.isUserActive('chat.pii')).toBe(true)

    getUserStatusMock.mockRejectedValueOnce(new Error('offline'))
    await expect(store.fetchUserModules()).rejects.toThrow()
    expect(store.isUserActive('chat.pii')).toBe(false)
  })
})

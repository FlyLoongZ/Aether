import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { disposeTheme, initializeTheme, setThemeMode, subscribeTheme } from '../runtime'

let media: MediaQueryList
let changes: EventTarget

beforeEach(() => {
  disposeTheme()
  document.documentElement.removeAttribute('data-theme')
  document.documentElement.removeAttribute('data-theme-mode')
  document.documentElement.classList.remove('dark')
  changes = new EventTarget()
  media = {
    matches: true,
    addEventListener: vi.fn((type, listener) => changes.addEventListener(type, listener as EventListener)),
    removeEventListener: vi.fn((type, listener) => changes.removeEventListener(type, listener as EventListener)),
  } as unknown as MediaQueryList
  vi.stubGlobal('matchMedia', () => media)
})
afterEach(() => {
  disposeTheme()
  vi.unstubAllGlobals()
})

describe('全局主题运行时', () => {
  it('接管 head 的深色状态，不先回到浅色、不写入默认偏好', () => {
    document.documentElement.dataset.theme = 'dark'
    document.documentElement.dataset.themeMode = 'system'
    document.documentElement.classList.add('dark')
    const subscriber = vi.fn()
    const unsubscribe = subscribeTheme(subscriber)
    initializeTheme()
    expect(subscriber.mock.calls[0][0]).toEqual({ mode: 'system', resolved: 'dark' })
    expect(localStorage.getItem('theme')).toBeNull()
    expect(document.documentElement.dataset.themeTransition).toBeUndefined()
    expect(media.addEventListener).toHaveBeenCalledTimes(1)
    unsubscribe()
  })

  it('仅系统模式响应系统变化', () => {
    initializeTheme()
    Object.defineProperty(media, 'matches', { value: false, configurable: true })
    changes.dispatchEvent(new Event('change'))
    expect(document.documentElement.dataset.theme).toBe('light')
    setThemeMode('dark')
    changes.dispatchEvent(new Event('change'))
    expect(document.documentElement.dataset.theme).toBe('dark')
    expect(localStorage.getItem('theme')).toBe('dark')
  })

  it('跨标签页变更及清空存储会同步主题而不会写回', () => {
    initializeTheme()
    localStorage.setItem('theme', 'light')
    window.dispatchEvent(new StorageEvent('storage', { key: 'theme', newValue: 'light' }))
    expect(document.documentElement.dataset.theme).toBe('light')
    localStorage.clear()
    window.dispatchEvent(new StorageEvent('storage', { key: null }))
    expect(document.documentElement.dataset.theme).toBe('dark')
    expect(localStorage.getItem('theme')).toBeNull()
  })

  it('卸载会清理监听，并且显式切换立即更新 DOM', () => {
    initializeTheme()
    setThemeMode('light')
    expect(document.documentElement.dataset.theme).toBe('light')
    expect(document.documentElement.dataset.themeTransition).toBe('true')
    disposeTheme()
    expect(media.removeEventListener).toHaveBeenCalledTimes(1)
    expect(document.documentElement.dataset.themeTransition).toBeUndefined()
  })
})

import { describe, expect, it, vi } from 'vitest'
import { bootstrapTheme, parseThemeMode, resolveTheme } from '../core'

describe('首屏主题规则', () => {
  it.each([
    ['dark', false, 'dark'], ['light', true, 'light'],
    ['system', true, 'dark'], [null, true, 'dark'], ['invalid', false, 'light'],
  ])('存储 %s，系统深色 %s → %s', (stored, dark, expected) => {
    const root = document.createElement('html')
    const win = {
      localStorage: { getItem: () => stored },
      matchMedia: () => ({ matches: dark }),
    } as unknown as Window
    const state = bootstrapTheme(win, root)
    expect(state.resolved).toBe(expected)
    expect(root.dataset.theme).toBe(expected)
    expect(root.dataset.themeMode).toBe(parseThemeMode(stored))
    expect(root.classList.contains('dark')).toBe(expected === 'dark')
  })

  it('存储不可读时跟随系统，不需要 body', () => {
    const win = {
      get localStorage() { throw new Error('SecurityError') },
      matchMedia: () => ({ matches: true }),
    } as unknown as Window
    const root = document.createElement('html')
    expect(bootstrapTheme(win, root)).toEqual({ mode: 'system', resolved: 'dark' })
    expect(root.dataset.themeTransition).toBeUndefined()
  })

  it('系统偏好检测不可用时安全回退', () => {
    const win = {
      localStorage: { getItem: vi.fn(() => null) },
      matchMedia: () => { throw new Error('unsupported') },
    } as unknown as Window
    expect(bootstrapTheme(win, document.createElement('html')).resolved).toBe('light')
    expect(resolveTheme('light', true)).toBe('light')
  })
})

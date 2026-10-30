import {
  applyTheme, bootstrapTheme, parseThemeMode, readThemeMode, resolveTheme,
  SYSTEM_THEME_QUERY, THEME_STORAGE_KEY, type ThemeMode, type ThemeState,
} from './core'

let state: ThemeState = { mode: 'system', resolved: 'light' }
let initialized = false
let mediaQuery: MediaQueryList | null = null
let animationTimer: ReturnType<typeof setTimeout> | undefined
const subscribers = new Set<(state: ThemeState) => void>()

function update(mode: ThemeMode, animate = false): void {
  state = { mode, resolved: resolveTheme(mode, mediaQuery?.matches ?? false) }
  if (typeof document !== 'undefined') {
    const root = document.documentElement
    if (animate && root.dataset.theme !== state.resolved) {
      root.dataset.themeTransition = 'true'
      clearTimeout(animationTimer)
      animationTimer = setTimeout(() => { delete root.dataset.themeTransition }, 250)
    }
    applyTheme(root, state)
  }
  subscribers.forEach(subscriber => subscriber(state))
}

function onSystemChange(): void {
  if (state.mode === 'system') update('system')
}

function onStorage(event: StorageEvent): void {
  if (event.key !== THEME_STORAGE_KEY && event.key !== null) return
  try {
    if (event.storageArea && event.storageArea !== window.localStorage) return
  } catch { return }
  update(readThemeMode(window))
}

export function initializeTheme(): ThemeState {
  if (initialized || typeof window === 'undefined') return state
  initialized = true
  // 接管 head 已写入的状态；不先重置到浅色，也不在初始化时写入存储。
  const root = document.documentElement
  state = root.dataset.theme === 'light' || root.dataset.theme === 'dark'
    ? { mode: parseThemeMode(root.dataset.themeMode), resolved: root.dataset.theme }
    : bootstrapTheme(window, root)
  try {
    mediaQuery = window.matchMedia(SYSTEM_THEME_QUERY)
    mediaQuery.addEventListener('change', onSystemChange)
  } catch {
    mediaQuery = null
  }
  window.addEventListener('storage', onStorage)
  if (mediaQuery) update(state.mode)
  return state
}

export function subscribeTheme(subscriber: (state: ThemeState) => void): () => void {
  subscriber(initializeTheme())
  subscribers.add(subscriber)
  return () => { subscribers.delete(subscriber) }
}

export function setThemeMode(mode: ThemeMode): void {
  initializeTheme()
  try { window.localStorage.setItem(THEME_STORAGE_KEY, mode) } catch { /* 存储受限不影响切换 */ }
  update(mode, true)
}

export function disposeTheme(): void {
  if (typeof window !== 'undefined') window.removeEventListener('storage', onStorage)
  mediaQuery?.removeEventListener('change', onSystemChange)
  clearTimeout(animationTimer)
  if (typeof document !== 'undefined') delete document.documentElement.dataset.themeTransition
  mediaQuery = null
  initialized = false
  subscribers.clear()
}

if (import.meta.hot) import.meta.hot.dispose(disposeTheme)

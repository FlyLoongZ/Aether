export const THEME_STORAGE_KEY = 'theme'
export const SYSTEM_THEME_QUERY = '(prefers-color-scheme: dark)'
export type ThemeMode = 'system' | 'light' | 'dark'
export type ResolvedTheme = 'light' | 'dark'
export interface ThemeState {
  mode: ThemeMode
  resolved: ResolvedTheme
}

export function parseThemeMode(value: unknown): ThemeMode {
  return value === 'light' || value === 'dark' ? value : 'system'
}

export function readThemeMode(win: Window): ThemeMode {
  try {
    return parseThemeMode(win.localStorage.getItem(THEME_STORAGE_KEY))
  } catch {
    return 'system'
  }
}

export function systemPrefersDark(win: Window): boolean {
  try {
    return win.matchMedia(SYSTEM_THEME_QUERY).matches
  } catch {
    return false
  }
}

export function resolveTheme(mode: ThemeMode, prefersDark: boolean): ResolvedTheme {
  return mode === 'system' ? (prefersDark ? 'dark' : 'light') : mode
}

export function applyTheme(root: HTMLElement, state: ThemeState): void {
  root.dataset.themeMode = state.mode
  root.dataset.theme = state.resolved
  root.classList.toggle('dark', state.resolved === 'dark')
}

export function bootstrapTheme(win: Window, root: HTMLElement): ThemeState {
  const mode = readThemeMode(win)
  const state = { mode, resolved: resolveTheme(mode, systemPrefersDark(win)) }
  applyTheme(root, state)
  return state
}

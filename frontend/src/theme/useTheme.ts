import { computed, readonly, ref } from 'vue'
import { initializeTheme, setThemeMode, subscribeTheme } from './runtime'
import type { ThemeMode } from './core'

const themeMode = ref<ThemeMode>('system')
const isDark = ref(false)
let subscribed = false

export function useTheme() {
  initializeTheme()
  if (!subscribed) {
    subscribed = true
    subscribeTheme(state => {
      themeMode.value = state.mode
      isDark.value = state.resolved === 'dark'
    })
  }
  return {
    themeMode: readonly(themeMode),
    isDark: readonly(isDark),
    isSystemMode: computed(() => themeMode.value === 'system'),
    setThemeMode,
    setDarkMode: (dark: boolean) => setThemeMode(dark ? 'dark' : 'light'),
    toggleDarkMode: () => setThemeMode(
      themeMode.value === 'system' ? 'light' : themeMode.value === 'light' ? 'dark' : 'system',
    ),
  }
}

if (import.meta.hot) import.meta.hot.dispose(() => { subscribed = false })

import type { App, InjectionKey, Ref } from 'vue'
import { computed, inject, readonly, ref } from 'vue'
import {
  defaultLocale,
  localeBundleLoaded,
  registerLocaleBundle,
  supportedLocales,
  translateMessage,
  type Locale,
  type MessageKey,
} from './messages'

type Params = Record<string, string | number>

interface I18nContext {
  locale: Ref<Locale>
  setLocale: (locale: Locale) => Promise<void>
  t: (key: MessageKey, params?: Params) => string
}

const STORAGE_KEY = 'aether_locale'
const i18nKey: InjectionKey<I18nContext> = Symbol('aether-i18n')
const locale = ref<Locale>(readInitialLocale())

function isLocale(value: string | null | undefined): value is Locale {
  return !!value && supportedLocales.includes(value as Locale)
}

function readInitialLocale(): Locale {
  if (typeof window === 'undefined') return defaultLocale

  try {
    const stored = normalizeLocale(localStorage.getItem(STORAGE_KEY))
    if (stored) return stored
  } catch {
    // Browser storage may be unavailable; the in-memory preference still works.
  }

  const preferred = navigator.languages?.find(language => {
    const normalized = normalizeLocale(language)
    return isLocale(normalized)
  })
  const normalizedPreferred = normalizeLocale(preferred)
  return isLocale(normalizedPreferred) ? normalizedPreferred : defaultLocale
}

export function normalizeLocale(value: string | null | undefined): Locale | undefined {
  if (!value) return undefined
  const language = value.trim().toLowerCase().split(/[-_]/)[0]
  if (language === 'zh') return 'zh-CN'
  if (language === 'en') return 'en-US'
  return undefined
}

let localeBundleRequest: Promise<void> | null = null

/**
 * Loads a locale dictionary on demand.
 *
 * The default locale is bundled; every other locale arrives as its own chunk.
 * Callers must await this before relying on `translateMessage` for that locale,
 * and the bootstrap does so before mounting the app.
 */
export async function ensureLocaleMessages(nextLocale: Locale): Promise<void> {
  if (localeBundleLoaded(nextLocale)) return
  localeBundleRequest ??= import('./messages-en')
    .then(({ enMessages }) => {
      registerLocaleBundle('en-US', enMessages)
    })
    .catch(error => {
      // Reset the cached promise: leaving a rejected promise here made the locale
      // permanently unloadable for the lifetime of the page, so switching to
      // English kept failing after a single flaky chunk fetch (offline start,
      // stale service worker, transient CDN error) until a full reload.
      localeBundleRequest = null
      throw error
    })
  await localeBundleRequest
}

function applyLocale(nextLocale: Locale): void {
  locale.value = nextLocale
  if (typeof document !== 'undefined') {
    document.documentElement.lang = nextLocale
  }
  if (typeof window !== 'undefined') {
    try {
      localStorage.setItem(STORAGE_KEY, nextLocale)
    } catch {
      // Persistence is optional; do not interrupt language switching.
    }
  }
}

async function setLocale(nextLocale: Locale): Promise<void> {
  if (!isLocale(nextLocale)) return
  // The new language must be resident before it becomes active: translating with
  // a missing bundle would render raw keys for a frame.
  await ensureLocaleMessages(nextLocale)
  applyLocale(nextLocale)
}

/**
 * Loads `nextLocale` and reports whether it became active.
 *
 * Callers that a user clicked (the language switcher) need to tell "switched" from
 * "the chunk could not be loaded" so they can surface it; the silent variant above
 * is for bootstrap, where a missing bundle merely keeps the default locale.
 */
export async function trySetLocale(nextLocale: Locale): Promise<boolean> {
  if (!isLocale(nextLocale)) return false
  try {
    await ensureLocaleMessages(nextLocale)
  } catch {
    // Fall back to the bundled locale so the UI is never left half-translated.
    applyLocale(defaultLocale)
    return false
  }
  applyLocale(nextLocale)
  return true
}

export function translate(key: MessageKey, params?: Params): string {
  return translateMessage(key, locale.value, params)
}

const t = translate

const context: I18nContext = {
  locale,
  setLocale,
  t,
}

export function createI18n() {
  return {
    install(app: App) {
      app.provide(i18nKey, context)
      app.config.globalProperties.$t = t
      // The bootstrap already awaited the initial locale's dictionary, so the
      // install stays synchronous; an unloaded locale would fall back to the
      // async path.
      if (localeBundleLoaded(locale.value)) {
        applyLocale(locale.value)
      } else {
        void setLocale(locale.value)
      }
    }
  }
}

export function useI18n() {
  return inject(i18nKey, context)
}

export function setI18nLocale(locale: Locale): Promise<void> {
  // Apply immediately when the dictionary is already resident: the default
  // locale always is, and so is any locale loaded by an earlier switch. Only a
  // first-time switch has to wait for its chunk.
  if (localeBundleLoaded(locale)) {
    applyLocale(locale)
    return Promise.resolve()
  }
  return setLocale(locale)
}

export function getI18nLocale(): Locale {
  return locale.value
}

export function useLocaleOptions() {
  const { locale: currentLocale, setLocale: applyLocale } = useI18n()
  const currentLocaleLabel = computed(() => {
    return currentLocale.value === 'zh-CN' ? t('common.chinese') : t('common.english')
  })

  return {
    locale: readonly(currentLocale),
    supportedLocales,
    currentLocaleLabel,
    setLocale: applyLocale,
    /** Switches language and reports whether the dictionary actually loaded. */
    trySetLocale,
  }
}

export type { Locale, MessageKey }

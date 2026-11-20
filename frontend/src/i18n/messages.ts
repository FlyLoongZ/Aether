import { zhMessages } from './messages-zh'

/**
 * Message resolution.
 *
 * Only the default locale is bundled: `messages-zh` is imported statically,
 * while every other locale is registered at runtime from a dynamic import (see
 * `ensureLocaleMessages` in `./index`). That keeps a user who never switches
 * language from downloading a second full dictionary.
 *
 * There is deliberately no default-locale fallback in `translateMessage`: with
 * two aligned dictionaries (enforced by `translation-coverage.spec.ts`) a missing
 * key is a bug, and silently showing the default language hides it. A missing key
 * renders as the key itself, which is visible in review and in tests.
 */

export type Locale = 'zh-CN' | 'en-US'
export type MessageKey = keyof typeof zhMessages
export type MessageParams = Record<string, string | number>
export type Translator = (key: MessageKey, params?: MessageParams) => string

export const defaultLocale = 'zh-CN' as const
export const supportedLocales: Locale[] = ['zh-CN', 'en-US']

/** Every message key, in dictionary order. */
export const messageKeys = Object.keys(zhMessages) as MessageKey[]

const bundles: Record<Locale, Record<string, string>> = {
  'zh-CN': zhMessages,
  'en-US': {},
}

/** Installs a locale loaded on demand. */
export function registerLocaleBundle(locale: Exclude<Locale, typeof defaultLocale>, messages: Record<string, string>): void {
  bundles[locale] = messages
}

/** Whether the locale's dictionary is resident and can be used synchronously. */
export function localeBundleLoaded(locale: Locale): boolean {
  return Object.keys(bundles[locale]).length > 0
}

export function translateMessage(key: MessageKey, locale: Locale, params?: MessageParams): string {
  // Fall back to the bundled default locale before falling back to the raw key: a
  // locale whose chunk failed to load used to render `common.language` style keys
  // all over the UI instead of readable text.
  const template = bundles[locale]?.[key] ?? bundles[defaultLocale]?.[key] ?? key
  if (!params) return template
  return template.replace(/\{(\w+)\}/g, (_, name: string) => String(params[name] ?? `{${name}}`))
}

import { describe, expect, it } from 'vitest'
import { messageKeys, translateMessage, registerLocaleBundle, type MessageKey } from '@/i18n/messages'
import { zhMessages } from '@/i18n/messages-zh'
import { enMessages } from '@/i18n/messages-en'

const placeholders = (message: string) => [...message.matchAll(/\{(\w+)\}/g)]
  .map(match => match[1]).sort()

describe('translation coverage', () => {
  it('keeps locale keys and interpolation parameters aligned', () => {
    // English is loaded on demand at runtime, so the parity check reads the
    // aggregate directly instead of the registry.
    expect(Object.keys(enMessages).sort()).toEqual(Object.keys(zhMessages).sort())
    for (const key of messageKeys as MessageKey[]) {
      expect(placeholders(enMessages[key]), key).toEqual(placeholders(zhMessages[key]))
      expect(enMessages[key], key).not.toMatch(/[\u4e00-\u9fff]/u)
    }
  })

  it('interpolates parameters once without interpreting their contents as messages', () => {
    registerLocaleBundle('en-US', enMessages)
    expect(translateMessage('auth.login.title', 'en-US', { siteName: '保存 {other}' }))
      .toBe('Sign in to 保存 {other}')
    expect(translateMessage('site.privacy.currentVersion', 'en-US'))
      .toBe('Current version: {version}')
  })
})

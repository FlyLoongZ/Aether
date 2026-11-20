import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { setI18nLocale } from '@/i18n'
import { getPasswordPolicyHint, getPasswordPolicyPlaceholder, validatePasswordByPolicy } from '../passwordPolicy'

beforeEach(async () => { await setI18nLocale('zh-CN') })
afterEach(() => setI18nLocale('zh-CN'))

describe('passwordPolicy utils', () => {
  it('formats validation errors into a single message', async () => {
    expect(validatePasswordByPolicy('abc', 'strong')).toBe(
      '密码需要：至少 8 个字符、包含大写字母、包含数字、包含特殊字符',
    )
  })

  it('renders localized hints and placeholders', async () => {
    expect(getPasswordPolicyHint('strong')).toBe('至少 8 个字符，且需包含大写字母、小写字母、数字和特殊字符')
    await setI18nLocale('en-US')
    expect(getPasswordPolicyHint('strong')).not.toMatch(/[\u4e00-\u9fff]/u)
    expect(getPasswordPolicyPlaceholder('medium')).toBe('At least 8 characters, with letters and numbers')
    setI18nLocale('zh-CN')
    expect(getPasswordPolicyPlaceholder('weak')).toBe('至少 6 个字符')
  })
})

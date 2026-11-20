import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { PoolKeyDetail } from '@/api/endpoints/pool'
import { setI18nLocale, translate } from '@/i18n'
import {
  getQuotaLabelOrder,
  getQuotaProgressLabel,
  normalizeQuotaLabel,
  parseQuotaProgressItems,
} from '../poolKeyQuotaProgress'

beforeEach(async () => { await setI18nLocale('zh-CN') })
afterEach(() => {
  setI18nLocale('zh-CN')
  vi.useRealTimers()
})

describe('pool quota display localization', () => {
  it('retains canonical parser categories and sorting across locales', async () => {
    await setI18nLocale('en-US')
    expect(normalizeQuotaLabel('周剩余')).toBe('周')
    expect(getQuotaLabelOrder(normalizeQuotaLabel('周剩余'))).toBe(7)
    const key = { account_quota: '月剩余 25% | 5H剩余 50% | 周剩余 75%' } as PoolKeyDetail
    const items = parseQuotaProgressItems(key, 'codex', translate)
    expect(items.map(item => item.label)).toEqual(['5H', '周', '月'])
    expect(items.map(item => getQuotaProgressLabel(item.label))).toEqual(['5H', 'Weekly', 'Month'])
    expect(getQuotaProgressLabel('保存：自定义配额')).toBe('保存：自定义配额')
  })
})

import { describe, expect, it } from 'vitest'

import { getQuotaDisplayText } from '../providerKeyQuota'

describe('providerKeyQuota', () => {
  it('includes Codex Spark quota windows in display text', () => {
    expect(getQuotaDisplayText({
      status_snapshot: {
        oauth: {
          code: 'valid',
        },
        account: {
          code: 'ok',
          blocked: false,
        },
        quota: {
          provider_type: 'codex',
          code: 'ok',
          exhausted: false,
          windows: [
            {
              code: 'weekly',
              remaining_ratio: 0.9,
            },
            {
              code: '5h',
              remaining_ratio: 0.8,
            },
            {
              code: 'spark_5h',
              remaining_ratio: 0.6,
            },
            {
              code: 'spark_weekly',
              remaining_ratio: 0.95,
            },
          ],
        },
      },
    }, 'codex')).toBe('周剩余 90.0% | 5H剩余 80.0% | Spark5H剩余 60.0% | Spark周剩余 95.0%')
  })

  it('uses actual Codex window durations and ignores zero placeholders', () => {
    expect(getQuotaDisplayText({
      status_snapshot: {
        oauth: { code: 'valid' },
        account: { code: 'ok', blocked: false },
        quota: {
          provider_type: 'codex',
          code: 'ok',
          exhausted: false,
          windows: [
            {
              code: 'weekly',
              label: '周',
              window_minutes: 0,
              remaining_ratio: 1,
            },
            {
              code: '5h',
              label: '5H',
              window_minutes: 43_800,
              remaining_ratio: 0.86,
            },
          ],
        },
      },
    }, 'codex')).toBe('月剩余 86.0%')
  })

  it('keeps account and model Codex weekly quotas distinct in display text', () => {
    expect(getQuotaDisplayText({
      status_snapshot: {
        oauth: { code: 'valid' },
        account: { code: 'ok', blocked: false },
        quota: {
          provider_type: 'codex',
          code: 'ok',
          exhausted: false,
          windows: [
            {
              code: 'weekly',
              label: '周',
              scope: 'account',
              window_minutes: 10_080,
              remaining_ratio: 0.9,
            },
            {
              code: 'additional_0_primary',
              label: 'gpt-reserve',
              scope: 'model',
              model: 'gpt-reserve',
              window_minutes: 10_080,
              remaining_ratio: 0.4,
            },
          ],
        },
      },
    }, 'codex')).toBe('周剩余 90.0% | gpt-reserve 周剩余 40.0%')
  })

  it('formats xAI weekly credits as remaining percent', () => {
    expect(getQuotaDisplayText({
      status_snapshot: {
        oauth: { code: 'valid' },
        account: { code: 'ok', blocked: false },
        quota: {
          provider_type: 'xai',
          code: 'ok',
          exhausted: false,
          windows: [
            {
              code: 'usage',
              scope: 'account',
              used_ratio: 0.46,
              remaining_ratio: 0.54,
            },
            {
              code: 'prepaid',
              remaining_value: 12.5,
            },
          ],
        },
      },
    }, 'xai')).toBe('剩余 54.0% | 预付剩余 12.5')
  })
})

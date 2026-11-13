import { describe, expect, it } from 'vitest'

import { getOAuthRefreshFeedback } from '@/features/providers/utils/oauthRefreshFeedback'

describe('oauthRefreshFeedback', () => {
  it('reports blocked result after successful recheck', () => {
    expect(
      getOAuthRefreshFeedback({
        accountStateRecheckAttempted: true,
        snapshot: {
          status_snapshot: {
            oauth: { code: 'valid' },
            account: {
              code: 'account_disabled',
              label: '账号停用',
              reason: 'account has been deactivated',
              blocked: true,
            },
            quota: { code: 'unknown', exhausted: false },
          },
        },
      }),
    ).toEqual({
      tone: 'warning',
      message: 'Token 刷新成功，已重新检查额度/账号状态；当前状态仍是账号停用',
    })
  })

  it('reports plain success when no blocked state remains', () => {
    expect(
      getOAuthRefreshFeedback({
        accountStateRecheckAttempted: true,
        accountStateRecheckError: null,
      }),
    ).toEqual({
      tone: 'success',
      message: 'Token 刷新成功，已重新检查额度/账号状态',
    })
  })

  it('includes the recheck failure detail after a successful token refresh', () => {
    expect(
      getOAuthRefreshFeedback({
        accountStateRecheckAttempted: true,
        accountStateRecheckError: 'wham/usage API 返回状态码 401',
      }),
    ).toEqual({
      tone: 'warning',
      message: 'Token 刷新成功，但额度/账号状态复检失败：wham/usage API 返回状态码 401',
    })
  })
})

import { describe, expect, it } from 'vitest'
import { parseImportText } from '@/features/providers/utils/oauthCredentialImport'

const parseCodexCredential = (text: string) => parseImportText(text, 'codex')

describe('OAuth credential import', () => {
  it.each(['session_token', 'sessionToken', 'sso_token', 'ssoToken'])(
    'does not promote removed web credential %s to an access token', field => {
      expect(parseCodexCredential(JSON.stringify({ [field]: 'removed-web-token' }))).toBeNull()
    },
  )

  it.each([
    { access_token: 'access-token' },
    { accessToken: 'access-token' },
    { headers: { Authorization: 'Bearer access-token' } },
    { access_token: 'access-token', session_token: 'must-not-win' },
  ])('preserves supported access-token credentials (%j)', payload => {
    expect(parseCodexCredential(JSON.stringify(payload))?.access_token).toBe('access-token')
  })

  it('preserves refresh-token imports', () => {
    expect(parseCodexCredential('{"refresh_token":"refresh-token"}')?.refresh_token).toBe('refresh-token')
  })
})

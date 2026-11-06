
import { translate as t } from '@/i18n'
export interface OAuthImportCredentials {
  api_key?: string
  token?: string
  refresh_token?: string
  access_token?: string
  password?: string
  expires_at?: number
  name?: string
  email?: string
  account_id?: string
  account_user_id?: string
  plan_type?: string
  pool_tier?: string
  sso_rw_token?: string
  cf_cookies?: string
  cf_clearance?: string
  user_agent?: string
  browser_profile?: string
  user_id?: string
  account_name?: string
  headers?: Record<string, string>
}

export function parseImportText(text: string, providerType: string | null | undefined): OAuthImportCredentials | null {
  const trimmed = text.trim()
  if (!trimmed) return null

  const normalizedProviderType = (providerType || '').toLowerCase()
  const isKiroProvider = normalizedProviderType === 'kiro'
  const isXaiProvider = normalizedProviderType === 'xai'
  const isClaudeCodeProvider = normalizedProviderType === 'claude_code'

  // Kiro: keep full JSON so backend can extract auth_method/region/client_id, etc.
  if (isKiroProvider) {
    return { refresh_token: trimmed }
  }

  if (isXaiProvider) {
    try {
      const parsed: unknown = JSON.parse(trimmed)
      if (typeof parsed === 'object' && parsed !== null) {
        const obj = parsed as Record<string, unknown>
        const apiKey = normalizeStringField(obj.api_key) ?? normalizeStringField(obj.apiKey)
        const refreshToken = normalizeStringField(obj.refresh_token) ?? normalizeStringField(obj.refreshToken)
        const accessToken = normalizeStringField(obj.access_token) ?? normalizeStringField(obj.accessToken) ?? apiKey
        if (refreshToken || accessToken) {
          return {
            refresh_token: refreshToken,
            access_token: accessToken,
            name: normalizeStringField(obj.name) ?? normalizeStringField(obj.email),
            email: normalizeStringField(obj.email),
          }
        }
      }
    } catch {
      // Raw xAI API keys / access tokens are imported as access_token.
    }
    return { access_token: trimmed }
  }

  if (isClaudeCodeProvider) {
    try {
      const parsed: unknown = JSON.parse(trimmed)
      if (isObjectRecord(parsed) && isObjectRecord(parsed.claudeAiOauth)) {
        const claudeAiOauth = parsed.claudeAiOauth
        const refreshToken = normalizeStringField(claudeAiOauth.refreshToken)
          ?? normalizeStringField(claudeAiOauth.refresh_token)
        const accessToken = normalizeStringField(claudeAiOauth.accessToken)
          ?? normalizeStringField(claudeAiOauth.access_token)
        if (refreshToken || accessToken) {
          return {
            refresh_token: refreshToken,
            access_token: accessToken,
            expires_at: normalizeClaudeCredentialsExpiry(
              claudeAiOauth.expiresAt ?? claudeAiOauth.expires_at,
            ),
          }
        }
      }
    } catch {
      // Raw Claude refresh tokens continue through the generic import path.
    }
  }

  try {
    const parsed: unknown = JSON.parse(trimmed)
    if (typeof parsed === 'object' && parsed !== null) {
      const obj = parsed as Record<string, unknown>
      const refreshToken = obj.refresh_token
      const refreshTokenCamel = obj.refreshToken
      const accessToken = obj.access_token
      const accessTokenCamel = obj.accessToken
      const normalizedRefreshToken = typeof refreshToken === 'string' && refreshToken.trim()
        ? refreshToken.trim()
        : (typeof refreshTokenCamel === 'string' && refreshTokenCamel.trim() ? refreshTokenCamel.trim() : undefined)
      const normalizedAccessToken = typeof accessToken === 'string' && accessToken.trim()
        ? accessToken.trim()
        : (typeof accessTokenCamel === 'string' && accessTokenCamel.trim() ? accessTokenCamel.trim() : undefined)
      const normalizedHeaders = normalizeHeadersField(obj.headers)
        ?? normalizeHeadersField(obj.request_headers)
        ?? normalizeHeadersField(obj.requestHeaders)
        ?? normalizeHeadersField(obj.header_overrides)
        ?? normalizeHeadersField(obj.headerOverrides)
        ?? normalizeHeadersField(obj.extra_headers)
        ?? normalizeHeadersField(obj.extraHeaders)
      const importedAccessToken = normalizedAccessToken ?? bearerTokenFromHeaders(normalizedHeaders)
      if (normalizedRefreshToken || importedAccessToken) {
        return {
          refresh_token: normalizedRefreshToken,
          access_token: importedAccessToken,
          expires_at: normalizeExpiryField(obj.expires_at) ?? normalizeExpiryField(obj.expiresAt) ?? normalizeExpiryField(obj.expired),
          name: (typeof obj.name === 'string' ? obj.name : undefined) || (typeof obj.oauth_email === 'string' ? obj.oauth_email : undefined),
          email: normalizeStringField(obj.email) ?? normalizeStringField(obj.oauth_email),
          account_id: normalizeStringField(obj.account_id) ?? normalizeStringField(obj.accountId) ?? normalizeStringField(obj.chatgpt_account_id) ?? normalizeStringField(obj.chatgptAccountId),
          account_user_id: normalizeStringField(obj.account_user_id) ?? normalizeStringField(obj.accountUserId) ?? normalizeStringField(obj.chatgpt_account_user_id) ?? normalizeStringField(obj.chatgptAccountUserId),
          plan_type: normalizeStringField(obj.plan_type) ?? normalizeStringField(obj.planType) ?? normalizeStringField(obj.chatgpt_plan_type) ?? normalizeStringField(obj.chatgptPlanType),
          user_id: normalizeStringField(obj.user_id) ?? normalizeStringField(obj.userId) ?? normalizeStringField(obj.chatgpt_user_id) ?? normalizeStringField(obj.chatgptUserId),
          account_name: normalizeStringField(obj.account_name) ?? normalizeStringField(obj.accountName),
          headers: normalizedHeaders,
        }
      }
      return null
    }
  } catch {
    // Not JSON: treat as raw token.
  }

  if (isLikelyJwtToken(trimmed)) {
    return { access_token: trimmed }
  }

  return { refresh_token: trimmed }
}

export function normalizeStringField(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined
}

export function normalizeHeadersField(value: unknown): Record<string, string> | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined
  const headers: Record<string, string> = {}
  for (const [rawKey, rawValue] of Object.entries(value as Record<string, unknown>)) {
    const key = rawKey.trim().toLowerCase()
    if (!key || ['host', 'content-length', 'connection', 'transfer-encoding', 'proxy-authorization'].includes(key)) {
      continue
    }
    let headerValue: string | undefined
    if (typeof rawValue === 'string') {
      headerValue = rawValue.trim()
    } else if (typeof rawValue === 'number' || typeof rawValue === 'boolean') {
      headerValue = String(rawValue)
    }
    if (headerValue) {
      headers[key] = headerValue
    }
  }
  return Object.keys(headers).length > 0 ? headers : undefined
}

export function bearerTokenFromHeaders(headers: Record<string, string> | undefined): string | undefined {
  const authorization = headers?.authorization?.trim()
  if (!authorization) return undefined
  const match = authorization.match(/^bearer\s+(.+)$/i)
  return match?.[1]?.trim() || undefined
}

export function normalizeNumberField(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return Math.floor(value)
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value.trim())
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.floor(parsed)
    }
  }
  return undefined
}

export function normalizeExpiryField(value: unknown): number | undefined {
  const numeric = normalizeNumberField(value)
  if (numeric) return numeric
  if (typeof value === 'string' && value.trim()) {
    const parsed = Date.parse(value.trim())
    if (Number.isFinite(parsed) && parsed > 0) {
      return Math.floor(parsed / 1000)
    }
  }
  return undefined
}

export function normalizeClaudeCredentialsExpiry(value: unknown): number | undefined {
  const expiresAt = normalizeExpiryField(value)
  if (!expiresAt) return undefined
  return expiresAt >= 10_000_000_000 ? Math.floor(expiresAt / 1000) : expiresAt
}

export function isLikelyJwtToken(token: string): boolean {
  const parts = token.trim().split('.')
  if (parts.length !== 3 || parts.some(part => !part)) return false

  try {
    const header = JSON.parse(decodeBase64Url(parts[0])) as Record<string, unknown>
    const payload = JSON.parse(decodeBase64Url(parts[1])) as Record<string, unknown>
    const tokenType = typeof header.typ === 'string' ? header.typ.toLowerCase() : ''
    if (tokenType && tokenType !== 'jwt' && tokenType !== 'at+jwt') return false
    return ['exp', 'aud', 'iss', 'scope', 'scp'].some(key => key in payload)
  } catch {
    return false
  }
}

export function decodeBase64Url(value: string): string {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/')
  const padded = normalized.padEnd(normalized.length + ((4 - (normalized.length % 4)) % 4), '=')
  return atob(padded)
}

export function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export type AgentIdentityAccessTokenResolution =
  | { ok: true, accessToken: string }
  | { ok: false, message: string }

export function resolveAgentIdentityAccessToken(input: string): AgentIdentityAccessTokenResolution {
  const normalized = input.trim()
  if (!normalized.startsWith('{') && !normalized.startsWith('[')) {
    return { ok: true, accessToken: normalized }
  }

  let parsed: unknown
  try {
    parsed = JSON.parse(normalized)
  } catch {
    return { ok: false, message: t('ui.invalidChatgptAuthSessionJson') }
  }
  if (!isObjectRecord(parsed)) {
    return { ok: false, message: t('ui.chatgptAuthSessionJsonIsMissingAccesstoken') }
  }

  const accessToken = normalizeStringField(parsed.accessToken)
    ?? normalizeStringField(parsed.access_token)
  return accessToken
    ? { ok: true, accessToken }
    : { ok: false, message: t('ui.chatgptAuthSessionJsonIsMissingAccesstoken') }
}

export function isCodexAgentIdentityObject(root: Record<string, unknown>): boolean {
  const nestedValue = root.agent_identity ?? root.agentIdentity
  const nested = isObjectRecord(nestedValue) ? nestedValue : null
  const authMode = normalizeStringField(root.auth_mode)
    ?? normalizeStringField(root.authMode)
    ?? (nested
      ? normalizeStringField(nested.auth_mode) ?? normalizeStringField(nested.authMode)
      : undefined)
  if (authMode?.toLowerCase() === 'agentidentity') return true

  return nested !== null
    && Boolean(
      normalizeStringField(nested.agent_runtime_id) ?? normalizeStringField(nested.agentRuntimeId),
    )
    && Boolean(
      normalizeStringField(nested.agent_private_key) ?? normalizeStringField(nested.agentPrivateKey),
    )
}

export function requiresCodexBatchImport(credentials: string, isCodexProvider: boolean): boolean {
  if (!isCodexProvider) return false

  try {
    const parsed: unknown = JSON.parse(credentials)
    if (!isObjectRecord(parsed)) return false

    return isCodexAgentIdentityObject(parsed)
      || normalizeStringField(parsed.type)?.toLowerCase() === 'sub2api-data'
  } catch {
    return false
  }
}

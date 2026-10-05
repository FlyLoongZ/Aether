/**
 * Sensitive header masking for the usage detail drawer.
 *
 * This is a display-only convenience: captured header values are still stored
 * and returned verbatim by the admin API, so it hides secrets from casual
 * reading rather than acting as an access boundary. Values are revealed one
 * key at a time by clicking the mask.
 */

/** Mirrors the gateway's default `sensitive_headers` system config. */
export const SENSITIVE_HEADER_NAMES: readonly string[] = [
  'authorization',
  'proxy-authorization',
  'x-api-key',
  'api-key',
  'cookie',
  'set-cookie',
]

export const MASKED_HEADER_VALUE = '****'

const SENSITIVE_HEADER_NAME_SET = new Set(SENSITIVE_HEADER_NAMES)

export function normalizeHeaderName(name: string): string {
  return name.trim().toLowerCase()
}

export function isSensitiveHeaderName(name: string): boolean {
  return SENSITIVE_HEADER_NAME_SET.has(normalizeHeaderName(name))
}

/** Lowercased names of the sensitive headers present in `headers`. */
export function sensitiveHeaderKeySet(
  headers: Record<string, unknown> | null | undefined,
): Set<string> {
  const keys = new Set<string>()
  if (!headers || typeof headers !== 'object') return keys
  for (const key of Object.keys(headers)) {
    if (isSensitiveHeaderName(key)) keys.add(normalizeHeaderName(key))
  }
  return keys
}

export function hasSensitiveHeader(
  headers: Record<string, unknown> | null | undefined,
): boolean {
  return sensitiveHeaderKeySet(headers).size > 0
}

/**
 * Returns a copy with the values of still-hidden sensitive headers replaced by
 * the mask. `revealedKeys` holds lowercased header names the user revealed.
 * Non-object input is returned untouched so callers can render it as before.
 */
export function maskSensitiveHeaderValues<T>(
  headers: T,
  revealedKeys: ReadonlySet<string>,
): T {
  if (!headers || typeof headers !== 'object') return headers
  const masked: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(headers as Record<string, unknown>)) {
    const hidden = isSensitiveHeaderName(key) && !revealedKeys.has(normalizeHeaderName(key))
    masked[key] = hidden ? MASKED_HEADER_VALUE : value
  }
  return masked as T
}

/** Escapes text for safe interpolation into the raw JSON preview markup. */
export function escapeHeaderPreviewHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/**
 * Renders the header map as JSON-ish text in which every hidden value is its
 * own clickable span, so the raw view reveals a header by clicking its mask
 * (the raw view is plain text and cannot host per-value components).
 */
export function maskedJsonPreviewHtml(
  headers: Record<string, unknown> | null | undefined,
  revealedKeys: ReadonlySet<string>,
): string {
  if (!headers || typeof headers !== 'object') return ''
  const lines = Object.entries(headers as Record<string, unknown>).map(([key, value]) => {
    const hidden = isSensitiveHeaderName(key) && !revealedKeys.has(normalizeHeaderName(key))
    const rendered = hidden
      ? `<span class="reveal-mask" data-mask-key="${escapeHeaderPreviewHtml(key)}">${MASKED_HEADER_VALUE}</span>`
      : escapeHeaderPreviewHtml(JSON.stringify(value))
    return `  ${escapeHeaderPreviewHtml(JSON.stringify(key))}: ${rendered}`
  })
  return lines.length === 0 ? '{}' : `{\n${lines.join(',\n')}\n}`
}

/** Lowercased names currently still hidden, for value-level mask rendering. */
export function hiddenSensitiveHeaderKeys(
  headers: Record<string, unknown> | null | undefined,
  revealedKeys: ReadonlySet<string>,
): string[] {
  const hidden: string[] = []
  for (const key of sensitiveHeaderKeySet(headers)) {
    if (!revealedKeys.has(key)) hidden.push(key)
  }
  return hidden
}

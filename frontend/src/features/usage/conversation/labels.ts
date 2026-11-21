/**
 * Display strings the conversation renderers need.
 *
 * The renderers run inside the body-document worker, which has no locale of its
 * own: `window` is undefined there, so the i18n module always resolves the
 * default locale and an English UI would still show Chinese labels. Passing the
 * labels in is also why the worker no longer imports `@/i18n` at all — that
 * import used to inline the entire dictionary (both locales, ~665 KB) into the
 * worker chunk.
 *
 * Defaults are English so a caller that forgets to pass labels degrades to a
 * readable label instead of a raw i18n key.
 */
export interface ConversationLabels {
  roleUser: string
  roleAssistant: string
  roleSystem: string
  roleTool: string
  roleModel: string
  roleInstructions: string
  badgeThinking: string
  badgeToolCall: string
  badgeToolResult: string
  badgeImage: string
  badgeFunctionCall: string
  badgeFunctionResult: string
  emptyRequestBody: string
  emptyResponseBody: string
  emptyResponseData: string
  unknownApiFormat: string
  /** Inline placeholder that stands in for an image inside a text body. */
  imageInline: string
  /** Marker appended when a preview is cut short. */
  truncatedMarker: string
  /** Alt text for an image whose bytes are too large to preview. */
  largeImageAlt: string
  /** `{count}` placeholder: collapsible title for a thinking block. */
  thinkingSummary: string
  /** Title for a thinking block whose content the provider hid. */
  thinkingHidden: string
  /** Stand-in body for a thinking block the provider encrypted. */
  thinkingEncrypted: string
  /** Longer explanation added under an encrypted thinking block. */
  thinkingEncryptedNote: string
  /** `{error}` placeholder: request body could not be parsed. */
  parseFailed: string
  /** `{error}` placeholder: CLI-format request body could not be parsed. */
  cliParseFailed: string
  /** `{error}` placeholder: response body could not be rendered. */
  renderFailed: string
  /** `{error}` placeholder: CLI-format response body could not be rendered. */
  cliRenderFailed: string
  /** `{message}` placeholder: error block rendered inline in the body. */
  inlineError: string
  /** Badge for an error block. */
  badgeError: string
}

export const DEFAULT_CONVERSATION_LABELS: ConversationLabels = {
  roleUser: 'User',
  roleAssistant: 'Assistant',
  roleSystem: 'System',
  roleTool: 'Tool',
  roleModel: 'Model',
  roleInstructions: 'Instructions',
  badgeThinking: 'Thinking',
  badgeToolCall: 'Tool call',
  badgeToolResult: 'Tool result',
  badgeImage: 'Image',
  badgeFunctionCall: 'Function call',
  badgeFunctionResult: 'Function result',
  emptyRequestBody: 'Empty request body',
  emptyResponseBody: 'Empty response body',
  emptyResponseData: 'No response data',
  unknownApiFormat: 'Unrecognized API format',
  imageInline: '[Image]',
  truncatedMarker: '…（content truncated; copy to get the full body）',
  largeImageAlt: 'This image is too large to preview; copy the body to inspect it',
  thinkingSummary: 'Thinking ({count} chars)',
  thinkingHidden: 'Thinking (hidden)',
  thinkingEncrypted: '(thinking content is encrypted by the provider)',
  thinkingEncryptedNote:
    'The thinking content is encrypted by the provider for safety and was redacted; it still has to be forwarded verbatim in later turns for signature validation.',
  parseFailed: 'Parse failed: {error}',
  cliParseFailed: 'Failed to parse CLI format: {error}',
  renderFailed: 'Render failed: {error}',
  cliRenderFailed: 'Failed to render CLI format: {error}',
  inlineError: '[Error: {message}]',
  badgeError: 'Error',
}

let current: ConversationLabels = DEFAULT_CONVERSATION_LABELS

/**
 * Installs the labels for the next render.
 *
 * The worker processes one message at a time, so a module-level value is the
 * right scope: there is no concurrent render that could observe it changing.
 */
export function setConversationLabels(next?: Partial<ConversationLabels> | null): void {
  current = next ? { ...DEFAULT_CONVERSATION_LABELS, ...next } : DEFAULT_CONVERSATION_LABELS
}

export function conversationLabels(): ConversationLabels {
  return current
}

/**
 * Fills `{name}` placeholders in a label template.
 *
 * Labels are plain strings so they can cross the worker boundary; this is the
 * only interpolation they need.
 */
export function formatLabel(
  template: string,
  params: Record<string, string | number>
): string {
  return template.replace(/\{(\w+)\}/g, (match, name: string) =>
    name in params ? String(params[name]) : match
  )
}

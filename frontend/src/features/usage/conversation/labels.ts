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

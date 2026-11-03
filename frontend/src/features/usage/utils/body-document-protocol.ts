import type { RenderResult } from '../conversation'
import type { JsonDisplayLine, JsonPageOptions } from './json-viewer'

export const MAX_BODY_BYTES = 64 * 1024 * 1024
export const MAX_ENCODED_BODY_BYTES = MAX_BODY_BYTES + 1024 * 1024
export const BODY_WORKER_TIMEOUT = 30_000
export type BodyDocumentErrorCode = 'too_large' | 'decode_failed' | 'unsupported' | 'worker_failed' | 'timeout'

export class BodyDocumentError extends Error {
  constructor(public readonly code: BodyDocumentErrorCode) {
    super(code)
    this.name = 'BodyDocumentError'
  }
}

/**
 * How the stored bytes should be read.
 *
 * `json` parses, `text` and `sse` stay strings (the conversation renderers
 * already know how to walk an SSE stream), and `binary` is shown as base64.
 */
export type BodyEncoding = 'json' | 'text' | 'sse' | 'binary'

export type BodyJsonOptions = JsonPageOptions

export interface BodyJsonPage {
  lines: JsonDisplayLine[]
  hasNext: boolean
  text?: string
  parseError?: string
}

export interface BodyConversationOptions {
  kind: 'request' | 'response'
  apiFormat?: string
  page?: number
  previewLimit?: number
}

export interface BodyConversationPage {
  result: RenderResult
  hasNext: boolean
  truncated: boolean
}

export interface BodyDocumentSummary {
  byteLength: number
}

export type BodyWorkerCommand =
  | { action: 'load', bytes: ArrayBuffer, encoding: BodyEncoding }
  | { action: 'json', options: BodyJsonOptions }
  | { action: 'conversation', options: BodyConversationOptions }
  | { action: 'copy', conversation?: BodyConversationOptions }

export type BodyWorkerResult = BodyDocumentSummary | BodyJsonPage | BodyConversationPage | string
export type BodyWorkerRequest = BodyWorkerCommand & { id: number }
export type BodyWorkerResponse = { id: number } & (
  | { ok: true, result: BodyWorkerResult }
  | { ok: false, code: BodyDocumentErrorCode }
)

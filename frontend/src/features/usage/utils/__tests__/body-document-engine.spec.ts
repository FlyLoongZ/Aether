import { describe, expect, it, vi } from 'vitest'
import { BodyDocumentEngine, decodeBody } from '../body-document-engine'
import { JSON_PAGE_SIZE, JSON_TEXT_CHUNK_SIZE } from '../json-viewer'
import type { BodyWorkerRequest } from '../body-document-protocol'

function bytes(value: string) { return new TextEncoder().encode(value).buffer }

describe('body document decoding', () => {
  it('loads and copies complete captured bodies through the worker entry point', async () => {
    const postMessage = vi.fn()
    vi.stubGlobal('postMessage', postMessage)
    vi.stubGlobal('onmessage', undefined)
    try {
      await import('../body-document.worker')
      const dispatch = globalThis.onmessage as unknown as (event: { data: BodyWorkerRequest }) => Promise<void>
      const value = { messages: [{ role: 'user', content: `${'x'.repeat(100_000)}BODY-END` }] }
      const text = JSON.stringify(value)
      await dispatch({ data: { id: 1, action: 'load', bytes: bytes(text), encoding: 'json' } })
      expect(postMessage).toHaveBeenLastCalledWith({ id: 1, ok: true, result: { byteLength: bytes(text).byteLength } })
      await dispatch({ data: { id: 2, action: 'copy' } })
      expect(postMessage).toHaveBeenLastCalledWith({ id: 2, ok: true, result: JSON.stringify(value, null, 2) })
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('decodes raw json off the UI protocol with a byte count', async () => {
    const text = JSON.stringify({ text: '你好🙂', count: 0, enabled: false })
    const decoded = await decodeBody(bytes(text), 'json')
    expect(decoded.value).toEqual(JSON.parse(text))
    expect(decoded.byteLength).toBe(bytes(text).byteLength)
  })

  it('enforces body size while streaming, including exact boundaries', async () => {
    const text = JSON.stringify('x'.repeat(100_000))
    await expect(decodeBody(bytes(text), 'json', text.length)).resolves.toHaveProperty('byteLength', text.length)
    await expect(decodeBody(bytes(text), 'json', text.length - 1)).rejects.toHaveProperty('code', 'too_large')
  })

  it('rejects invalid UTF-8 with safe codes', async () => {
    await expect(decodeBody(new Uint8Array([34, 255, 34]).buffer, 'json')).rejects.toHaveProperty('code', 'decode_failed')
  })

  it('keeps a truncated JSON capture as the text that arrived instead of failing', async () => {
    const truncated = bytes('{"messages": [{"role": "user"')
    const decoded = await decodeBody(truncated, 'json')
    expect(decoded.value).toBe('{"messages": [{"role": "user"')
    expect(decoded.byteLength).toBe(truncated.byteLength)
  })
})

describe('body encodings', () => {
  it('keeps an SSE stream as the text that arrived, for the stream renderers', async () => {
    const stream = 'event: response.created\ndata: {"type":"response.created"}\n\ndata: [DONE]\n\n'
    const decoded = await decodeBody(bytes(stream), 'sse')
    expect(decoded.value).toBe(stream)
  })

  it('keeps plain text as text', async () => {
    const decoded = await decodeBody(bytes('upstream said no'), 'text')
    expect(decoded.value).toBe('upstream said no')
  })

  it('renders binary captures as base64 rather than failing the decode', async () => {
    const decoded = await decodeBody(new Uint8Array([0xff, 0x00, 0x41]).buffer, 'binary')
    expect(decoded.value).toBe('/wBB')
  })
})

describe('worker-owned body previews', () => {
  it('sends bounded rows and strings, while copy preserves every byte of content', () => {
    const value = { messages: Array.from({ length: 6000 }, () => ({ content: 'x'.repeat(4000) })) }
    const document = new BodyDocumentEngine(value)
    const page = document.json({ expandDepth: 999 })
    expect(page.lines).toHaveLength(JSON_PAGE_SIZE)
    expect(page.hasNext).toBe(true)
    expect(page.lines.some(line => line.continuation)).toBe(true)
    expect(page.lines.every(line => (line.tokens ?? []).reduce((length, token) => length + token.text.length, 0) <= JSON_TEXT_CHUNK_SIZE)).toBe(true)
    const next = document.json({ page: 1, expandDepth: 999 })
    expect(new Set([...page.lines, ...next.lines].map(line => line.id)).size).toBe(JSON_PAGE_SIZE * 2)
    expect(JSON.parse(document.copy())).toEqual(value)
  })

  it('keeps collapsed children unread and identifiers small even with giant keys', () => {
    const hidden = new Proxy({}, { ownKeys: () => { throw new Error('must remain lazy') } })
    const document = new BodyDocumentEngine({ parent: { hidden } })
    expect(document.json({ expandDepth: 0 }).lines).toHaveLength(3)
    const key = 'k'.repeat(100_000)
    const value = 'v'.repeat(10_000)
    const giant = new BodyDocumentEngine({ [key]: value })
    const lines = giant.json().lines
    expect(lines.every(line => line.id.length < 40)).toBe(true)
    expect(lines.filter(line => line.lineNumber === 2).flatMap(line => line.tokens ?? []).map(token => token.text).join('')).toBe(`${JSON.stringify(key)}: ${JSON.stringify(value)}`)
    expect(JSON.parse(giant.copy())).toEqual({ [key]: value })
  })

  it('supports smaller scrolling batches without relaxing the worker transfer limit', () => {
    const value = Array.from({ length: 1000 }, (_value, index) => `message-${index}`)
    const document = new BodyDocumentEngine(value)
    expect(document.json({ pageSize: 50 }).lines).toHaveLength(50)
    expect(document.json({ pageSize: 50, page: 1 }).lines[0].lineNumber).toBe(51)
    expect(document.json({ pageSize: 10_000 }).lines).toHaveLength(JSON_PAGE_SIZE)
    expect(document.json({ pageSize: 50, page: 20 }).hasNext).toBe(false)
    expect(JSON.parse(document.copy())).toEqual(value)
  })

  it('streams all raw text and parse-error response chunks without shipping the full string at once', () => {
    const text = 'x'.repeat(100_000)
    for (const value of [text, { raw_response: text, metadata: { parse_error: 'invalid upstream response' } }]) {
      const document = new BodyDocumentEngine(value)
      expect(document.json().text).toHaveLength(16_000)
      expect(document.json().hasNext).toBe(true)
      const chunks = []
      for (let page = 0; page < 10; page += 1) {
        const chunk = document.json({ page })
        chunks.push(chunk.text)
        if (!chunk.hasNext) break
      }
      expect(chunks.join('')).toBe(text)
      expect(JSON.parse(document.copy())).toEqual(value)
    }
  })

  it('paginates conversation blocks and bounds long text without truncating copy', () => {
    const content = 'message content '.repeat(10_000)
    const document = new BodyDocumentEngine({ model: 'test', messages: Array.from({ length: 40 }, () => ({ role: 'user', content })) })
    const options = { kind: 'request' as const, apiFormat: 'openai:chat' }
    const preview = document.conversationPage(options)
    expect(preview.hasNext).toBe(true)
    expect(preview.truncated).toBe(true)
    expect(JSON.stringify(preview).length).toBeLessThan(70_000)
    expect(document.copy(options)).toContain(content)
    expect(document.conversationPage({ ...options, page: 1 }).result.blocks.length).toBeGreaterThan(0)
  })

  it.each([null, false, 0])('keeps the primitive %s as a valid document', value => {
    const document = new BodyDocumentEngine(value)
    expect(document.json().lines[0].value).toBe(value)
    expect(document.copy()).toBe(JSON.stringify(value))
  })
})

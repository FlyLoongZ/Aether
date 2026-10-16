import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, h, nextTick, ref, type App } from 'vue'
import { AxiosError } from 'axios'
import type { RequestBodyField, RequestDetail } from '@/api/dashboard'
import { BodyDocumentError } from '../../utils/body-document-protocol'
import RequestDetailDrawer from '../RequestDetailDrawer.vue'

const mocks = vi.hoisted(() => ({ getRequestDetail: vi.fn(), getRequestBodyPair: vi.fn(), load: vi.fn(), copyToClipboard: vi.fn() }))
vi.mock('@/composables/useClipboard', () => ({ useClipboard: () => ({ copyToClipboard: mocks.copyToClipboard }) }))
vi.mock('@/api/dashboard', async importOriginal => {
  const actual = await importOriginal<typeof import('@/api/dashboard')>()
  return { ...actual, dashboardApi: { ...actual.dashboardApi, getRequestDetail: mocks.getRequestDetail, getRequestBodyPair: mocks.getRequestBodyPair } }
})
vi.mock('../../utils/body-document', () => ({ BodyDocument: { load: mocks.load } }))
vi.mock('../HorizontalRequestTimeline.vue', () => ({ default: { render: () => null } }))
vi.mock('../RequestDetailDrawer/JsonContent.vue', async () => {
  const { defineComponent, h } = await import('vue')
  return { default: defineComponent({
    props: { data: { type: null, default: null }, bodyDocument: { type: Object, default: null } },
    setup: props => () => h('pre', { 'data-testid': 'captured-body' }, JSON.stringify(props.bodyDocument?.display ?? props.data)),
  }) }
})

const mountedApps: Array<{ app: App, root: HTMLElement }> = []
const documents: Array<{ display: unknown, byteLength: number, dispose: ReturnType<typeof vi.fn>, copy: ReturnType<typeof vi.fn> }> = []
function body(value: unknown) { return { bytes: new TextEncoder().encode(JSON.stringify(value)).buffer } }
// The gateway stores each direction as one container holding both halves, so a
// real pair response carries the sibling the drawer did not ask for.
function pairFor(field: RequestBodyField, text: string = field) {
  const fields: RequestBodyField[] = field === 'request_body' || field === 'provider_request_body'
    ? ['request_body', 'provider_request_body']
    : ['response_body', 'client_response_body']
  const bodies: Partial<Record<RequestBodyField, ArrayBuffer>> = {}
  for (const half of fields) bodies[half] = body({ field: half, text }).bytes
  return { encoding: '1', bodies }
}

beforeEach(() => {
  mocks.getRequestDetail.mockImplementation(async id => ({ ...buildDetail(true), id, request_id: `req-${id}` }))
  mocks.getRequestBodyPair.mockImplementation(async (_id, field: RequestBodyField) => pairFor(field))
  mocks.copyToClipboard.mockResolvedValue(true)
  mocks.load.mockImplementation(async (bytes: ArrayBuffer, signal: AbortSignal) => {
    if (signal.aborted) throw new DOMException('Aborted', 'AbortError')
    const display = JSON.parse(new TextDecoder().decode(bytes))
    const document = { display, byteLength: bytes.byteLength, dispose: vi.fn(), copy: vi.fn(async () => JSON.stringify(display, null, 2)) }
    documents.push(document)
    return document
  })
})
afterEach(async () => {
  for (const { app, root } of mountedApps.splice(0)) { app.unmount(); root.remove() }
  await nextTick()
  document.body.replaceChildren()
  vi.useRealTimers()
  vi.resetAllMocks()
  documents.length = 0
})

function buildDetail(captured: boolean): RequestDetail {
  return {
    id: 'usage-full-capture', request_id: 'req-full-capture',
    user: { id: 'user-1', username: 'test-user', email: 'test@example.com' },
    api_key: { id: 'key-1', name: 'test-key', display: 'test-key' },
    provider: 'test-provider', api_format: 'openai:chat', model: 'test-model',
    tokens: { input: 10, output: 20, total: 30 }, cost: { input: 0, output: 0, total: 0 },
    request_type: 'chat', is_stream: false, status: 'completed', status_code: 200,
    response_time_ms: 10, created_at: '2026-09-07T00:00:00Z',
    request_headers: { 'content-type': 'application/json', authorization: '[redacted]' },
    has_request_body: captured, has_provider_request_body: false,
    has_response_body: captured, has_client_response_body: false,
  }
}

async function openDrawer() {
  const isOpen = ref(false)
  const requestId = ref('usage-full-capture')
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(defineComponent({ setup: () => () => h(RequestDetailDrawer, { isOpen: isOpen.value, requestId: requestId.value }) }))
  app.mount(root)
  mountedApps.push({ app, root })
  isOpen.value = true
  await nextTick()
  await vi.waitFor(() => expect(findButton('请求头')).toBeDefined())
  return { isOpen, requestId }
}
function findButton(label: string) {
  return [...document.body.querySelectorAll('button')].find(button => button.textContent?.trim() === label)
}
async function source(label: '客户端' | '提供商') {
  await nextTick()
  document.body.querySelector<HTMLButtonElement>(`button[title="${label}"]`)!.click()
  await nextTick()
}
async function expectBody(text: string) {
  await vi.waitFor(() => expect(document.body.querySelector('[data-testid="captured-body"]')?.textContent).toContain(text))
}
function lastSignal() { return mocks.getRequestBodyPair.mock.calls[mocks.getRequestBodyPair.mock.calls.length - 1][2] as AbortSignal }
function pairOf(field: RequestBodyField): 'request' | 'response' {
  return field === 'request_body' || field === 'provider_request_body' ? 'request' : 'response'
}

describe('RequestDetailDrawer body capture', () => {
  it('displays original values for all four captured header directions and copies the selected headers', async () => {
    const detail = {
      ...buildDetail(false),
      request_headers: {
        authorization: 'Bearer original-client-token',
        originator: 'codex-cli',
        'session-id': 'original-session',
        'x-codex-turn-metadata': '{"turn_id":"original-turn"}',
      },
      provider_request_headers: { authorization: 'Bearer original-provider-token', 'x-api-key': 'original-provider-key' },
      response_headers: { 'set-cookie': 'session=original-upstream', 'x-upstream-custom': 'original-upstream-value' },
      client_response_headers: { 'set-cookie': 'session=original-client', 'x-client-custom': 'original-client-value' },
    }
    mocks.getRequestDetail.mockResolvedValue(detail)
    await openDrawer()

    for (const [tab, dataSource, expected] of [
      ['请求头', '客户端', detail.request_headers],
      ['请求头', '提供商', detail.provider_request_headers],
      ['响应头', '提供商', detail.response_headers],
      ['响应头', '客户端', detail.client_response_headers],
    ] as const) {
      findButton(tab)!.click()
      await source(dataSource)
      await vi.waitFor(() => {
        const text = document.body.querySelector('[data-testid="captured-body"]')?.textContent
        expect(JSON.parse(text ?? 'null')).toEqual(expected)
      })
    }
    document.body.querySelector<HTMLButtonElement>('button[title="复制"]')!.click()
    await vi.waitFor(() => expect(mocks.copyToClipboard).toHaveBeenCalledWith(JSON.stringify(detail.client_response_headers, null, 2), false))
    expect(mocks.getRequestBodyPair).not.toHaveBeenCalled()
  })

  it('uses only shallow details and loads a single binary body on demand', async () => {
    await openDrawer()
    expect(mocks.getRequestBodyPair).not.toHaveBeenCalled()
    expect(mocks.getRequestDetail).toHaveBeenCalledWith('usage-full-capture', expect.objectContaining({ includeBodies: false }))
    findButton('请求体')!.click()
    await expectBody('request_body')
    expect(mocks.getRequestBodyPair).toHaveBeenLastCalledWith('usage-full-capture', 'request_body', expect.any(AbortSignal))
    findButton('响应体')!.click()
    await expectBody('response_body')
    findButton('请求体')!.click()
    await expectBody('request_body')
    expect(mocks.getRequestDetail).toHaveBeenCalledTimes(1)
    expect(mocks.getRequestBodyPair).toHaveBeenCalledTimes(2)
  })

  it('does not offer body tabs or fetch uncaptured basic records', async () => {
    mocks.getRequestDetail.mockResolvedValue(buildDetail(false))
    await openDrawer()
    expect(findButton('请求体')).toBeUndefined()
    expect(findButton('响应体')).toBeUndefined()
    expect(mocks.getRequestBodyPair).not.toHaveBeenCalled()
  })

  it('reuses each direction from one download and never re-fetches a cached pair', async () => {
    mocks.getRequestDetail.mockResolvedValue({ ...buildDetail(true), has_provider_request_body: true, has_client_response_body: true })
    await openDrawer()
    findButton('请求体')!.click()
    await source('客户端')
    await expectBody('request_body')
    await source('提供商')
    await expectBody('provider_request_body')
    const requestDocument = documents.find(document => (document.display as { field: string }).field === 'request_body')!
    findButton('响应体')!.click()
    await source('提供商')
    await expectBody('response_body')
    // The request pair is still cached, so the response download does not evict it.
    expect(requestDocument.dispose).not.toHaveBeenCalled()
    await source('客户端')
    await expectBody('client_response_body')
    findButton('请求体')!.click()
    await source('客户端')
    await expectBody('request_body')
    // One download per direction, and the sibling halves decode locally.
    const fetched = mocks.getRequestBodyPair.mock.calls.map(([, field]) => field as RequestBodyField)
    expect(fetched).toHaveLength(2)
    expect(new Set(fetched.map(pairOf))).toEqual(new Set(['request', 'response']))
  })

  it('cancels stale tab requests without caching their late responses', async () => {
    let resolveBody!: (value: unknown) => void
    mocks.getRequestBodyPair.mockImplementationOnce(() => new Promise(resolve => { resolveBody = resolve }))
    await openDrawer()
    findButton('请求体')!.click()
    await vi.waitFor(() => expect(mocks.getRequestBodyPair).toHaveBeenCalledOnce())
    const signal = lastSignal()
    findButton('响应体')!.click()
    await expectBody('response_body')
    expect(signal.aborted).toBe(true)
    resolveBody(body({ text: 'stale request' }))
    await nextTick()
    findButton('请求体')!.click()
    await expectBody('request_body')
    expect(document.body.textContent).not.toContain('stale request')
    expect(mocks.getRequestBodyPair).toHaveBeenCalledTimes(3)
  })

  it.each(['close', 'record'] as const)('cancels loading on %s and releases completed workers', async action => {
    const host = await openDrawer()
    findButton('请求体')!.click()
    await expectBody('request_body')
    mocks.getRequestBodyPair.mockImplementationOnce(() => new Promise(() => undefined))
    findButton('响应体')!.click()
    await vi.waitFor(() => expect(mocks.getRequestBodyPair).toHaveBeenCalledTimes(2))
    const signal = lastSignal()
    if (action === 'close') host.isOpen.value = false
    else host.requestId.value = 'usage-other'
    await vi.waitFor(() => expect(signal.aborted).toBe(true))
    await vi.waitFor(() => expect(documents[0].dispose).toHaveBeenCalledOnce())
  })

  it('shows network timeout details and retries only the active body', async () => {
    mocks.getRequestBodyPair.mockRejectedValueOnce(new AxiosError('timeout', 'ECONNABORTED'))
    await openDrawer()
    findButton('请求体')!.click()
    await vi.waitFor(() => expect(document.body.textContent).toContain('正文加载超时'))
    findButton('重试')!.click()
    await expectBody('request_body')
    expect(mocks.getRequestBodyPair.mock.calls.every(([, field]) => field === 'request_body')).toBe(true)
  })

  it('reopens on a lightweight tab without starting an obsolete body request', async () => {
    const host = await openDrawer()
    findButton('请求体')!.click()
    await expectBody('request_body')
    host.isOpen.value = false
    await nextTick()
    host.isOpen.value = true
    await vi.waitFor(() => expect(mocks.getRequestDetail).toHaveBeenCalledTimes(2))
    expect(mocks.getRequestBodyPair).toHaveBeenCalledOnce()
    expect(documents[0].dispose).toHaveBeenCalledOnce()
  })

  it.each(['too_large', 'decode_failed'] as const)('isolates non-retryable %s worker failures from other bodies', async code => {
    mocks.load.mockRejectedValueOnce(new BodyDocumentError(code))
    await openDrawer()
    findButton('请求体')!.click()
    await vi.waitFor(() => expect(document.body.textContent).toContain(code === 'too_large' ? '64 MiB' : '解析失败'))
    expect(findButton('重试')).toBeUndefined()
    findButton('响应体')!.click()
    await expectBody('response_body')
    findButton('请求体')!.click()
    await vi.waitFor(() => expect(document.body.textContent).toContain(code === 'too_large' ? '64 MiB' : '解析失败'))
    expect(mocks.getRequestBodyPair).toHaveBeenCalledTimes(2)
  })

  it('reports missing storage from binary response headers without parsing an error body', async () => {
    mocks.getRequestBodyPair.mockRejectedValueOnce(new AxiosError('missing', 'ERR_BAD_REQUEST', undefined, undefined, {
      status: 404, statusText: 'Not Found', headers: { 'x-aether-body-error': 'missing' }, data: new ArrayBuffer(0), config: { headers: {} } as never,
    }))
    await openDrawer()
    findButton('请求体')!.click()
    await vi.waitFor(() => expect(document.body.textContent).toContain('正文暂不可用'))
    expect(findButton('重试')).toBeDefined()
    expect(mocks.load).not.toHaveBeenCalled()
  })

  it('does not pass invalid binary responses to the worker', async () => {
    mocks.getRequestBodyPair.mockRejectedValueOnce(new Error('Invalid body response'))
    await openDrawer()
    findButton('请求体')!.click()
    await vi.waitFor(() => expect(document.body.textContent).toContain('正文内容加载失败'))
    expect(mocks.load).not.toHaveBeenCalled()
  })

  it('invalidates the worker cache when a streaming request finishes', async () => {
    vi.useFakeTimers()
    let finished = false
    mocks.getRequestDetail.mockImplementation(async () => ({ ...buildDetail(true), status: finished ? 'completed' : 'streaming' }))
    mocks.getRequestBodyPair.mockImplementation(async (_id, field: RequestBodyField) => pairFor(field, finished ? 'final response' : 'partial response'))
    await openDrawer()
    findButton('响应体')!.click()
    await expectBody('partial response')
    finished = true
    await vi.advanceTimersByTimeAsync(1000)
    await expectBody('final response')
    expect(documents[0].dispose).toHaveBeenCalledOnce()
    expect(mocks.getRequestBodyPair).toHaveBeenCalledTimes(2)
  })

  it('enforces the decoded-byte cache budget, not just a pair count', async () => {
    await openDrawer()
    findButton('请求体')!.click()
    await expectBody('request_body')
    documents[0].byteLength = 64 * 1024 * 1024
    findButton('响应体')!.click()
    await expectBody('response_body')
    expect(documents[0].dispose).toHaveBeenCalledOnce()
  })

  it('requests full copy from the worker only after an explicit click', async () => {
    const value = { text: 'x'.repeat(100_000), last: 'complete content' }
    mocks.getRequestBodyPair.mockResolvedValue({ encoding: '1', bodies: { request_body: body(value).bytes } })
    await openDrawer()
    findButton('请求体')!.click()
    await expectBody('complete content')
    expect(documents[0].copy).not.toHaveBeenCalled()
    document.body.querySelector<HTMLButtonElement>('button[title="复制"]')!.click()
    await vi.waitFor(() => expect(mocks.copyToClipboard).toHaveBeenCalledWith(JSON.stringify(value, null, 2), false))
    expect(documents[0].copy).toHaveBeenCalledOnce()
  })
})

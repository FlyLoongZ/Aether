import { beforeEach, describe, expect, it, vi } from 'vitest'

const { getMock } = vi.hoisted(() => ({ getMock: vi.fn() }))
vi.mock('@/api/client', () => ({ default: { get: getMock } }))

import { dashboardApi } from '@/api/dashboard'
import { requestTraceApi } from '@/api/requestTrace'
import { cache } from '@/utils/cache'

beforeEach(() => {
  cache.clear()
  getMock.mockReset()
  getMock.mockResolvedValue({ data: { id: 'usage-1' } })
})

describe('dashboard body loading', () => {
  it('reports download progress so diagnostic exports can abort oversized bodies', async () => {
    const onProgress = vi.fn()
    getMock.mockResolvedValue({ data: new ArrayBuffer(0), headers: { 'x-aether-body-encoding': 'json', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'response_body' } })
    await dashboardApi.getRequestBody('usage-1', 'response_body', undefined, onProgress)
    getMock.mock.calls[0][1].onDownloadProgress({ loaded: 2 * 1024 * 1024 })
    expect(onProgress).toHaveBeenCalledWith(2 * 1024 * 1024)
  })

  it('records the gateway version at export without confusing it with failure-time metadata', async () => {
    getMock.mockResolvedValue({ data: { request_id: 'request-1', candidates: [] }, headers: { 'x-aether-build-version': 'test-build' } })
    expect(await requestTraceApi.getRequestTrace('request-1')).toMatchObject({ gateway_version: 'test-build' })
    getMock.mockResolvedValue({ data: { request_id: 'request-1', candidates: [] } })
    expect(await requestTraceApi.getRequestTrace('request-1')).toMatchObject({ gateway_version: null })
  })
  it('requests opaque body bytes, outside the JSON detail cache', async () => {
    const bytes = new ArrayBuffer(20)
    const controller = new AbortController()
    getMock.mockResolvedValue({ data: bytes, headers: { 'x-aether-body-encoding': 'json', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'request_body' } })
    await expect(dashboardApi.getRequestBody('usage-1', 'request_body', controller.signal)).resolves.toEqual({ bytes })
    expect(getMock).toHaveBeenCalledWith('/api/admin/usage/usage-1', { params: { include_bodies: true, body_field: 'request_body', body_format: 'raw' }, responseType: 'arraybuffer', signal: controller.signal })
  })

  it.each([
    { 'x-aether-body-encoding': 'br', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'request_body' },
    { 'x-aether-body-encoding': 'gzip', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'request_body' },
    { 'x-aether-body-encoding': 'json', 'x-aether-usage-id': 'usage-other', 'x-aether-body-field': 'request_body' },
    { 'x-aether-body-encoding': 'json', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'response_body' },
    {},
  ])('rejects mismatched or invalid body response headers', async headers => {
    getMock.mockResolvedValue({ data: new ArrayBuffer(10), headers })
    await expect(dashboardApi.getRequestBody('usage-1', 'request_body')).rejects.toThrow('Invalid body response')
  })

  it.each([
    ['1', 'request_body', 'request'],
    ['1', 'provider_request_body', 'provider_request'],
    ['2', 'response_body', 'response'],
    ['2', 'client_response_body', 'client_response'],
  ] as const)('picks this field out of a %s container', async (encoding, field, key) => {
    const container = {
      request: { model: 'gpt-5' },
      provider_request: { model: 'gpt-5.4' },
      response: { id: 'chatcmpl-1' },
      client_response: { id: 'chatcmpl-2' },
    }
    getMock.mockResolvedValue({ data: new TextEncoder().encode(JSON.stringify(container)).buffer as ArrayBuffer, headers: { 'x-aether-body-encoding': encoding, 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': field } })
    const { bytes } = await dashboardApi.getRequestBody('usage-1', field)
    expect(JSON.parse(new TextDecoder().decode(bytes))).toEqual(container[key])
  })

  it('returns both halves of a pair container from one download', async () => {
    const container = { response: { id: 'chatcmpl-1' }, client_response: { id: 'chatcmpl-2' } }
    getMock.mockResolvedValue({ data: new TextEncoder().encode(JSON.stringify(container)).buffer as ArrayBuffer, headers: { 'x-aether-body-encoding': '1', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'response_body' } })
    const pair = await dashboardApi.getRequestBodyPair('usage-1', 'response_body')
    expect(pair.encoding).toBe('1')
    expect(Object.keys(pair.bodies).sort()).toEqual(['client_response_body', 'response_body'])
    expect(JSON.parse(new TextDecoder().decode(pair.bodies.response_body!))).toEqual(container.response)
    expect(JSON.parse(new TextDecoder().decode(pair.bodies.client_response_body!))).toEqual(container.client_response)
  })

  it('keeps a single body to the requested field only', async () => {
    const bytes = new ArrayBuffer(20)
    getMock.mockResolvedValue({ data: bytes, headers: { 'x-aether-body-encoding': 'json', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'request_body' } })
    const pair = await dashboardApi.getRequestBodyPair('usage-1', 'request_body')
    expect(pair.encoding).toBe('json')
    expect(Object.keys(pair.bodies)).toEqual(['request_body'])
    expect(pair.bodies.request_body).toBe(bytes)
  })

  it('treats the encoding column value 0 as a single body', async () => {
    const bytes = new ArrayBuffer(20)
    getMock.mockResolvedValue({ data: bytes, headers: { 'x-aether-body-encoding': '0', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'request_body' } })
    await expect(dashboardApi.getRequestBody('usage-1', 'request_body')).resolves.toEqual({ bytes })
  })

  it('rejects a container without the requested field or without valid json', async () => {
    const headers = { 'x-aether-body-encoding': '1', 'x-aether-usage-id': 'usage-1', 'x-aether-body-field': 'provider_request_body' }
    getMock.mockResolvedValue({ data: new TextEncoder().encode(JSON.stringify({ request: { model: 'gpt-5' }, provider_request: null })).buffer as ArrayBuffer, headers })
    await expect(dashboardApi.getRequestBody('usage-1', 'provider_request_body')).rejects.toThrow('Invalid body response')
    getMock.mockResolvedValue({ data: new TextEncoder().encode('not json').buffer as ArrayBuffer, headers })
    await expect(dashboardApi.getRequestBody('usage-1', 'provider_request_body')).rejects.toThrow('Invalid body response')
  })

  it('defaults to shallow details and isolates cached records', async () => {
    await dashboardApi.getRequestDetail('usage-1')
    expect(getMock).toHaveBeenLastCalledWith('/api/admin/usage/usage-1', { params: { include_bodies: false } })
    await dashboardApi.getRequestDetail('usage-1', { cacheTtlMs: 5000 })
    await dashboardApi.getRequestDetail('usage-2', { cacheTtlMs: 5000 })
    await dashboardApi.getRequestDetail('usage-1', { cacheTtlMs: 5000 })
    expect(getMock).toHaveBeenCalledTimes(3)
    expect(getMock).toHaveBeenLastCalledWith('/api/admin/usage/usage-2', { params: { include_bodies: false } })
  })

  it('does not share abortable requests with another caller or a cancelled request', async () => {
    const first = new AbortController()
    const second = new AbortController()
    const firstRequest = dashboardApi.getRequestDetail('usage-1', { signal: first.signal })
    first.abort()
    const secondRequest = dashboardApi.getRequestDetail('usage-1', { signal: second.signal })
    await Promise.all([firstRequest, secondRequest])
    expect(getMock).toHaveBeenCalledTimes(2)
    expect(getMock).toHaveBeenLastCalledWith('/api/admin/usage/usage-1', {
      params: { include_bodies: false }, signal: second.signal,
    })
  })
})

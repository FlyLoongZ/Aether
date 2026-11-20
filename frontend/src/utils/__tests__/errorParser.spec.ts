import { AxiosError, AxiosHeaders } from 'axios'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { setI18nLocale } from '@/i18n'
import { parseApiError, parseUpstreamModelError } from '@/utils/errorParser'

beforeEach(async () => { await setI18nLocale('zh-CN') })
afterEach(() => setI18nLocale('zh-CN'))

describe('errorParser', () => {
  it('normalizes reused refresh token errors from legacy string details', async () => {
    const error = {
      response: {
        data: {
          detail: "token refresh 失败: {'message': 'Your refresh token has already been used to generate a new access token. Please try signing in again.', 'type': 'invalid_request_error', 'param': None, 'code': 'refresh_token_reused'}",
        },
      },
    }

    expect(parseApiError(error, 'Token 刷新失败')).toBe(
      'Token 刷新失败：refresh_token 已被使用并轮换，请重新登录授权',
    )
  })

  it('keeps normalized Chinese refresh failures intact', async () => {
    const error = {
      response: {
        data: {
          detail: 'Token 刷新失败：refresh_token 已被使用并轮换，请重新登录授权',
        },
      },
    }

    expect(parseApiError(error, 'Token 刷新失败')).toBe(
      'Token 刷新失败：refresh_token 已被使用并轮换，请重新登录授权',
    )
  })

  it('normalizes expired refresh token errors', async () => {
    const error = {
      response: {
        data: {
          detail: '{"error":{"message":"Could not validate your refresh token. Please try signing in again.","type":"invalid_request_error","code":"refresh_token_expired"}}',
        },
      },
    }

    expect(parseApiError(error, 'Token 刷新失败')).toBe(
      'Token 刷新失败：refresh_token 无效、已过期或已撤销，请重新登录授权',
    )
  })

  it.each(['保存', '删除', '12 分钟', '  保存\n删除  ', ' \n '])('preserves raw API message %j', async (message) => {
    await setI18nLocale('en-US')
    for (const data of [
      { detail: message },
      { error: { message } },
      { message },
      { detail: { message } },
    ]) {
      expect(parseApiError({ response: { data } })).toBe(message)
    }
    expect(parseApiError(new Error(message))).toBe(message)
  })

  it('translates local fallback messages without rewriting caller fallbacks', async () => {
    await setI18nLocale('en-US')
    expect(parseApiError(undefined)).toBe('Operation failed')
    expect(parseApiError(undefined, '保存')).toBe('保存')
    expect(parseApiError({})).toBe('Cannot connect to the server. Check your network connection.')
  })

  it('formats known validation errors in English and preserves unknown error content', async () => {
    await setI18nLocale('en-US')
    expect(parseApiError({ response: { data: { detail: [
      { loc: ['body', 'username'], type: 'string_too_short', msg: '保存', ctx: { min_length: 5 } },
      { loc: ['body', 'custom', '保存'], type: 'value_error', msg: '  删除  ' },
    ] } } })).toBe('Username: Must contain at least 5 characters\ncustom.保存:   删除  ')
  })

  it('translates recognized token failures and preserves their unknown reason', async () => {
    await setI18nLocale('en-US')
    expect(parseApiError(new Error('refresh_token_reused')))
      .toBe('Token refresh failed: refresh_token has already been used and rotated. Sign in again.')
    expect(parseApiError(new Error('Token refresh failed: 保存'))).toBe('Token refresh failed: 保存')
  })

  it('maps Axios network-layer failures to localized copy instead of English fallbacks', async () => {
    setI18nLocale('zh-CN')
    expect(parseApiError(new AxiosError('Network Error', 'ERR_NETWORK')))
      .toBe('无法连接到服务器，请检查网络连接')
    expect(parseApiError(new AxiosError('timeout of 30000ms exceeded', 'ECONNABORTED')))
      .toBe('请求超时，请稍后重试')
    expect(parseApiError(new AxiosError('timeout of 30000ms exceeded', 'ETIMEDOUT')))
      .toBe('请求超时，请稍后重试')
    await setI18nLocale('en-US')
    expect(parseApiError(new AxiosError('Network Error', 'ERR_NETWORK')))
      .toBe('Cannot connect to the server. Check your network connection.')
    expect(parseApiError(new AxiosError('timeout of 30000ms exceeded', 'ECONNABORTED')))
      .toBe('Request timed out. Try again later.')
  })

  it('falls back to the caller message when a response carries no usable body', async () => {
    setI18nLocale('zh-CN')
    const config = { headers: new AxiosHeaders() }
    const gatewayError = new AxiosError('Request failed with status code 502', 'ERR_BAD_RESPONSE', config, undefined, {
      status: 502,
      statusText: 'Bad Gateway',
      data: '<html>Bad Gateway</html>',
      headers: new AxiosHeaders(),
      config,
    })
    expect(parseApiError(gatewayError, '保存失败')).toBe('保存失败')
    // 非 Axios 的错误对象仍保留自己的 message，避免把调用方自定义错误吞掉
    expect(parseApiError({ response: { data: {} }, message: '自定义错误' }, '保存失败')).toBe('自定义错误')
  })

  it('translates upstream status descriptions while preserving unknown API content', async () => {
    await setI18nLocale('en-US')
    expect(parseUpstreamModelError('HTTP 401: {"error":{"message":"保存"}}')).toBe('The key is invalid or expired: 保存')
    expect(parseUpstreamModelError('HTTP 400: {"message":"删除"}')).toBe('删除')
    expect(parseUpstreamModelError('12 分钟')).toBe('12 分钟')
    expect(parseUpstreamModelError('Request error: timeout')).toBe('Request timed out. The upstream service responded too slowly.')
  })
})

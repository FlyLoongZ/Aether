import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { submitPaymentInstructions, type PaymentSubmissionHandlers } from '../paymentSubmission'

function createHandlers() {
  const onStripeInstructions = vi.fn()
  const onUnsafeUrl = vi.fn()
  const handlers: PaymentSubmissionHandlers = { onStripeInstructions, onUnsafeUrl }
  return { handlers, onStripeInstructions, onUnsafeUrl }
}

const stripeInstructions = {
  gateway: 'stripe',
  display_name: 'Stripe',
  gateway_order_id: 'order_1',
  intent_id: 'pi_1',
  client_secret: 'pi_1_secret',
  publishable_key: 'pk_test',
  expires_at: '2026-01-01T00:00:00Z',
  pay_amount: 10,
  pay_currency: 'usd',
  payment_channel: 'card',
  payment_method_types: ['card'],
  submit_method: 'confirmation_token',
}

let openSpy: ReturnType<typeof vi.spyOn>

beforeEach(() => {
  openSpy = vi.spyOn(window, 'open').mockReturnValue({} as Window)
  vi.spyOn(HTMLFormElement.prototype, 'submit').mockImplementation(() => {})
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe('submitPaymentInstructions', () => {
  it('会让调用方接管 Stripe 指令', () => {
    const { handlers, onStripeInstructions } = createHandlers()
    submitPaymentInstructions(stripeInstructions, handlers)

    expect(onStripeInstructions).toHaveBeenCalledWith(stripeInstructions)
    expect(openSpy).not.toHaveBeenCalled()
  })

  it('没有支付地址时什么也不做', () => {
    const { handlers, onStripeInstructions, onUnsafeUrl } = createHandlers()
    submitPaymentInstructions({ payment_method: 'alipay' }, handlers)

    expect(onStripeInstructions).not.toHaveBeenCalled()
    expect(onUnsafeUrl).not.toHaveBeenCalled()
    expect(openSpy).not.toHaveBeenCalled()
  })

  it('拒绝非 HTTPS 的支付地址', () => {
    const { handlers, onUnsafeUrl } = createHandlers()
    submitPaymentInstructions({ payment_url: 'http://pay.example/checkout' }, handlers)

    expect(onUnsafeUrl).toHaveBeenCalledTimes(1)
    expect(openSpy).not.toHaveBeenCalled()
  })

  it('新窗口打开可信地址', () => {
    const { handlers } = createHandlers()
    submitPaymentInstructions({ payment_url: 'https://pay.example/checkout?order=1' }, handlers)

    expect(openSpy).toHaveBeenCalledWith(
      'https://pay.example/checkout?order=1',
      '_blank',
      'noopener,noreferrer',
    )
  })

  it('浏览器拦截弹窗时退回当前窗口跳转', () => {
    openSpy.mockReturnValue(null)
    const { handlers } = createHandlers()
    const assign = vi.fn()
    const originalLocation = window.location
    Object.defineProperty(window, 'location', {
      configurable: true,
      value: { ...originalLocation, href: '', set: assign },
    })

    submitPaymentInstructions({ payment_url: 'https://pay.example/checkout' }, handlers)

    expect(String(window.location.href)).toContain('https://pay.example/checkout')
    Object.defineProperty(window, 'location', { configurable: true, value: originalLocation })
  })

  it('带 payment_params 时以 POST 表单提交', () => {
    const { handlers } = createHandlers()
    const appendSpy = vi.spyOn(document.body, 'appendChild')

    submitPaymentInstructions({
      payment_url: 'https://pay.example/submit',
      payment_params: { order: '1', sign: 'abc', skip: null },
    }, handlers)

    const form = appendSpy.mock.calls.map(([node]) => node)
      .find(node => node instanceof HTMLFormElement) as HTMLFormElement | undefined
    expect(form).toBeTruthy()
    expect(form?.method).toBe('post')
    expect(form?.action).toBe('https://pay.example/submit')
    const fields = [...(form?.querySelectorAll('input') ?? [])]
      .map(input => [input.name, input.value])
    expect(fields).toEqual([['order', '1'], ['sign', 'abc']])
    expect(HTMLFormElement.prototype.submit).toHaveBeenCalled()
    expect(openSpy).not.toHaveBeenCalled()
  })
})

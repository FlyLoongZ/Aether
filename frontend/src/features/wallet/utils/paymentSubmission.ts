/**
 * 支付指令的统一提交流程。
 *
 * BillingPlans / WalletCenter 两个页面各自复制了同一份「Stripe 弹窗 / 表单 POST /
 * 新窗口跳转」分支逻辑与 Safari 判定，这里集中维护，避免支付链路修改时漏改一处。
 */
import {
  getPaymentInstructionString,
  getStripePaymentInstructions,
  type PaymentInstructionMap,
} from './paymentInstructions'
import { safePaymentTargetUrl } from './paymentUrl'

export interface PaymentSubmissionHandlers {
  /** 命中 Stripe 指令：由调用方打开自己的支付弹窗 */
  onStripeInstructions: (instructions: PaymentInstructionMap) => void
  /** 支付网关返回了非 HTTPS / 非法地址 */
  onUnsafeUrl: () => void
}

/**
 * Safari 对新窗口打开 POST 表单的处理不稳定，此时不设置 target，
 * 由当前窗口完成跳转。
 */
export function isSafariBrowser(): boolean {
  return navigator.userAgent.includes('Safari') && !navigator.userAgent.includes('Chrome')
}

function submitPaymentForm(
  url: string,
  params: Record<string, unknown>,
  handlers: PaymentSubmissionHandlers,
): void {
  const safeUrl = safePaymentTargetUrl(url)
  if (!safeUrl) {
    handlers.onUnsafeUrl()
    return
  }
  const form = document.createElement('form')
  form.action = safeUrl
  form.method = 'POST'
  if (!isSafariBrowser()) {
    form.target = '_blank'
  }
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined) return
    const input = document.createElement('input')
    input.type = 'hidden'
    input.name = key
    input.value = String(value)
    form.appendChild(input)
  })
  document.body.appendChild(form)
  form.submit()
  document.body.removeChild(form)
}

/** 按支付指令把用户带到收银台：Stripe 弹窗、POST 表单或新窗口跳转。 */
export function submitPaymentInstructions(
  instructions: PaymentInstructionMap | null | undefined,
  handlers: PaymentSubmissionHandlers,
): void {
  if (!instructions) return

  if (getStripePaymentInstructions(instructions)) {
    handlers.onStripeInstructions(instructions)
    return
  }

  const paymentUrl = getPaymentInstructionString(instructions, 'payment_url')
  if (!paymentUrl) return

  const safePaymentUrl = safePaymentTargetUrl(paymentUrl)
  if (!safePaymentUrl) {
    handlers.onUnsafeUrl()
    return
  }

  const paymentParams = instructions.payment_params
  if (paymentParams && typeof paymentParams === 'object' && !Array.isArray(paymentParams)) {
    submitPaymentForm(safePaymentUrl, paymentParams as Record<string, unknown>, handlers)
    return
  }

  const opened = window.open(safePaymentUrl, '_blank', 'noopener,noreferrer')
  if (!opened) {
    window.location.href = safePaymentUrl
  }
}

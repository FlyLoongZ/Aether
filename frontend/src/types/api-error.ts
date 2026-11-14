/**
 * API 错误类型定义
 * 用于替代各处的 any 类型，提供类型安全的错误处理
 */

import type { AxiosError } from 'axios'

/**
 * 后端标准错误响应格式
 */
export interface ApiErrorResponse {
  error?: {
    type?: string
    message?: string
    details?: Record<string, unknown>
  }
  detail?: string
  message?: string
}

/**
 * API 错误类型
 * 封装 Axios 错误，提供类型安全的错误处理
 */
export interface ApiError extends AxiosError<ApiErrorResponse> {
  response?: AxiosError<ApiErrorResponse>['response'] & {
    data?: ApiErrorResponse
  }
}

/**
 * 类型守卫：检查是否为 API 错误
 */
export function isApiError(error: unknown): error is ApiError {
  return (
    typeof error === 'object' &&
    error !== null &&
    'response' in error
  )
}

/**
 * 从错误对象中安全提取 HTTP 状态码
 */
export function getErrorStatus(error: unknown): number | undefined {
  if (isApiError(error)) {
    return error.response?.status
  }
  return undefined
}

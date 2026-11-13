

// 网络配置
export const NETWORK_CONFIG = {
  API_TIMEOUT: 30000, // 30秒
  REQUEST_RETRY_LIMIT: 3, // 最大重试次数
} as const

// 认证配置
export const AUTH_CONFIG = {
  MAX_RETRY_COUNT: 3, // token刷新最大重试次数
} as const

// Toast 配置
export const TOAST_CONFIG = {
  SUCCESS_DURATION: 3000, // 成功消息持续时间(ms)
  ERROR_DURATION: 5000, // 错误消息持续时间(ms)
  WARNING_DURATION: 5000, // 警告消息持续时间(ms)
  INFO_DURATION: 3000, // 信息消息持续时间(ms)
} as const

// 日志级别
export const LogLevel = {
  DEBUG: 'DEBUG',
  INFO: 'INFO',
  WARN: 'WARN',
  ERROR: 'ERROR',
} as const

export type LogLevel = typeof LogLevel[keyof typeof LogLevel]

// 环境判断
export const isDev = import.meta.env.DEV

// 数据缓存 TTL
export const CACHE_TTL = {
  /** 活动热力图：变化慢，允许较长缓存 */
  ACTIVITY_HEATMAP_MS: 30 * 60 * 1000,
  /** 用量分析聚合：需要与其他看板保持相对新鲜 */
  USAGE_ANALYTICS_MS: 30 * 1000,
} as const

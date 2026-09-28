// ========== 路由预览相关类型 ==========

/**
 * Key 路由信息
 */
export interface RoutingKeyInfo {
  id: string
  name: string
  masked_key: string
  internal_priority: number
  global_priority_by_format?: Record<string, number> | null  // 按 API 格式的全局优先级
  /** 应用系统默认路由策略后的有效内部优先级 */
  effective_internal_priority?: number | null
  /** 应用系统默认路由策略后的有效全局优先级（当前 endpoint 格式） */
  effective_global_priority?: number | null
  /** 优先级来源：catalog（目录原值）或 policy_override（策略覆盖） */
  priority_source?: string
  rpm_limit?: number | null
  is_adaptive: boolean
  effective_rpm?: number | null
  cache_ttl_minutes: number
  health_score: number  // 0-1 小数格式
  is_active: boolean
  api_formats: string[]
  allowed_models?: string[] | null  // 允许的模型列表，null 表示不限制
  circuit_breaker_open: boolean
  circuit_breaker_formats: string[]
  next_probe_at?: string | null  // 下次探测时间（ISO格式）
}

/**
 * Endpoint 路由信息
 */
export interface RoutingEndpointInfo {
  id: string
  api_format: string
  base_url: string
  custom_path?: string | null
  format_acceptance_config?: {
    enabled?: boolean
    accept_formats?: string[] | null
    reject_formats?: string[] | null
  } | null
  is_active: boolean
  keys: RoutingKeyInfo[]
  total_keys: number
  active_keys: number
}

/**
 * 模型名称映射信息
 */
export interface RoutingModelMapping {
  name: string
  priority: number
  api_formats?: string[] | null
}

/**
 * Provider 路由信息
 */
export interface RoutingProviderInfo {
  id: string
  name: string
  model_id: string
  provider_priority: number
  /** 应用系统默认路由策略后的有效提供商优先级 */
  effective_provider_priority?: number | null
  /** 优先级来源：catalog（目录原值）或 policy_override（策略覆盖） */
  provider_priority_source?: string
  /** 该提供商是否使用号池调度（号池候选按池优先级排名） */
  is_pool_provider?: boolean
  /** 策略中的号池优先级覆盖（如有） */
  effective_pool_priority?: number | null
  enable_format_conversion?: boolean
  keep_priority_on_conversion?: boolean
  billing_type?: string | null
  monthly_quota_usd?: number | null
  monthly_used_usd?: number | null
  is_active: boolean
  provider_model_name: string
  model_mappings: RoutingModelMapping[]
  model_is_active: boolean
  endpoints: RoutingEndpointInfo[]
  total_endpoints: number
  active_endpoints: number
}

/**
 * 全局 Key 白名单项（用于前端实时匹配）
 */
export interface GlobalKeyWhitelistItem {
  key_id: string
  key_name: string
  masked_key: string
  provider_id: string
  provider_name: string
  allowed_models: string[]
}

/**
 * 链路预览应用的有效路由策略元数据
 */
export interface RoutingEffectivePolicy {
  source: string  // system_default | system_default_unresolved | none
  group_id?: string | null
  group_name?: string | null
  requested_model?: string
  note?: string
}

/**
 * 模型请求链路预览响应
 */
export interface ModelRoutingPreviewResponse {
  global_model_id: string
  global_model_name: string
  display_name: string
  is_active: boolean
  global_model_mappings: string[]  // GlobalModel 的模型映射规则（正则模式）
  providers: RoutingProviderInfo[]
  total_providers: number
  active_providers: number
  scheduling_mode: string
  priority_mode: string
  keep_priority_on_conversion?: boolean
  effective_policy?: RoutingEffectivePolicy | null
  all_keys_whitelist: GlobalKeyWhitelistItem[]
}

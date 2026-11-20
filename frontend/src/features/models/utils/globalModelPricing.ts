import type { GlobalModelResponse } from '@/api/global-models'
import type { PricingTier, TieredPricingConfig } from '@/api/endpoints/types'

// 全局模型列表里的定价摘要展示

// 从 GlobalModel 的 default_tiered_pricing 获取第一阶梯价格
export function getFirstTierPrice(model: GlobalModelResponse, type: 'input' | 'output'): number | null {
  const tiered = model.default_tiered_pricing
  if (!tiered?.tiers?.length) return null
  const firstTier = tiered.tiers[0]
  if (type === 'input') {
    return firstTier.input_price_per_1m || null
  }
  return firstTier.output_price_per_1m || null
}

// 检测是否有阶梯计费（多于一个阶梯）
export function hasTieredPricing(model: GlobalModelResponse): boolean {
  const tiered = model.default_tiered_pricing
  return (tiered?.tiers?.length || 0) > 1
}

/** 阶梯内 1 小时缓存创建价格（未配置时返回 '-'）。 */
export function get1hCachePrice(tier: PricingTier): string {
  const ttl1h = tier.cache_ttl_pricing?.find(item => item.ttl_minutes === 60)
  return ttl1h ? `$${ttl1h.cache_creation_price_per_1m.toFixed(2)}` : '-'
}

/** 第一阶梯的 1 小时缓存创建价格。 */
export function getFirst1hCachePrice(tieredPricing: TieredPricingConfig | undefined | null): string {
  if (!tieredPricing?.tiers?.length) return '-'
  return get1hCachePrice(tieredPricing.tiers[0])
}

/**
 * 视频分辨率计价的共享定义。
 *
 * GlobalModel 表单与 Provider 模型表单此前各自复制了一份预设表与
 * 「分辨率归一化」逻辑，改动时需要同步两处，这里统一维护。
 */

export type VideoResolutionPriceRow = { resolution: string; price_per_second: number | undefined }

export type VideoResolutionPricePreset = 'common' | 'sora' | 'veo'

export const VIDEO_RESOLUTION_PRICE_PRESETS: Record<VideoResolutionPricePreset, VideoResolutionPriceRow[]> = {
  common: [
    { resolution: '480p', price_per_second: 0 },
    { resolution: '720p', price_per_second: 0 },
    { resolution: '1080p', price_per_second: 0 },
    { resolution: '4k', price_per_second: 0 },
  ],
  sora: [
    { resolution: '720x1080', price_per_second: 0 },
    { resolution: '1024x1792', price_per_second: 0 },
  ],
  veo: [
    { resolution: '720p', price_per_second: 0 },
    { resolution: '1080p', price_per_second: 0 },
    { resolution: '4k', price_per_second: 0 },
  ],
}

/**
 * 归一化分辨率键：小写、去空白、统一乘号，并把 `WxH` 的宽高按小到大排序，
 * 使 `1080x720` 与 `720x1080` 落到同一个键上。
 */
export function normalizeResolutionKey(raw: string): string {
  let key = (raw || '').trim().toLowerCase().replace(/\s+/g, '').replace(/×/g, 'x')
  const match = key.match(/^(\d+)x(\d+)$/)
  if (match) {
    const a = parseInt(match[1], 10)
    const b = parseInt(match[2], 10)
    key = a <= b ? `${a}x${b}` : `${b}x${a}`
  }
  return key
}

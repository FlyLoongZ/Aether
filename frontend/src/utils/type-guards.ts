/**
 * 通用类型守卫。
 *
 * `isRecord` 此前在 7 个模块里各写一份（判断条件略有差异但语义相同），
 * 统一到这里，避免「非 null 对象且非数组」的判定出现分叉。
 */

/** 是否为普通对象（排除 null 与数组）。 */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
}

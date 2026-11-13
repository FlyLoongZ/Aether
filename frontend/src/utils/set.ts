/**
 * 返回「切换某元素存在性」后的新集合（不修改入参）。
 *
 * 折叠状态切换在多个弹窗里各写一遍「has/delete/add + 重新赋值」，
 * 统一到这里，同时避免直接改动 Set 导致 Vue 无法感知变化。
 */
export function toggleSetMember<T>(source: Set<T>, value: T): Set<T> {
  const next = new Set(source)
  if (next.has(value)) next.delete(value)
  else next.add(value)
  return next
}

/**
 * 健康度（0-1 小数）对应的 Tailwind 颜色类。
 *
 * 模型路由页与提供商详情抽屉各自复制了一份阈值判断，这里集中维护，
 * 避免两处对"健康/一般/危险"的阈值或配色出现分叉。
 */
export function getHealthScoreBarColor(score: number): string {
  if (score >= 0.8) return 'bg-green-500 dark:bg-green-400'
  if (score >= 0.5) return 'bg-yellow-500 dark:bg-yellow-400'
  return 'bg-red-500 dark:bg-red-400'
}

export function getHealthScoreTextColor(score: number): string {
  if (score >= 0.8) return 'text-green-600 dark:text-green-400'
  if (score >= 0.5) return 'text-yellow-600 dark:text-yellow-400'
  return 'text-red-600 dark:text-red-400'
}

import { useDarkMode } from '@/composables/useDarkMode'

/**
 * 「流式 / 标准」徽章的样式变体。
 *
 * 暗色模式下交换填充与描边的强调方式；该规则原先在三个用量视图里各写一份，
 * 这里集中维护，组件只需 `const streamBadgeVariant = useStreamBadgeVariant()`。
 */
export function useStreamBadgeVariant() {
  const { isDark } = useDarkMode()

  return (isStream: boolean): 'secondary' | 'outline' => {
    if (isDark.value) {
      return isStream ? 'outline' : 'secondary'
    }
    return isStream ? 'secondary' : 'outline'
  }
}

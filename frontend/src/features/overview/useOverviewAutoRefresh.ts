import { computed, onScopeDispose, ref } from 'vue'
import { useI18n } from '@/i18n'

export function useOverviewAutoRefresh(refreshRange: () => void) {
  const { t } = useI18n()
  const revision = ref(0)
  const autoRefresh = ref(false)
  const refreshTitle = computed(() => autoRefresh.value
    ? t('overview.stopAutoRefreshEvery10Seconds')
    : t('overview.startAutoRefreshEvery10Seconds'))
  let timer: ReturnType<typeof setInterval> | undefined
  function refreshAll() { refreshRange(); revision.value += 1 }
  function stop() {
    autoRefresh.value = false
    if (timer !== undefined) clearInterval(timer)
    timer = undefined
  }
  function toggleAutoRefresh() {
    if (autoRefresh.value) { stop(); return }
    autoRefresh.value = true
    refreshAll()
    timer = setInterval(() => {
      if (document.visibilityState === 'visible') refreshAll()
    }, 10_000)
  }
  onScopeDispose(stop)
  return { revision, autoRefresh, refreshTitle, refreshAll, toggleAutoRefresh }
}

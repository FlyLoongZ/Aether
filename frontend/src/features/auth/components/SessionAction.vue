<template>
  <RouterLink
    v-if="authStore.accountState === 'authenticated'"
    :to="dashboardPath"
    :class="buttonClass"
  >
    <Rocket
      v-if="variant === 'hero'"
      class="h-5 w-5"
    />
    {{ t('site.home.enterDashboard') }}
  </RouterLink>
  <button
    v-else
    type="button"
    :class="buttonClass"
    :disabled="checking"
    :aria-busy="checking"
    @click="handleClick"
  >
    <Rocket
      v-if="variant === 'hero'"
      class="h-5 w-5"
    />
    {{ label }}
  </button>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import { RouterLink } from 'vue-router'
import { Rocket } from '@lucide/vue'
import { useAuthStore } from '@/stores/auth'
import { useI18n } from '@/i18n'
import { log } from '@/utils/logger'

const props = withDefaults(defineProps<{ variant?: 'compact' | 'header' | 'hero' }>(), { variant: 'header' })
const emit = defineEmits<{ login: [] }>()
const authStore = useAuthStore()
const { t } = useI18n()
const retrying = ref(false)
const checking = computed(() => retrying.value || authStore.accountState === 'checking')
const dashboardPath = computed(() => authStore.canAccessAdmin ? '/admin/dashboard' : '/dashboard')
const label = computed(() => checking.value ? t('auth.session.checking')
  : authStore.accountState === 'unavailable' ? t('auth.session.retry')
  : t(props.variant === 'hero' ? 'site.home.startNow' : 'site.home.login'))
const buttonClass = computed(() => {
  const common = 'inline-flex shrink-0 items-center justify-center gap-2 whitespace-nowrap transition disabled:cursor-wait disabled:opacity-60'
  if (props.variant === 'hero') return `${common} w-[180px] rounded-xl border-2 border-[#cc785c] bg-transparent px-6 py-3 text-base font-semibold text-[#cc785c] hover:bg-[#cc785c]/10 dark:border-[#d4a27f] dark:text-[#d4a27f] dark:hover:bg-[#d4a27f]/10`
  const size = props.variant === 'compact'
    ? 'min-w-[100px] rounded-lg px-3 py-1.5 text-xs'
    : 'min-w-[120px] rounded-xl px-4 py-2 text-sm'
  return `${common} ${size} bg-[#cc785c] font-medium text-white hover:bg-[#b86d52]`
})

async function handleClick() {
  if (authStore.accountState !== 'unavailable') { emit('login'); return }
  retrying.value = true
  try { await authStore.retryAuth() } catch (error) { log.warn('Manual session retry failed', error) }
  finally { retrying.value = false }
}
</script>

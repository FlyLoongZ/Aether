<template>
  <main
    class="startup-screen"
    :role="phase === 'error' ? 'alert' : 'status'"
    aria-live="polite"
  >
    <span
      v-if="phase === 'loading'"
      class="startup-indicator"
      aria-hidden="true"
    />
    <p>{{ t(phase === 'error' ? 'startup.failed' : slow ? 'startup.slow' : 'startup.loading') }}</p>
    <a
      v-if="phase === 'error' || slow"
      :href="retryHref"
      class="startup-retry"
    >
      {{ t('startup.retry') }}
    </a>
  </main>
</template>

<script setup lang="ts">
import { useI18n } from '@/i18n'
import type { StartupPhase } from './state'

withDefaults(defineProps<{ phase: StartupPhase; slow?: boolean; retryHref?: string }>(), {
  slow: false,
  retryHref: '',
})
const { t } = useI18n()
</script>

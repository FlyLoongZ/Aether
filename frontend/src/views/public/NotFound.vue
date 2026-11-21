<template>
  <div class="min-h-screen flex items-center justify-center px-6">
    <EmptyState
      type="notFound"
      size="lg"
      :title="t('ui.notFound')"
      :description="t('common.routeNotFoundDescription')"
      :action-text="t('common.backToHome')"
      @action="goHome"
    />
  </div>
</template>

<script setup lang="ts">
import { useRouter } from 'vue-router'
import EmptyState from '@/components/common/EmptyState.vue'
import { useI18n } from '@/i18n'

const { t } = useI18n()
const router = useRouter()

function goHome() {
  void router.push('/')
}
</script>

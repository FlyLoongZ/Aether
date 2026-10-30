<template>
  <div class="min-h-screen flex items-center justify-center px-6">
    <Card class="w-full max-w-md p-6 space-y-2">
      <h1 class="text-lg font-semibold text-foreground">
        {{ t('site.auth.processing') }}
      </h1>
      <p
        v-if="!failure"
        class="text-sm text-muted-foreground"
        role="status"
        aria-live="polite"
      >
        {{ hint }}
      </p>
      <div
        v-if="failure"
        role="alert"
        class="space-y-3"
      >
        <p class="text-sm text-destructive">
          {{ failure }}
        </p>
        <button
          type="button"
          class="startup-retry"
          @click="processCallback"
        >
          {{ t('auth.session.retry') }}
        </button>
        <RouterLink
          to="/"
          class="ml-3 text-sm underline"
        >
          {{ t('site.home.login') }}
        </RouterLink>
      </div>
    </Card>
  </div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { RouterLink, useRoute, useRouter } from 'vue-router'
import Card from '@/components/ui/card.vue'
import { useAuthStore } from '@/stores/auth'
import { useToast } from '@/composables/useToast'
import { useI18n } from '@/i18n'
import { safeInternalNavigationPath } from '@/utils/navigationSecurity'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()
const { success, error: showError } = useToast()
const { t } = useI18n()
const callbackQuery = { ...route.query }
const hint = ref(t('site.auth.waiting'))
const failure = ref('')
let active = true
let processing = false

function consumeRedirectPath(): string | null {
  try {
    const redirectPath = sessionStorage.getItem('redirectPath')
    sessionStorage.removeItem('redirectPath')
    return safeInternalNavigationPath(redirectPath)
  } catch { return null }
}

function errorMessageFromCode(code: string): string {
  const map: Record<string, string> = {
    authorization_denied: t('site.auth.cancelled'),
    provider_disabled: t('site.auth.providerDisabled'),
    provider_unavailable: t('site.auth.providerUnavailable'),
    invalid_callback: t('site.auth.invalidCallback'),
    invalid_state: t('site.auth.invalidState'),
    token_exchange_failed: t('site.auth.tokenExchangeFailed'),
    userinfo_fetch_failed: t('site.auth.userinfoFetchFailed'),
    email_exists_local: t('site.auth.emailExistsLocal'),
    email_is_ldap: t('site.auth.emailIsLdap'),
    email_is_oauth: t('site.auth.emailIsOauth'),
    registration_disabled: t('site.auth.registrationDisabled'),
    oauth_already_bound: t('site.auth.oauthAlreadyBound'),
    already_bound_provider: t('site.auth.alreadyBoundProvider'),
    last_oauth_binding: t('site.auth.lastOauthBinding'),
    last_login_method: t('site.auth.lastLoginMethod'),
    ldap_no_oauth: t('site.auth.ldapNoOauth'),
  }
  return map[code] || t('site.auth.failed')
}

async function processCallback() {
  if (processing) return
  processing = true
  failure.value = ''
  try {
    const oauthBound = callbackQuery.oauth_bound
    if (typeof oauthBound === 'string' && oauthBound) {
      success(t('site.auth.bound', { provider: oauthBound }))
      await router.replace(consumeRedirectPath() || '/dashboard/settings')
      return
    }
    const errorCode = callbackQuery.error_code
    if (typeof errorCode === 'string' && errorCode) {
      showError(errorMessageFromCode(errorCode))
      await router.replace(consumeRedirectPath() || '/')
      return
    }

    // 回调代表新的登录事件，不能复用先前“无会话”的已完成判断。
    hint.value = t('site.auth.writing')
    if (!(await authStore.restoreSession(true, true))) throw new Error(t('site.auth.failed'))
    if (!active) return
    hint.value = t('site.auth.fetchingUser')
    const user = await authStore.fetchCurrentUser()
    if (!active) return
    if (!user) throw new Error(t('site.auth.userinfoFetchFailed'))
    success(t('site.auth.success'))
    await router.replace(consumeRedirectPath() || (authStore.canAccessAdmin ? '/admin/dashboard' : '/dashboard'))
  } catch (error) {
    if (active) failure.value = error instanceof Error ? error.message : t('site.auth.failed')
    if (active) hint.value = t('site.auth.waiting')
  } finally {
    processing = false
  }
}

onMounted(() => {
  // 不读取旧 fragment 中的 token；在恢复会话之前移除 URL 中的敏感状态。
  window.history.replaceState({}, document.title, window.location.pathname)
  void processCallback()
})
onUnmounted(() => { active = false })
</script>

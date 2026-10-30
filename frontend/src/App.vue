<template>
  <StartupScreen
    v-if="startupState.phase !== 'ready'"
    :phase="startupState.phase"
    :slow="startupState.slow"
    :retry-href="startupState.retryHref"
  />
  <RouterView v-else />
  <ToastContainer />
  <ConfirmContainer />
</template>

<script setup lang="ts">
import { onMounted, onErrorCaptured, onUnmounted } from 'vue'
import { useAuthStore } from '@/stores/auth'
import ToastContainer from '@/components/ToastContainer.vue'
import ConfirmContainer from '@/components/ConfirmContainer.vue'
import {
  AUTH_SESSION_SIGNAL_KEY,
  AUTH_STATE_CHANGE_EVENT,
  parseAuthSessionSignal,
  type AuthStateChangeDetail,
} from '@/api/client'
import StartupScreen from '@/startup/StartupScreen.vue'
import { startup } from '@/startup/state'
import { isModuleLoadError } from '@/utils/importRetry'
import router from '@/router'
import { requiresAuthentication } from '@/router/authBootstrap'
import { log } from '@/utils/logger'

const authStore = useAuthStore()
const startupState = startup.state

// 全局错误处理器 - 只处理特定错误,避免完全吞掉所有错误
onErrorCaptured((error: Error) => {
  log.error('Error captured in component', error)
  if (startup.state.phase !== 'ready' || isModuleLoadError(error)) {
    startup.fail()
    return false
  }
  // 对于非关键错误,不阻止传播
  return true
})

// 模块失败由可见启动壳承接，禁止自动刷新循环与全局清缓存。
function handleUnhandledRejection(event: PromiseRejectionEvent): void {
  if (isModuleLoadError(event.reason)) {
    event.preventDefault()
    startup.fail()
  }
  log.error('Unhandled promise rejection', event.reason)
}

function handleGlobalError(event: ErrorEvent): void {
  if (event.message?.includes('ResizeObserver loop')) {
    event.preventDefault()
    return
  }
  if (isModuleLoadError(event.error)) startup.fail()
  if (event.error) log.error('Global error', event.error)
}

async function syncExternalAuthState(authenticated: boolean): Promise<void> {
  if (!authenticated) {
    if (authStore.token || authStore.user) {
      authStore.applyExternalLogout()
      if (requiresAuthentication(router.currentRoute.value)) await router.replace('/')
    }
    return
  }

  if (!await authStore.applyExternalLogin()) {
    return
  }

  if (router.currentRoute.value.path.startsWith('/admin') && !authStore.canAccessAdmin) {
    await router.replace('/dashboard')
  }
}

function handleAuthStorageChange(event: StorageEvent): void {
  if (event.key !== AUTH_SESSION_SIGNAL_KEY) {
    return
  }

  const signal = parseAuthSessionSignal(event.newValue)
  if (!signal) {
    return
  }
  syncExternalAuthState(signal.authenticated).catch((err) => log.error('syncExternalAuthState failed', err))
}

function handleLocalAuthStateChange(event: Event): void {
  const authEvent = event as CustomEvent<AuthStateChangeDetail>
  if (!authEvent.detail) {
    return
  }
  if (!authEvent.detail.authenticated) {
    syncExternalAuthState(false).catch((err) => log.error('syncExternalAuthState failed', err))
  } else {
    authStore.syncToken()
  }
}

onMounted(() => {
  if (typeof window !== 'undefined') {
    window.addEventListener('unhandledrejection', handleUnhandledRejection)
    window.addEventListener('error', handleGlobalError)
    window.addEventListener('storage', handleAuthStorageChange)
    window.addEventListener(AUTH_STATE_CHANGE_EVENT, handleLocalAuthStateChange as (event: Event) => void)
  }

  // 会话恢复由路由策略或 OAuth 回调负责，不再无条件抢先发起。
})

onUnmounted(() => {
  if (typeof window !== 'undefined') {
    window.removeEventListener('unhandledrejection', handleUnhandledRejection)
    window.removeEventListener('error', handleGlobalError)
    window.removeEventListener('storage', handleAuthStorageChange)
    window.removeEventListener(
      AUTH_STATE_CHANGE_EVENT,
      handleLocalAuthStateChange as (event: Event) => void,
    )
  }
})
</script>

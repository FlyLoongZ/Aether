import type { NavigationGuard } from 'vue-router'
import type { useAuthStore } from '@/stores/auth'
import type { useModuleStore } from '@/stores/modules'
import { log } from '@/utils/logger'
import { requiresAuthentication, resolveAuthBootstrap } from '../authBootstrap'
import { ensureUserLoaded } from './authGuard'
import { resolveHomeRedirect } from './homeGuard'
import { checkAdminAccess } from './adminGuard'
import { checkModuleAccess } from './moduleGuard'

export function createAccessGuard(
  getAuthStore: () => ReturnType<typeof useAuthStore>,
  getModuleStore: () => ReturnType<typeof useModuleStore>,
): NavigationGuard {
  return async (to, from) => {
    // 公共内容及回调先进入；后台恢复只在导航成功后启动。
    if (resolveAuthBootstrap(to, from) !== 'blocking') return true
    const authStore = getAuthStore()
    const authenticated = await ensureUserLoaded(authStore)
    const requiresAuth = requiresAuthentication(to)

    // token 已恢复但角色未知时，不误把管理员跳到普通用户仪表盘。
    if (authenticated && !authStore.user) {
      if (!requiresAuth) return true
      throw new Error('用户信息暂时无法加载，请重试。')
    }

    const homeRedirect = resolveHomeRedirect(to, from, authStore)
    if (homeRedirect !== null) return homeRedirect === '' ? true : homeRedirect

    if (requiresAuth && !authenticated) {
      try { sessionStorage.setItem('redirectPath', to.fullPath) } catch { /* 存储受限 */ }
      log.debug('No valid session found, redirecting to home')
      return '/'
    }

    const moduleStore = getModuleStore()
    if (to.matched.some(record => record.meta.requiresAdmin)) {
      return await checkAdminAccess(to, authStore, moduleStore) ?? true
    }
    return await checkModuleAccess(to, moduleStore) ?? true
  }
}

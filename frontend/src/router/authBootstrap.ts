import type { RouteLocationNormalized } from 'vue-router'
import { canStayOnHome } from './guards/homeGuard'

export type AuthBootstrap = 'skip' | 'background' | 'blocking' | 'owned'
declare module 'vue-router' {
  interface RouteMeta {
    authBootstrap?: AuthBootstrap | 'home'
  }
}

export function requiresAuthentication(to: RouteLocationNormalized): boolean {
  // 父路由要求认证时，子路由不得通过覆盖 meta 绕过。
  return to.matched.some(record => record.meta.requiresAuth !== false ||
    record.meta.requiresAdmin || record.meta.module)
}

export function resolveAuthBootstrap(
  to: RouteLocationNormalized,
  from: RouteLocationNormalized,
): AuthBootstrap {
  if (requiresAuthentication(to)) return 'blocking'
  if (to.meta.authBootstrap === 'home') return canStayOnHome(to, from) ? 'background' : 'blocking'
  return to.meta.authBootstrap ?? 'skip'
}

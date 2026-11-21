import type { RouteRecordRaw } from 'vue-router'
import { view } from './helpers'
import { adminRoutes } from './admin'
import { dashboardRoutes } from './dashboard'
import { publicRoutes } from './public'

export const routes: RouteRecordRaw[] = [
  ...publicRoutes,
  ...dashboardRoutes,
  ...adminRoutes,
  {
    // Without a catch-all, an unknown URL matched nothing: `accessGuard` let it
    // through, `afterEach` saw no failure and dropped the startup shell, leaving the
    // content area blank with no explanation.
    path: '/:pathMatch(.*)*',
    name: 'NotFound',
    component: view(() => import('@/views/public/NotFound.vue')),
    meta: { requiresAuth: false, authBootstrap: 'skip' }
  }
]

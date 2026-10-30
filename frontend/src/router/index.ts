import { createRouter, createWebHistory } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import { useModuleStore } from '@/stores/modules'
import { log } from '@/utils/logger'
import { createAccessGuard } from './guards/accessGuard'
import { resolveAuthBootstrap } from './authBootstrap'
import { routes } from './routes'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
})

router.beforeEach(createAccessGuard(useAuthStore, useModuleStore))

router.afterEach((to, from, failure) => {
  if (!failure && resolveAuthBootstrap(to, from) === 'background') {
    // 不等待，也不在恢复结束后替用户导航。store 会复用保护路由的并发请求。
    void useAuthStore().checkAuth().catch(error => log.warn('Background auth restore failed', error))
  }
})

export default router

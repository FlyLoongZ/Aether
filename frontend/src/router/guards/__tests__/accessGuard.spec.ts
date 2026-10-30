import { describe, expect, it, vi } from 'vitest'
import { createMemoryHistory, createRouter, START_LOCATION, type RouteLocationNormalized } from 'vue-router'
import type { useAuthStore } from '@/stores/auth'
import type { useModuleStore } from '@/stores/modules'
import type { User } from '@/api/auth'
import { publicRoutes } from '../../routes/public'
import { dashboardRoutes } from '../../routes/dashboard'
import { adminRoutes } from '../../routes/admin'
import { resolveAuthBootstrap } from '../../authBootstrap'
import { createAccessGuard } from '../accessGuard'

const router = createRouter({
  history: createMemoryHistory(),
  routes: [...publicRoutes, ...dashboardRoutes, ...adminRoutes],
})
const currentUser: User = {
  id: 'user-1', username: 'user', role: 'user', is_active: true, created_at: '',
}

function setup() {
  const auth = {
    token: null as string | null,
    user: null as User | null,
    sessionStatus: 'anonymous',
    isAuthenticated: false,
    canAccessAdmin: false,
    restoreSession: vi.fn(async () => false),
    fetchCurrentUser: vi.fn(async () => null as User | null),
    logout: vi.fn(),
  }
  const getAuth = vi.fn(() => auth as unknown as ReturnType<typeof useAuthStore>)
  const getModules = vi.fn(() => ({} as ReturnType<typeof useModuleStore>))
  const guard = createAccessGuard(getAuth, getModules)
  const invoke = (path: string, from = '/') => guard(
    router.resolve(path) as RouteLocationNormalized,
    router.resolve(from) as RouteLocationNormalized,
    vi.fn(),
  )
  return { auth, getAuth, getModules, invoke }
}

describe('路由认证启动策略', () => {
  it.each([
    '/status', '/privacy-policy', '/logo-demo', '/register', '/auth/callback',
    '/guide', '/guide/architecture', '/guide/concepts', '/guide/strategy',
    '/guide/advanced', '/guide/faq', '/guide/modules',
  ])('%s 不在守卫中等待认证，也不读取角色或模块状态', async path => {
    const { getAuth, getModules, invoke } = setup()
    await expect(invoke(path, '/status')).resolves.toBe(true)
    expect(getAuth).not.toHaveBeenCalled()
    expect(getModules).not.toHaveBeenCalled()
  })

  it('首页冷启动与回访不阻塞，其他公开页面进入首页保留目的地判断', () => {
    const home = router.resolve('/') as RouteLocationNormalized
    expect(resolveAuthBootstrap(home, START_LOCATION)).toBe('background')
    for (const path of ['/dashboard', '/admin/dashboard', '/guide']) {
      expect(resolveAuthBootstrap(home, router.resolve(path) as RouteLocationNormalized)).toBe('background')
    }
    expect(resolveAuthBootstrap(home, router.resolve('/status') as RouteLocationNormalized)).toBe('blocking')
    expect(resolveAuthBootstrap(router.resolve('/?returnTo=/guide') as RouteLocationNormalized,
      router.resolve('/status') as RouteLocationNormalized)).toBe('background')
  })

  it('公共 meta 不能覆盖父路由的认证要求', () => {
    const protectedChild = {
      ...router.resolve('/guide'),
      meta: { requiresAuth: false, authBootstrap: 'skip' },
      matched: [{ meta: { requiresAuth: true } }, { meta: { requiresAuth: false } }],
    } as unknown as RouteLocationNormalized
    expect(resolveAuthBootstrap(protectedChild, START_LOCATION)).toBe('blocking')
  })

  it('保护路由等待会话恢复后再放行', async () => {
    const { auth, invoke } = setup()
    let finish!: (restored: boolean) => void
    auth.restoreSession.mockImplementation(() => new Promise(resolve => { finish = resolve }))
    auth.fetchCurrentUser.mockImplementation(async () => {
      auth.user = currentUser
      return currentUser
    })
    let settled = false
    const result = Promise.resolve(invoke('/dashboard')).then(value => { settled = true; return value })
    await Promise.resolve()
    expect(settled).toBe(false)
    auth.token = 'restored-token'
    finish(true)
    await expect(result).resolves.toBe(true)
    expect(auth.fetchCurrentUser).toHaveBeenCalledTimes(1)
  })

  it('确认匿名才重定向，并保存受保护路径', async () => {
    const { invoke } = setup()
    await expect(invoke('/dashboard/api-keys')).resolves.toBe('/')
    expect(sessionStorage.getItem('redirectPath')).toBe('/dashboard/api-keys')
  })

  it('网络失败不冒充匿名；角色未知不放行后台', async () => {
    const { auth, invoke } = setup()
    auth.sessionStatus = 'unavailable'
    await expect(invoke('/dashboard')).rejects.toThrow('会话恢复')
    auth.token = 'valid-token'
    auth.sessionStatus = 'authenticated'
    await expect(invoke('/admin/dashboard')).rejects.toThrow('用户信息')
  })

  it('从其他公开页面进入首页，仍按已确认角色跳转', async () => {
    const { auth, invoke } = setup()
    auth.token = 'valid-token'
    auth.user = { ...currentUser, role: 'admin' }
    auth.isAuthenticated = true
    auth.canAccessAdmin = true
    await expect(invoke('/', '/status')).resolves.toBe('/admin/dashboard')
  })
})

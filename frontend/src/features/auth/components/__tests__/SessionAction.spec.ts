import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick, reactive, type App } from 'vue'
import { createMemoryHistory, createRouter } from 'vue-router'
import SessionAction from '../SessionAction.vue'

const store = reactive({
  accountState: 'checking', canAccessAdmin: false, retryAuth: vi.fn(async () => {}),
})
vi.mock('@/stores/auth', () => ({ useAuthStore: () => store }))
let app: App | undefined
afterEach(() => { app?.unmount(); document.body.innerHTML = '' })

function mount() {
  const root = document.createElement('div')
  document.body.append(root)
  const login = vi.fn()
  app = createApp(SessionAction, { onLogin: login })
  app.use(createRouter({ history: createMemoryHistory(), routes: [{ path: '/', component: { template: '<div />' } }] }))
  app.mount(root)
  return { root, login }
}

describe('首页身份操作', () => {
  it('身份未知或角色未恢复时不出现登录入口或仪表盘链接', () => {
    store.accountState = 'checking'
    const { root } = mount()
    expect(root.querySelector('a')).toBeNull()
    expect(root.querySelector('button')?.disabled).toBe(true)
    expect(root.textContent).toContain('恢复登录状态')
  })

  it('身份明确后为管理员显示正确的链接', async () => {
    store.accountState = 'checking'
    const { root } = mount()
    store.canAccessAdmin = true
    store.accountState = 'authenticated'
    await nextTick()
    expect(root.querySelector('a')?.getAttribute('href')).toBe('/admin/dashboard')
  })

  it('匿名允许登录，恢复失败则重试，不误触发登录', async () => {
    store.accountState = 'anonymous'
    const { root, login } = mount()
    root.querySelector('button')?.click()
    expect(login).toHaveBeenCalledTimes(1)
    store.accountState = 'unavailable'
    await nextTick()
    root.querySelector('button')?.click()
    await nextTick()
    expect(store.retryAuth).toHaveBeenCalledTimes(1)
    expect(login).toHaveBeenCalledTimes(1)
  })
})

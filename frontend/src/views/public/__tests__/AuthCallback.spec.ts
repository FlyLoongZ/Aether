import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, h, nextTick, type App } from 'vue'

import AuthCallback from '../AuthCallback.vue'

const routerReplaceMock = vi.hoisted(() => vi.fn())
const authStoreMock = vi.hoisted(() => ({
  canAccessAdmin: false,
  restoreSession: vi.fn(),
  fetchCurrentUser: vi.fn(),
}))
const toastMocks = vi.hoisted(() => ({ success: vi.fn(), error: vi.fn() }))

vi.mock('vue-router', async (importOriginal) => {
  const actual = await importOriginal<typeof import('vue-router')>()
  return {
    ...actual,
    RouterLink: defineComponent({
      props: { to: String },
      setup(props, { slots }) { return () => h('a', { href: props.to }, slots.default?.()) },
    }),
    useRoute: () => ({ query: {} }),
    useRouter: () => ({ replace: routerReplaceMock }),
  }
})

vi.mock('@/stores/auth', () => ({ useAuthStore: () => authStoreMock }))
vi.mock('@/composables/useToast', () => ({ useToast: () => toastMocks }))
vi.mock('@/i18n', () => ({
  useI18n: () => ({
    t: (key: string) => key,
    legacyT: (value: string) => value,
  }),
  setI18nLocale: vi.fn(),
}))
vi.mock('@/components/ui/card.vue', () => ({
  default: defineComponent({
    name: 'CardStub',
    setup(_props, { attrs, slots }) {
      return () => h('div', attrs, slots.default?.())
    },
  }),
}))

let app: App | null = null

async function settle() {
  for (let index = 0; index < 5; index += 1) {
    await Promise.resolve()
    await nextTick()
  }
}

describe('AuthCallback', () => {
  beforeEach(() => {
    routerReplaceMock.mockReset()
    authStoreMock.restoreSession.mockReset().mockResolvedValue(true)
    authStoreMock.fetchCurrentUser.mockReset().mockResolvedValue({ id: 'user-1' })
    toastMocks.success.mockReset()
    toastMocks.error.mockReset()
    sessionStorage.clear()
    window.history.replaceState({}, '', '/auth/callback#access_token=legacy-url-token')
  })

  afterEach(() => {
    app?.unmount()
    app = null
    document.body.innerHTML = ''
  })

  it('renders before session restoration and keeps failures local with a retry', async () => {
    let finish!: (success: boolean) => void
    authStoreMock.restoreSession.mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
    const root = document.createElement('div')
    document.body.appendChild(root)
    app = createApp(AuthCallback)
    app.mount(root)
    expect(root.textContent).toContain('site.auth.processing')
    expect(routerReplaceMock).not.toHaveBeenCalled()
    finish(false)
    await settle()
    expect(root.querySelector('[role="alert"]')).not.toBeNull()
    expect(routerReplaceMock).not.toHaveBeenCalled()
    root.querySelector('button')?.click()
    await settle()
    expect(routerReplaceMock).toHaveBeenCalledWith('/dashboard')
  })

  it('does not navigate after the user leaves during restoration', async () => {
    let finish!: (success: boolean) => void
    authStoreMock.restoreSession.mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
    app = createApp(AuthCallback)
    app.mount(document.createElement('div'))
    app.unmount()
    app = null
    finish(true)
    await settle()
    expect(routerReplaceMock).not.toHaveBeenCalled()
    expect(authStoreMock.fetchCurrentUser).not.toHaveBeenCalled()
  })

  it('discards a legacy fragment and restores only through the HttpOnly cookie', async () => {
    const root = document.createElement('div')
    document.body.appendChild(root)
    app = createApp(AuthCallback)
    app.mount(root)
    await settle()

    expect(window.location.hash).toBe('')
    expect(authStoreMock.restoreSession).toHaveBeenCalledWith(true, true)
    expect(authStoreMock.fetchCurrentUser).toHaveBeenCalledTimes(1)
    expect(routerReplaceMock).toHaveBeenCalledWith('/dashboard')
  })
})

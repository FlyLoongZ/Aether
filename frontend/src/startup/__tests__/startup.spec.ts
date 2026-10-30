import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, type App } from 'vue'
import StartupScreen from '../StartupScreen.vue'
import { createStartupState } from '../state'
import { installStartupFallback } from '../fallback'

let app: App | undefined
let dispose: (() => void) | undefined
afterEach(() => {
  app?.unmount()
  dispose?.()
  vi.useRealTimers()
  document.body.innerHTML = ''
  delete document.documentElement.dataset.appMounted
})

describe('启动壳', () => {
  it('慢加载提供退出等待的选择，路由完成后清理计时器', () => {
    vi.useFakeTimers()
    const startup = createStartupState()
    startup.begin()
    vi.advanceTimersByTime(15_000)
    expect(startup.state.slow).toBe(true)
    startup.ready()
    vi.advanceTimersByTime(30_000)
    expect(startup.state).toMatchObject({ phase: 'ready', slow: false })
    startup.fail('/dashboard/api-keys')
    expect(startup.state).toMatchObject({ phase: 'error', retryHref: '/dashboard/api-keys' })
  })

  it('错误界面不会强刷，重试链接保留失败目的地', () => {
    const root = document.createElement('div')
    app = createApp(StartupScreen, { phase: 'error', retryHref: '/dashboard/api-keys' })
    app.mount(root)
    expect(root.querySelector('[role="alert"]')).not.toBeNull()
    expect(root.querySelector('a')?.getAttribute('href')).toBe('/dashboard/api-keys')
    expect(root.querySelector('.startup-indicator')).toBeNull()
  })

  it('入口 JS 未加载时，静态占位能显示模块失败及重试', () => {
    document.body.innerHTML = '<main data-startup-placeholder><span class="startup-indicator"></span><p data-startup-message>正在加载</p></main>'
    dispose = installStartupFallback(window, document)
    const script = document.createElement('script')
    script.type = 'module'
    document.body.append(script)
    script.dispatchEvent(new Event('error'))
    expect(document.querySelector('[data-startup-message]')?.textContent).toContain('暂时无法加载')
    expect(document.querySelector('.startup-retry')).not.toBeNull()
    expect(document.querySelector('.startup-indicator')).toBeNull()
  })

  it('Vue 接管后，静态计时器和监听不再改变页面', () => {
    vi.useFakeTimers()
    document.body.innerHTML = '<main data-startup-placeholder><p data-startup-message>正在加载</p></main>'
    dispose = installStartupFallback(window, document)
    document.documentElement.dataset.appMounted = 'true'
    window.dispatchEvent(new Event('aether:app-mounted'))
    vi.advanceTimersByTime(30_000)
    const script = document.createElement('script')
    script.type = 'module'
    document.body.append(script)
    script.dispatchEvent(new Event('error'))
    expect(document.querySelector('.startup-retry')).toBeNull()
  })
})

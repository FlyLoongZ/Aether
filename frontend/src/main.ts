import { createApp } from 'vue'
import { createPinia } from 'pinia'
import router from './router'
import './style.css'
import App from './App.vue'
import { isModuleLoadError } from './utils/importRetry'
import { startup } from './startup/state'
import { log } from './utils/logger'
import { createI18n } from './i18n'
import { initializeTheme } from './theme/runtime'

initializeTheme()

startup.begin()
const app = createApp(App)
const pinia = createPinia()
const i18n = createI18n()

app.use(pinia)
app.use(i18n)
// 在初始导航开始前注册失败处理，任何等待都由同主题启动壳承接。
router.onError((error, to) => {
  log.error('Route loading failed', error)
  startup.fail(router.resolve(to).href)
})
router.afterEach((_to, _from, failure) => {
  if (!failure) startup.ready()
})
app.use(router)

// 全局错误处理器 - 只在开发环境下记录详细日志
app.config.errorHandler = (err: unknown, _instance, info) => {
  if (import.meta.env.DEV) {
    console.error('Global error handler:', err, info)
  }

  if (startup.state.phase !== 'ready' || isModuleLoadError(err)) startup.fail()
}

app.mount('#app')
document.documentElement.dataset.appMounted = 'true'
window.dispatchEvent(new Event('aether:app-mounted'))
void router.isReady().then(() => startup.ready()).catch(error => {
  log.error('Initial route failed', error)
  // onError 已记录失败目的地，不用当前 URL 覆盖。
  if (startup.state.phase !== 'error') startup.fail()
})

import { ref } from 'vue'
import { isPinyinLoaded, loadPinyin } from '@/utils/search'

/** 加载失败后的冷却时间：期间不再重试，避免每次按键都打一轮请求。 */
const RETRY_COOLDOWN_MS = 30_000

// pinyinMod 本身就是模块级的，ready/loading/failedAt 跟随它放在模块作用域，
// 这样任一实例加载成功后所有实例的 computed 都会重算，也避免
// 「A 实例已加载、B 实例的 ready 仍是 false」的分叉。
const ready = ref(isPinyinLoaded())
let loading = false
let failedAt = 0

/**
 * 拼音词典的按需加载状态。
 *
 * select-content 与 MultiSelect 都要在「搜索框聚焦 / 输入首个字符」时才加载
 * pinyin-pro（297 kB），并用一个响应式标记触发重新过滤。
 */
export function useLazyPinyin() {
  function ensureLoaded(): void {
    if (ready.value || loading) return
    if (failedAt && Date.now() - failedAt < RETRY_COOLDOWN_MS) return
    loading = true
    void loadPinyin().then((loaded) => {
      loading = false
      ready.value = loaded
      failedAt = loaded ? 0 : Date.now()
    })
  }

  /** 供 computed 显式依赖：词典就绪后需要重新过滤一次 */
  function track(): void {
    void ready.value
  }

  return { ensureLoaded, track }
}

/** 仅测试使用：重置模块级加载状态。 */
export function __resetPinyinLoaderForTests(): void {
  ready.value = isPinyinLoaded()
  loading = false
  failedAt = 0
}

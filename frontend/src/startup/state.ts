import { reactive } from 'vue'

export type StartupPhase = 'loading' | 'ready' | 'error'

export function createStartupState() {
  const state = reactive({ phase: 'loading' as StartupPhase, slow: false, retryHref: '' })
  let timer: ReturnType<typeof setTimeout> | undefined
  const stopTimer = () => { clearTimeout(timer); timer = undefined }
  return {
    state,
    begin() {
      stopTimer()
      state.phase = 'loading'
      state.slow = false
      state.retryHref = ''
      timer = setTimeout(() => { state.slow = true }, 15_000)
    },
    ready() { stopTimer(); state.phase = 'ready'; state.slow = false; state.retryHref = '' },
    fail(retryHref = '') {
      stopTimer()
      state.phase = 'error'
      state.slow = false
      state.retryHref = retryHref
    },
    dispose: stopTimer,
  }
}

export const startup = createStartupState()
if (import.meta.hot) import.meta.hot.dispose(startup.dispose)

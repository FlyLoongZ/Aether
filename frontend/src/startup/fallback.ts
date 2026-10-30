// 应用模块尚未执行时仍能显示失败/慢加载提示；无需 Vue 或外部资源。
export function installStartupFallback(win: Window, doc: Document): () => void {
  const mounted = () => doc.documentElement.dataset.appMounted === 'true'
  let failed = false
  const onDomReady = () => show(failed)
  function show(fatal: boolean) {
    failed ||= fatal
    if (mounted()) return
    const placeholder = doc.querySelector<HTMLElement>('[data-startup-placeholder]')
    if (!placeholder) {
      doc.addEventListener('DOMContentLoaded', onDomReady, { once: true })
      return
    }
    placeholder.setAttribute('role', failed ? 'alert' : 'status')
    const message = placeholder.querySelector('[data-startup-message]')
    if (message) message.textContent = failed
      ? '页面暂时无法加载，请检查网络后重试。' : '加载时间较长，你可以继续等待或重新加载。'
    if (failed) placeholder.querySelector('.startup-indicator')?.remove()
    if (!placeholder.querySelector('.startup-retry')) {
      const retry = doc.createElement('a')
      retry.href = ''
      retry.className = 'startup-retry'
      retry.textContent = '重新加载'
      placeholder.append(retry)
    }
  }
  const timer = win.setTimeout(() => show(false), 15_000)
  function onError(event: Event) {
    const target = event.target as HTMLScriptElement | null
    if (target?.tagName === 'SCRIPT' && target.type === 'module' ||
        'error' in event && (event as ErrorEvent).error) show(true)
  }
  function onRejected() { show(true) }
  function dispose() {
    win.clearTimeout(timer)
    doc.removeEventListener('DOMContentLoaded', onDomReady)
    win.removeEventListener('error', onError, true)
    win.removeEventListener('unhandledrejection', onRejected)
    win.removeEventListener('aether:app-mounted', dispose)
  }
  win.addEventListener('error', onError, true)
  win.addEventListener('unhandledrejection', onRejected)
  win.addEventListener('aether:app-mounted', dispose, { once: true })
  return dispose
}

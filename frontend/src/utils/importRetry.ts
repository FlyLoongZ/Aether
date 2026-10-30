/**
 * 路由模块仅做有界重试；失败交给启动壳，不清缓存、不强刷、不改写 URL。
 */
const MAX_RETRIES = 3
const RETRY_DELAY = 1000
const moduleCache = new Map<string, Promise<unknown>>()

export function isModuleLoadError(error: unknown): boolean {
  const err = error as { message?: string; name?: string } | null
  const message = err?.message || ''
  return message.includes('Failed to fetch') || message.includes('Loading chunk') ||
    message.includes('dynamically imported module') || message.includes('NetworkError') ||
    err?.name === 'ChunkLoadError'
}

export async function importWithRetry<T = unknown>(
  importFn: () => Promise<T>,
  retries: number = MAX_RETRIES,
  cacheKey?: string,
): Promise<T> {
  if (cacheKey && moduleCache.has(cacheKey)) return moduleCache.get(cacheKey) as Promise<T>
  const request = (async () => {
    for (let attempt = 0; ; attempt++) {
      try { return await importFn() }
      catch (error) {
        if (attempt >= Math.min(MAX_RETRIES, Math.max(0, retries)) || !isModuleLoadError(error)) throw error
        await new Promise(resolve => setTimeout(resolve, RETRY_DELAY))
      }
    }
  })()
  if (cacheKey) {
    moduleCache.set(cacheKey, request)
    // 两个分支都处理，避免 finally 派生出未被捕获的拒绝 Promise。
    void request.then(
      () => {},
      () => { if (moduleCache.get(cacheKey) === request) moduleCache.delete(cacheKey) },
    )
  }
  return request
}

export function createRetryableImport(importPath: string) {
  return () => importWithRetry(() => import(/* @vite-ignore */ importPath), MAX_RETRIES, importPath)
}

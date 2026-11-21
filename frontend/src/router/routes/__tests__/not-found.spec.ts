import { describe, expect, it } from 'vitest'
import { createRouter, createMemoryHistory } from 'vue-router'
import { routes } from '@/router/routes'

function buildRouter() {
  return createRouter({ history: createMemoryHistory(), routes })
}

describe('catch-all route', () => {
  it('resolves an unknown path to the NotFound view instead of nothing', async () => {
    // Before the catch-all, an unknown URL matched no record: the guard let it
    // through, `afterEach` saw no failure, the startup shell was removed and the
    // content area stayed blank with no message.
    const router = buildRouter()
    await router.push('/definitely/not/a/route')

    expect(router.currentRoute.value.matched.length).toBeGreaterThan(0)
    expect(router.currentRoute.value.name).toBe('NotFound')
    expect(router.currentRoute.value.meta.requiresAuth).toBe(false)
  })

  it('does not shadow existing routes', () => {
    // `resolve` (not `push`) so the test does not load the layout chunk.
    const router = buildRouter()
    expect(router.resolve('/dashboard').name).toBe('Dashboard')
    expect(router.resolve('/dashboard/api-keys').name).toBe('MyApiKeys')
  })
})

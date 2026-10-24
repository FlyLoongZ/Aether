import { readSource } from '@/tests/readSource'
import { describe, expect, it } from 'vitest'

const source = readSource('src/views/admin/PoolManagement.vue')

describe('PoolManagement provider detail drawer', () => {
  it('keeps the heavy drawer out of the initial route chunk and prefetches it when idle', () => {
    expect(source).not.toContain(
      "import ProviderDetailDrawer from '@/features/providers/components/ProviderDetailDrawer.vue'",
    )
    expect(source).toContain(
      "const loadProviderDetailDrawer = () => import('@/features/providers/components/ProviderDetailDrawer.vue')",
    )
    expect(source).toContain('defineAsyncComponent(loadProviderDetailDrawer)')
    expect(source).toContain('scheduleProviderDetailDrawerPrefetch()')
    expect(source).toContain('@prefetch-provider="prefetchProviderDetailDrawer"')
  })

  it('only mounts the drawer after the provider detail action is opened', () => {
    const drawerTemplate = source
      .split('<ProviderDetailDrawer')[1]
      ?.split('/>')[0]

    expect(drawerTemplate).toBeTruthy()
    expect(drawerTemplate).toContain('v-if="providerDrawerMounted && selectedProviderId"')
    expect(source).toContain('@view-provider="openProviderDrawer"')
    expect(source).toContain('providerDrawerMounted.value = true')
  })
})

import { readSource } from '@/tests/readSource'
import { describe, expect, it } from 'vitest'

const source = readSource('src/views/admin/ProviderManagement.vue')

describe('ProviderManagement detail drawer loading', () => {
  it('keeps the heavy detail drawer out of the initial route chunk', () => {
    expect(source).not.toContain(
      "import ProviderDetailDrawer from '@/features/providers/components/ProviderDetailDrawer.vue'",
    )
    expect(source).toContain(
      "() => import('@/features/providers/components/ProviderDetailDrawer.vue')",
    )
  })

  it('does not resolve the async drawer until it is opened', () => {
    const drawerTemplate = source
      .split('<ProviderDetailDrawer')[1]
      ?.split('/>')[0]

    expect(drawerTemplate).toBeTruthy()
    expect(drawerTemplate).toContain('v-if="providerDrawerMounted"')
    expect(source).toContain('providerDrawerMounted.value = true')
  })
})

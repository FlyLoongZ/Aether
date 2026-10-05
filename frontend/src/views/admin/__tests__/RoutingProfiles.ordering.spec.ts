import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick, reactive, type App } from 'vue'
import RoutingProfiles from '../RoutingProfiles.vue'
import { createEmptyRoutingGroupConfig } from '@/features/routing/utils/routingPolicy'
import type { RoutingGroupRecord, RoutingGroupUpdateRequest } from '@/api/routing-profiles'

const routingApi = vi.hoisted(() => ({
  listRoutingGroups: vi.fn(),
  updateRoutingGroup: vi.fn(),
  createRoutingGroup: vi.fn(),
  deleteRoutingGroup: vi.fn(),
}))
const toast = vi.hoisted(() => ({ success: vi.fn(), error: vi.fn() }))
const globalModelsApi = vi.hoisted(() => ({ getGlobalModels: vi.fn() }))
const route = reactive({ name: 'RoutingProfileDetail', params: { groupId: 'strategy-a' } })

vi.mock('@/api/routing-profiles', () => routingApi)
vi.mock('@/api/global-models', () => globalModelsApi)
vi.mock('@/composables/useToast', () => ({ useToast: () => toast }))
vi.mock('vue-router', () => ({ useRoute: () => route, useRouter: () => ({ replace: vi.fn(), push: vi.fn() }) }))
vi.mock('@/utils/logger', () => ({ log: { error: vi.fn(), warn: vi.fn() } }))
vi.mock('@/features/routing/components', async () => ({
  RoutingFailoverPolicyEditor: (await import('@/features/routing/components/RoutingFailoverPolicyEditor.vue')).default,
  RoutingSchedulingPolicyEditor: (await import('@/features/routing/components/RoutingSchedulingPolicyEditor.vue')).default,
}))
vi.mock('@/features/routing/components/RoutingPriorityPolicyEditor.vue', () => ({ default: { render: () => null } }))

const mounted: Array<{ app: App, root: HTMLElement }> = []

function group(id: string, sortOrder: number, enabled = true): RoutingGroupRecord {
  return {
    id,
    name: id,
    enabled,
    is_system_default: id === 'system-default',
    sort_order: sortOrder,
    config_json: createEmptyRoutingGroupConfig(),
    version: 1,
    created_at: 1,
    updated_at: 1,
  }
}

async function flush() {
  await nextTick()
  await new Promise(resolve => setTimeout(resolve, 0))
  await nextTick()
}

async function mountPage(groups: RoutingGroupRecord[]) {
  routingApi.listRoutingGroups.mockResolvedValue({ items: groups, total: groups.length })
  routingApi.updateRoutingGroup.mockImplementation(async (id: string, payload: RoutingGroupUpdateRequest) => ({
    ...groups.find(entry => entry.id === id),
    ...payload,
    version: 2,
  }))
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(RoutingProfiles)
  app.mount(root)
  mounted.push({ app, root })
  await flush()
  return root
}

function element<T extends HTMLElement>(root: HTMLElement, selector: string): T {
  const found = root.querySelector<T>(selector)
  if (!found) throw new Error(`Missing element: ${selector}`)
  return found
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.stubGlobal('ResizeObserver', class {
    observe() {}
    unobserve() {}
    disconnect() {}
  })
  globalModelsApi.getGlobalModels.mockResolvedValue({ models: [] })
  route.name = 'RoutingProfileDetail'
  route.params.groupId = 'strategy-a'
})

afterEach(() => {
  for (const { app, root } of mounted.splice(0)) {
    app.unmount()
    root.remove()
  }
  vi.unstubAllGlobals()
})


describe('RoutingProfiles create order', () => {
  it('inserts a strategy enabled before saving at the front and renumbers the rest', async () => {
    route.name = 'RoutingProfileCreate'
    route.params = { groupId: '' }
    routingApi.createRoutingGroup.mockImplementation(async (payload: Record<string, unknown>) => ({
      ...group('new-strategy', 0, true),
      ...payload,
    }))

    const root = await mountPage([group('system-default', 0), group('existing', 1)])
    element<HTMLButtonElement>(root, '[aria-label="启用策略"]').click()
    await flush()
    element<HTMLButtonElement>(root, '[aria-label="保存"]').click()
    await flush()

    expect(routingApi.createRoutingGroup).toHaveBeenCalledTimes(1)
    expect(routingApi.createRoutingGroup.mock.calls[0][0].sort_order).toBe(0)

    const renumbered = routingApi.updateRoutingGroup.mock.calls.map(call => call[1].sort_order)
    expect(renumbered).toEqual([1, 2])
    expect(toast.error).not.toHaveBeenCalled()
  })

  it('produces the same order when the strategy is enabled after saving', async () => {
    route.name = 'RoutingProfileCreate'
    route.params = { groupId: '' }
    routingApi.createRoutingGroup.mockImplementation(async (payload: Record<string, unknown>) => ({
      ...group('new-strategy', 0, false),
      ...payload,
    }))

    const root = await mountPage([group('system-default', 0), group('existing', 1)])
    element<HTMLButtonElement>(root, '[aria-label="保存"]').click()
    await flush()

    expect(routingApi.createRoutingGroup).toHaveBeenCalledTimes(1)
    expect(routingApi.createRoutingGroup.mock.calls[0][0].sort_order).toBe(0)

    const renumbered = routingApi.updateRoutingGroup.mock.calls.map(call => call[1].sort_order)
    expect(renumbered).toEqual([1, 2])
  })
})

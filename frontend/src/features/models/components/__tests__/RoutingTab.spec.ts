import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick } from 'vue'

import type {
  ModelRoutingPreviewResponse,
  RoutingKeyInfo,
  RoutingProviderInfo,
} from '@/api/global-models'

const toastMocks = vi.hoisted(() => ({ success: vi.fn(), error: vi.fn() }))
const timerMocks = vi.hoisted(() => ({ tick: { value: 0 }, start: vi.fn() }))

vi.mock('@/api/global-models', () => ({
  getGlobalModelRoutingPreview: vi.fn(),
}))

vi.mock('@/api/endpoints/health', () => ({ recoverKeyHealth: vi.fn() }))

vi.mock('@/utils/errorParser', () => ({
  parseApiError: (_error: unknown, fallback: string) => fallback,
}))

vi.mock('@/composables/useToast', () => ({
  useToast: () => toastMocks,
}))

vi.mock('@/composables/useCountdownTimer', () => ({
  useCountdownTimer: () => timerMocks,
  getProbeCountdown: () => '',
}))

vi.mock('lucide-vue-next', async () => {
  const { defineComponent, h } = await import('vue')
  const stub = (name: string) => defineComponent({ name, setup: () => () => h('span') })
  return {
    RefreshCw: stub('RefreshCw'),
    Loader2: stub('Loader2'),
    ArrowDown: stub('ArrowDown'),
    ChevronDown: stub('ChevronDown'),
    Route: stub('Route'),
    AlertCircle: stub('AlertCircle'),
    Power: stub('Power'),
    Link: stub('Link'),
  }
})

vi.mock('@/components/ui/card.vue', async () => {
  const { defineComponent, h } = await import('vue')
  return {
    default: defineComponent({
      name: 'CardStub',
      setup: (_props, { slots }) => () => h('div', slots.default?.()),
    }),
  }
})

vi.mock('@/components/ui/badge.vue', async () => {
  const { defineComponent, h } = await import('vue')
  return {
    default: defineComponent({
      name: 'BadgeStub',
      setup: (_props, { slots }) => () => h('span', slots.default?.()),
    }),
  }
})

vi.mock('@/components/ui/button.vue', async () => {
  const { defineComponent, h } = await import('vue')
  return {
    default: defineComponent({
      name: 'ButtonStub',
      inheritAttrs: false,
      setup: (_props, { attrs, slots }) => () => h('button', attrs, slots.default?.()),
    }),
  }
})

import RoutingTab from '@/features/models/components/RoutingTab.vue'

function makeKey(
  id: string,
  internalPriority: number,
  effectivePriority: number,
): RoutingKeyInfo {
  return {
    id,
    name: id,
    masked_key: `sk-${id}`,
    internal_priority: internalPriority,
    effective_internal_priority: effectivePriority,
    global_priority_by_format: { 'openai:chat': internalPriority },
    effective_global_priority: effectivePriority,
    is_adaptive: false,
    cache_ttl_minutes: 0,
    health_score: 1,
    is_active: true,
    api_formats: ['openai:chat'],
    circuit_breaker_open: false,
    circuit_breaker_formats: [],
  }
}

function makeProvider(
  id: string,
  catalogPriority: number,
  effectivePriority: number,
  keys: RoutingKeyInfo[],
): RoutingProviderInfo {
  return {
    id,
    name: id,
    model_id: `model-${id}`,
    provider_priority: catalogPriority,
    effective_provider_priority: effectivePriority,
    provider_priority_source: effectivePriority === catalogPriority ? 'catalog' : 'policy_override',
    is_active: true,
    provider_model_name: 'gpt-5',
    model_mappings: [],
    model_is_active: true,
    endpoints: [
      {
        id: `endpoint-${id}`,
        api_format: 'openai:chat',
        base_url: `https://${id}.example`,
        is_active: true,
        keys,
        total_keys: keys.length,
        active_keys: keys.length,
      },
    ],
    total_endpoints: 1,
    active_endpoints: 1,
  }
}

function makeRoutingData(): ModelRoutingPreviewResponse {
  return {
    global_model_id: 'global-gpt-5',
    global_model_name: 'gpt-5',
    display_name: 'GPT 5',
    is_active: true,
    global_model_mappings: [],
    providers: [
      // Catalog priority 20 but policy ranks it first, so the effective value
      // (1) must drive both the order and the rendered label.
      makeProvider('provider-effective-first', 20, 1, [
        makeKey('key-a', 9, 2),
        makeKey('key-b', 3, 5),
      ]),
      // Catalog priority 5 but policy pushes it back to 20.
      makeProvider('provider-catalog-low', 5, 20, [makeKey('key-c', 1, 1)]),
    ],
    total_providers: 2,
    active_providers: 2,
    scheduling_mode: 'cache_affinity',
    priority_mode: 'provider',
    all_keys_whitelist: [],
  }
}

interface MountedTab {
  app: ReturnType<typeof createApp>
  root: HTMLDivElement
}

const mountedTabs: MountedTab[] = []

function mountTab(routingData: ModelRoutingPreviewResponse): MountedTab {
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(RoutingTab as never, {
    routingData,
    globalModelId: routingData.global_model_id,
  })
  app.mount(root)
  const mounted = { app, root }
  mountedTabs.push(mounted)
  return mounted
}

async function expandOnlyFormat(root: HTMLElement) {
  const format = root.querySelector('[data-testid="routing-format"]') as HTMLElement | null
  expect(format).not.toBeNull()
  expect(format?.getAttribute('data-api-format')).toBe('openai:chat')
  format?.click()
  await nextTick()
}

afterEach(() => {
  for (const mounted of mountedTabs.splice(0)) {
    mounted.app.unmount()
    mounted.root.remove()
  }
  vi.clearAllMocks()
})

describe('RoutingTab effective routing policy', () => {
  it('sorts providers and labels them by effective provider priority', async () => {
    const { root } = mountTab(makeRoutingData())
    await nextTick()
    await expandOnlyFormat(root)

    const providers = Array.from(
      root.querySelectorAll('[data-testid="routing-provider"]'),
    ) as HTMLElement[]
    expect(providers.map(provider => provider.getAttribute('data-provider-id'))).toEqual([
      'provider-effective-first',
      'provider-catalog-low',
    ])
    expect(providers.map(provider => provider.getAttribute('data-effective-priority'))).toEqual([
      '1',
      '20',
    ])
    expect(providers[0]?.textContent).toContain('P1')
    // The second provider keeps catalog priority 5 but must render the
    // effective priority 20.
    expect(providers[1]?.textContent).toContain('P20')
    expect(providers[1]?.textContent).not.toContain('P5')
  })

  it.each(['cache_affinity', 'fixed_order', 'load_balance'])(
    'shows the effective priority for the first provider in %s mode', async (mode) => {
      const data = makeRoutingData()
      data.scheduling_mode = mode
      data.providers[0]!.effective_provider_priority = 7
      const { root } = mountTab(data)
      await nextTick()
      await expandOnlyFormat(root)
      const first = root.querySelector('[data-testid="routing-provider"]')
      expect(first?.textContent).toContain('P7')
    },
  )

  it('shows effective priorities for every global key including the first', async () => {
    const data = makeRoutingData()
    data.priority_mode = 'global_key'
    const { root } = mountTab(data)
    await nextTick()
    await expandOnlyFormat(root)
    const keys = Array.from(root.querySelectorAll('[data-testid="routing-key"]'))
    expect(keys.map(key => key.getAttribute('data-key-id'))).toEqual(['key-c', 'key-a', 'key-b'])
    expect(keys.map(key => key.textContent)).toEqual([
      expect.stringContaining('P1'),
      expect.stringContaining('P2'),
      expect.stringContaining('P5'),
    ])
  })

  it('groups provider keys by effective internal priority', async () => {
    const { root } = mountTab(makeRoutingData())
    await nextTick()
    await expandOnlyFormat(root)

    const firstProvider = root.querySelector(
      '[data-provider-id="provider-effective-first"]',
    ) as HTMLElement | null
    expect(firstProvider).not.toBeNull()

    const toggle = firstProvider?.querySelector(
      '[data-testid="routing-provider-toggle"]',
    ) as HTMLElement | null
    toggle?.click()
    await nextTick()

    const keys = Array.from(
      firstProvider?.querySelectorAll('[data-testid="routing-provider-key"]') ?? [],
    ) as HTMLElement[]
    expect(keys.map(key => key.getAttribute('data-key-id'))).toEqual(['key-a', 'key-b'])
    expect(keys.map(key => key.getAttribute('data-effective-priority'))).toEqual(['2', '5'])
  })
})

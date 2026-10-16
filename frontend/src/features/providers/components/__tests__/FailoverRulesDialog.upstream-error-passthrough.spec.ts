import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp, nextTick, type App } from 'vue'

import type { FailoverRulesConfig, ProviderWithEndpointsSummary } from '@/api/endpoints/types'
import FailoverRulesDialog from '../FailoverRulesDialog.vue'

const endpointMocks = vi.hoisted(() => ({
  updateProvider: vi.fn(),
}))

vi.mock('@/api/endpoints', () => ({
  updateProvider: endpointMocks.updateProvider,
}))

vi.mock('@/composables/useToast', () => ({
  useToast: () => ({
    success: vi.fn(),
    error: vi.fn(),
  }),
}))

const mountedApps: Array<{ app: App, root: HTMLElement }> = []

function makeProvider(failoverRules: FailoverRulesConfig | null): ProviderWithEndpointsSummary {
  return {
    id: 'provider-1',
    failover_rules: failoverRules,
  } as ProviderWithEndpointsSummary
}

function mountDialog(failoverRules: FailoverRulesConfig | null = null) {
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(FailoverRulesDialog, {
    open: true,
    provider: makeProvider(failoverRules),
    'onUpdate:open': vi.fn(),
  })
  app.mount(root)
  mountedApps.push({ app, root })
}

async function settle() {
  for (let index = 0; index < 4; index += 1) {
    await Promise.resolve()
    await nextTick()
  }
}

function exhaustionPassthroughSwitch(): HTMLButtonElement {
  const control = document.body.querySelector<HTMLButtonElement>(
    '[role="switch"][aria-label="上游错误透传"]',
  )
  if (!control) throw new Error('Missing upstream-error-passthrough switch')
  return control
}

function clickSave() {
  const button = [...document.body.querySelectorAll<HTMLButtonElement>('button')]
    .find(candidate => candidate.textContent?.trim() === '保存')
  if (!button) throw new Error('Missing save button')
  button.click()
}

beforeEach(() => {
  endpointMocks.updateProvider.mockReset()
  endpointMocks.updateProvider.mockResolvedValue(makeProvider(null))
})

afterEach(() => {
  for (const { app, root } of mountedApps.splice(0)) {
    app.unmount()
    root.remove()
  }
  document.body.innerHTML = ''
})

describe('FailoverRulesDialog upstream error passthrough', () => {
  it('passes the last upstream error through by default and shows the section', async () => {
    mountDialog()
    await settle()

    expect(exhaustionPassthroughSwitch().getAttribute('aria-checked')).toBe('true')
    expect(document.body.textContent).toContain('上游错误透传')
    expect(document.body.textContent).toContain('候选耗尽后返回上游错误')
    expect(document.body.textContent).toContain('关闭后改为返回网关合成的错误（503）')
  })

  it('does not persist the key while the default is kept', async () => {
    mountDialog({ max_retries: 2 })
    await settle()

    clickSave()
    await settle()

    expect(endpointMocks.updateProvider).toHaveBeenCalledWith('provider-1', {
      failover_rules: {
        max_retries: 2,
      },
    })
  })

  it('persists the opt-out when the switch is turned off', async () => {
    mountDialog()
    await settle()

    exhaustionPassthroughSwitch().click()
    await nextTick()
    expect(exhaustionPassthroughSwitch().getAttribute('aria-checked')).toBe('false')

    clickSave()
    await settle()

    expect(endpointMocks.updateProvider).toHaveBeenCalledWith('provider-1', {
      failover_rules: {
        preserve_last_upstream_error_on_exhaustion: false,
      },
    })
  })

  it('loads an opt-out and clears it when the switch is turned back on', async () => {
    mountDialog({
      preserve_last_upstream_error_on_exhaustion: false,
      max_retries: 3,
    })
    await settle()

    expect(exhaustionPassthroughSwitch().getAttribute('aria-checked')).toBe('false')
    exhaustionPassthroughSwitch().click()
    await nextTick()

    clickSave()
    await settle()

    expect(endpointMocks.updateProvider).toHaveBeenCalledWith('provider-1', {
      failover_rules: {
        max_retries: 3,
      },
    })
  })
})

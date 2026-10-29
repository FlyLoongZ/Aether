import { afterEach, describe, expect, it, vi } from 'vitest'
import { createApp, defineComponent, h, ref, type App, type ComponentPublicInstance } from 'vue'
import { createI18n } from '@/i18n'
import OAuthAccountDialog from '../OAuthAccountDialog.vue'

vi.mock('@/stores/proxy-nodes', () => ({
  useProxyNodesStore: () => ({ nodes: [], ensureLoaded: vi.fn() }),
}))
vi.mock('@/components/ui', () => Object.fromEntries([
  'Dialog', 'Button', 'Textarea', 'Popover', 'PopoverTrigger', 'PopoverContent',
].map(name => [name, { render: () => null }])))

interface DialogState {
  parseImportText: (text: string) => { access_token?: string; refresh_token?: string } | null
}

const mounted: Array<{ app: App; root: HTMLElement }> = []

function mountDialog(): DialogState {
  const component = ref<ComponentPublicInstance>()
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp(defineComponent({
    setup: () => () => h(OAuthAccountDialog, {
      open: false,
      providerId: 'provider-codex',
      providerType: 'codex',
      ref: component,
    }),
  }))
  app.use(createI18n())
  app.mount(root)
  mounted.push({ app, root })
  return (component.value!.$ as unknown as { setupState: DialogState }).setupState
}

afterEach(() => {
  for (const { app, root } of mounted.splice(0)) {
    app.unmount()
    root.remove()
  }
})

describe('OAuth credential import', () => {
  it.each(['session_token', 'sessionToken', 'sso_token', 'ssoToken'])(
    'does not promote removed web credential %s to an access token', field => {
      const state = mountDialog()
      expect(state.parseImportText(JSON.stringify({ [field]: 'removed-web-token' }))).toBeNull()
    },
  )

  it.each([
    { access_token: 'access-token' },
    { accessToken: 'access-token' },
    { headers: { Authorization: 'Bearer access-token' } },
    { access_token: 'access-token', session_token: 'must-not-win' },
  ])('preserves supported access-token credentials (%j)', payload => {
    const state = mountDialog()
    expect(state.parseImportText(JSON.stringify(payload))?.access_token).toBe('access-token')
  })

  it('preserves refresh-token imports', () => {
    const state = mountDialog()
    expect(state.parseImportText('{"refresh_token":"refresh-token"}')?.refresh_token).toBe('refresh-token')
  })
})

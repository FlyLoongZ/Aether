import { readSource } from '@/tests/readSource'
import { describe, expect, it } from 'vitest'

describe('ProviderFormDialog Responses WebSocket switch', () => {
  it('displays and submits the switch for every provider type', () => {
    const source = readSource('src/features/providers/components/ProviderFormDialog.vue')

    expect(source).toContain('ui.responsesWebsocketMode')
    expect(source).toContain('responses_websocket_enabled')
    expect(source).toContain('responses_websocket_enabled: form.value.responses_websocket_enabled')
    expect(source).toMatch(
      /<div(?=[^>]*data-testid="responses-websocket-setting")(?![^>]*\bv-if=)[^>]*>[\s\S]{0,500}ui\.responsesWebsocketMode/,
    )
    expect(source).toContain('id="responses-websocket-enabled"')
    expect(source).toContain(':aria-label="t(\'ui.responsesWebsocketMode\')"')
  })
})

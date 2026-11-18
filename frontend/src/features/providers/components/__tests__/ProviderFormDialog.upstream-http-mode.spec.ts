import { readSource } from '@/tests/readSource'
import { describe, expect, it } from 'vitest'

describe('ProviderFormDialog upstream HTTP protocol select', () => {
  const source = readSource('src/features/providers/components/ProviderFormDialog.vue')

  it('offers every supported mode', () => {
    expect(source).toContain('data-testid="upstream-http-mode-setting"')
    expect(source).toContain('ui.upstreamHttpProtocol')
    expect(source).toContain('v-model="form.upstream_http_mode"')
    for (const mode of ['auto', 'http1_only', 'h2c_prior_knowledge']) {
      expect(source).toContain(`<SelectItem value="${mode}">`)
    }
  })

  it('loads the persisted mode and submits it', () => {
    expect(source).toContain('upstream_http_mode: readUpstreamHttpMode(props.provider)')
    expect(source).toContain('upstream_http_mode: form.value.upstream_http_mode')
    expect(source).toContain("provider?.upstream_http_mode")
  })

  it('leaves the nested config merge to the backend', () => {
    // 前端只发一等字段，config.fingerprint 的合并由后端完成，避免整段覆盖丢键。
    expect(source).not.toContain('function withUpstreamHttpMode')
    expect(source).not.toContain('function buildProviderConfig')
  })
})

import { readSource } from '@/tests/readSource'
import { describe, expect, it } from 'vitest'

describe('endpoint form dialog layout', () => {
  it('portals the format selector outside the clipped dialog', () => {
    const source = readSource('src/features/providers/components/EndpointFormDialog.vue')
    const modelIndex = source.indexOf('v-model="newEndpoint.api_format"')
    const selectStart = source.lastIndexOf('<Select', modelIndex)
    const formatSelector = source.slice(
      selectStart,
      source.indexOf('</Select>', modelIndex),
    )

    expect(modelIndex).toBeGreaterThan(-1)
    expect(selectStart).toBeGreaterThan(-1)
    expect(formatSelector).toContain(':disable-portal="false"')
    expect(formatSelector).toContain('var(--radix-select-content-available-height)')
    expect(formatSelector).toContain('var(--radix-select-trigger-width)')
  })
})
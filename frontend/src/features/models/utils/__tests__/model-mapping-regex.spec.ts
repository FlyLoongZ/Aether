import { describe, it, expect } from 'vitest'
import { MAX_MAPPING_LENGTH, validateModelMappingPattern } from '@/features/models/utils/model-mapping-regex'

describe('model-mapping-regex', () => {
  it('validateModelMappingPattern: rejects empty', () => {
    expect(validateModelMappingPattern('').valid).toBe(false)
    expect(validateModelMappingPattern('   ').valid).toBe(false)
  })

  it('validateModelMappingPattern: rejects too long', () => {
    const tooLong = 'a'.repeat(MAX_MAPPING_LENGTH + 1)
    const result = validateModelMappingPattern(tooLong)
    expect(result.valid).toBe(false)
  })

  it('validateModelMappingPattern: rejects potentially dangerous patterns', () => {
    const result = validateModelMappingPattern('(a+)+')
    expect(result.valid).toBe(false)
  })

  it('validateModelMappingPattern: accepts basic patterns', () => {
    expect(validateModelMappingPattern('claude-haiku-.*').valid).toBe(true)
    expect(validateModelMappingPattern('gpt-4o').valid).toBe(true)
  })

})

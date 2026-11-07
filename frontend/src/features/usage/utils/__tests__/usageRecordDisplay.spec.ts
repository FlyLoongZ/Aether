import { describe, expect, it } from 'vitest'

import type { UsageRecord } from '../../types'
import { getModelTooltip, resolveModelFacts } from '../usageRecordDisplay'

function buildRecord(overrides: Partial<UsageRecord> = {}): UsageRecord {
  return {
    id: 'usage-1',
    model: 'gpt-5',
    input_tokens: 100,
    output_tokens: 50,
    total_tokens: 150,
    cost: 0.01,
    response_time_ms: 1000,
    first_byte_time_ms: 500,
    is_stream: true,
    status: 'completed',
    created_at: '2026-05-06T12:00:00Z',
    ...overrides,
  }
}

describe('resolveModelFacts', () => {
  it('keeps mapping and response models when they differ', () => {
    const facts = resolveModelFacts(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5.1',
      response_model: 'gpt-5.2',
    }))

    expect(facts).toEqual({
      requestModel: 'gpt-5',
      mappingModel: 'gpt-5.1',
      responseModel: 'gpt-5.2',
    })
  })

  it('hides the response model when it matches the mapping model', () => {
    const facts = resolveModelFacts(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5.1',
      response_model: 'gpt-5.1',
    }))

    expect(facts.mappingModel).toBe('gpt-5.1')
    expect(facts.responseModel).toBeNull()
  })

  it('keeps the response model when only the case differs from the mapping model', () => {
    const facts = resolveModelFacts(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5.1',
      response_model: ' GPT-5.1 ',
    }))

    expect(facts.mappingModel).toBe('gpt-5.1')
    expect(facts.responseModel).toBe('GPT-5.1')
  })

  it('hides the response model when it matches the request model', () => {
    const facts = resolveModelFacts(buildRecord({
      model: 'gpt-5',
      target_model: null,
      response_model: 'gpt-5',
    }))

    expect(facts.mappingModel).toBeNull()
    expect(facts.responseModel).toBeNull()
  })

  it('keeps a response model when no mapping exists', () => {
    const facts = resolveModelFacts(buildRecord({
      model: 'gpt-5',
      target_model: null,
      response_model: 'gpt-5.1',
    }))

    expect(facts.mappingModel).toBeNull()
    expect(facts.responseModel).toBe('gpt-5.1')
  })

  it('keeps the response model when the mapping equals the request model', () => {
    const facts = resolveModelFacts(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5',
      response_model: 'gpt-5.1',
    }))

    expect(facts.mappingModel).toBeNull()
    expect(facts.responseModel).toBe('gpt-5.1')
  })

  it('trims and ignores blank model facts', () => {
    const facts = resolveModelFacts(buildRecord({
      model: ' gpt-5 ',
      target_model: '   ',
      response_model: '',
    }))

    expect(facts).toEqual({
      requestModel: 'gpt-5',
      mappingModel: null,
      responseModel: null,
    })
  })
})

describe('getModelTooltip', () => {
  it('omits the response model when it matches the mapping model', () => {
    const tooltip = getModelTooltip(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5.1',
      response_model: 'gpt-5.1',
    }))

    expect(tooltip).toBe('gpt-5\n映射模型: gpt-5.1')
  })

  it('keeps both facts when they differ', () => {
    const tooltip = getModelTooltip(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5.1',
      response_model: 'gpt-5.2',
    }))

    expect(tooltip).toBe('gpt-5\n映射模型: gpt-5.1\n响应模型: gpt-5.2')
  })

  it('appends reasoning and tier facts after the model lines', () => {
    const tooltip = getModelTooltip(buildRecord({
      model: 'gpt-5',
      target_model: 'gpt-5.1',
      response_model: 'gpt-5.1',
      requested_reasoning_effort: 'xhigh',
      reasoning_effort: 'max',
      service_tier: 'priority',
    }))

    expect(tooltip).toBe(
      'gpt-5\n映射模型: gpt-5.1\nReasoning: xhigh -> max\n上游请求档位：Fast\n计费档位：Fast',
    )
  })
})

import { describe, it, expect } from 'vitest'
import { parseResponse, renderResponse } from '../registry'

describe('Claude thinking & redacted_thinking parsing', () => {
  it('parses and renders extended thinking block in Claude response', () => {
    const rawClaudeResponse = {
      type: 'message',
      role: 'assistant',
      content: [
        {
          type: 'thinking',
          thinking: 'Let me think through this step-by-step.',
          signature: 'sig_123',
        },
        {
          type: 'text',
          text: 'Here is the final result.',
        },
      ],
    }

    const rendered = renderResponse(rawClaudeResponse, undefined, 'claude')
    expect(rendered.blocks).toHaveLength(1)
    const assistantMsg = rendered.blocks[0]
    if (!assistantMsg || assistantMsg.type !== 'message') throw new Error('expected message block')
    expect(assistantMsg.role).toBe('assistant')
    expect(assistantMsg.badges?.some(b => b.label === '思考')).toBe(true)

    const thinkingBlock = assistantMsg.content.find(b => b.type === 'collapsible')
    expect(thinkingBlock).toBeDefined()
    if (thinkingBlock && thinkingBlock.type === 'collapsible') {
      expect(thinkingBlock.title).toContain('思考过程')
      const firstCode = thinkingBlock.content[0]
      if (firstCode && firstCode.type === 'code') {
        expect(firstCode.code).toBe('Let me think through this step-by-step.')
      }
    }
  })

  it('parses and renders redacted_thinking block gracefully without crashing or losing data', () => {
    const rawClaudeResponse = {
      type: 'message',
      role: 'assistant',
      content: [
        {
          type: 'redacted_thinking',
          data: 'encrypted_data_blob_xyz',
        },
        {
          type: 'text',
          text: 'Answer to user.',
        },
      ],
    }

    const parsed = parseResponse(rawClaudeResponse, undefined, 'claude')
    expect(parsed.messages).toHaveLength(1)
    expect(parsed.messages[0]?.content[0]?.type).toBe('thinking')

    const rendered = renderResponse(rawClaudeResponse, undefined, 'claude')
    const assistantMsg = rendered.blocks[0]
    if (!assistantMsg || assistantMsg.type !== 'message') throw new Error('expected message block')
    expect(assistantMsg.role).toBe('assistant')
    expect(assistantMsg.badges?.some(b => b.label === '思考')).toBe(true)

    const thinkingBlock = assistantMsg.content.find(b => b.type === 'collapsible')
    expect(thinkingBlock).toBeDefined()
    if (thinkingBlock && thinkingBlock.type === 'collapsible') {
      expect(thinkingBlock.title).toContain('思考过程')
    }
  })
})

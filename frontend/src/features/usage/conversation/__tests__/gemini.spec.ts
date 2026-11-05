import { describe, it, expect } from 'vitest'
import { parseResponse, renderResponse } from '../registry'

describe('Gemini conversation parser', () => {
  const requestBody = { model: 'gemini-3-pro-preview' }
  const normalizedResponse = {
    status: 'completed',
    output: [
      {
        type: 'message',
        role: 'assistant',
        content: [
          {
            type: 'output_text',
            text: 'Hello!',
          },
        ],
      },
    ],
  }

  it('parses normalized output payload when hint is gemini', () => {
    const parsed = parseResponse(normalizedResponse, requestBody, 'gemini:generate_content')
    expect(parsed.apiFormat).toBe('gemini')
    expect(parsed.messages).toHaveLength(1)
    expect(parsed.messages[0]?.role).toBe('assistant')
    expect(parsed.messages[0]?.content[0]).toMatchObject({
      type: 'text',
      text: 'Hello!',
    })
  })

  it('renders normalized output payload when hint is gemini', () => {
    const rendered = renderResponse(normalizedResponse, requestBody, 'gemini:generate_content')
    expect(rendered.error).toBeUndefined()
    expect(rendered.blocks).toHaveLength(1)
    expect(rendered.blocks[0]).toMatchObject({
      type: 'message',
      role: 'assistant',
    })

    const firstBlock = rendered.blocks[0]
    if (!firstBlock || firstBlock.type !== 'message') {
      throw new Error('expected first render block to be message')
    }

    expect(firstBlock.content[0]).toMatchObject({
      type: 'text',
      content: 'Hello!',
    })
  })

  it('parses and renders Gemini parts with thought: true', () => {
    const rawGeminiResponse = {
      candidates: [
        {
          content: {
            role: 'model',
            parts: [
              {
                text: 'Thinking through the answer...',
                thought: true,
              },
              {
                text: 'Here is the answer.',
              },
            ],
          },
        },
      ],
    }

    const rendered = renderResponse(rawGeminiResponse, undefined, 'gemini')
    expect(rendered.blocks).toHaveLength(1)
    const modelMsg = rendered.blocks[0]
    if (!modelMsg || modelMsg.type !== 'message') throw new Error('expected message block')
    expect(modelMsg.role).toBe('assistant')
    expect(modelMsg.badges?.some(b => b.label === '思考')).toBe(true)

    const thinkingBlock = modelMsg.content.find(b => b.type === 'collapsible')
    expect(thinkingBlock).toBeDefined()
    if (thinkingBlock && thinkingBlock.type === 'collapsible') {
      expect(thinkingBlock.title).toContain('思考过程')
    }

    const textBlock = modelMsg.content.find(b => b.type === 'text')
    expect(textBlock).toBeDefined()
    if (textBlock && textBlock.type === 'text') {
      expect(textBlock.content).toBe('Here is the answer.')
    }
  })

  it('parses Gemini streaming chunks with thought: true', () => {
    const streamPayload = {
      metadata: { stream: true },
      chunks: [
        {
          candidates: [
            {
              content: {
                role: 'model',
                parts: [{ text: 'Thinking chunk 1...', thought: true }],
              },
            },
          ],
        },
        {
          candidates: [
            {
              content: {
                role: 'model',
                parts: [{ text: 'Thinking chunk 2...', thought: true }],
              },
            },
          ],
        },
        {
          candidates: [
            {
              content: {
                role: 'model',
                parts: [{ text: 'Final response.' }],
              },
            },
          ],
        },
      ],
    }

    const rendered = renderResponse(streamPayload, undefined, 'gemini')
    const modelMsg = rendered.blocks[0]
    if (!modelMsg || modelMsg.type !== 'message') throw new Error('expected message block')
    expect(modelMsg.role).toBe('assistant')
    expect(modelMsg.badges?.some(b => b.label === '思考')).toBe(true)

    const thinkingBlock = modelMsg.content.find(b => b.type === 'collapsible')
    expect(thinkingBlock).toBeDefined()
    if (thinkingBlock && thinkingBlock.type === 'collapsible') {
      expect(thinkingBlock.title).toContain('思考过程')
      const firstCode = thinkingBlock.content[0]
      if (firstCode && firstCode.type === 'code') {
        expect(firstCode.code).toBe('Thinking chunk 1...Thinking chunk 2...')
      }
    }

    const textBlock = modelMsg.content.find(b => b.type === 'text')
    if (textBlock && textBlock.type === 'text') {
      expect(textBlock.content).toBe('Final response.')
    }
  })
})

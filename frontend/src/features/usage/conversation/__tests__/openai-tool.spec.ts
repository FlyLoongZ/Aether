import { describe, it, expect } from 'vitest'
import { parseRequest, renderRequest, renderResponse } from '../registry'
import { groupRenderBlocksIntoTurns } from '../grouper'
import type { MessageRenderBlock } from '../render'

describe('OpenAI Tool Message Parsing & Rendering', () => {
  const requestBodyWithTool = {
    model: 'gpt-4o',
    messages: [
      {
        role: 'user',
        content: '列出目录内容',
      },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          {
            id: 'call_ls_123',
            type: 'function',
            function: {
              name: 'bash',
              arguments: '{"command":"ls frontend"}',
            },
          },
        ],
      },
      {
        role: 'tool',
        tool_call_id: 'call_ls_123',
        content: 'components.json\npackage.json\nsrc\ntailwind.config.js',
      },
    ],
  }

  it('parses tool messages as a single tool_result block without duplicate text block', () => {
    const parsed = parseRequest(requestBodyWithTool, undefined, 'openai')
    expect(parsed.apiFormat).toBe('openai')
    expect(parsed.messages).toHaveLength(3)

    const toolMsg = parsed.messages[2]
    expect(toolMsg?.role).toBe('tool')
    // 关键断言：工具结果只应该有 1 个 content block（tool_result），绝对不能多一个 duplicate 的 text block
    expect(toolMsg?.content).toHaveLength(1)
    expect(toolMsg?.content[0]).toMatchObject({
      type: 'tool_result',
      toolUseId: 'call_ls_123',
      content: 'components.json\npackage.json\nsrc\ntailwind.config.js',
    })
  })

  it('renders tool messages as a single tool_result block without duplicate text block', () => {
    const rendered = renderRequest(requestBodyWithTool, undefined, 'openai')
    expect(rendered.error).toBeUndefined()
    expect(rendered.blocks).toHaveLength(3)

    const toolBlock = rendered.blocks[2] as MessageRenderBlock
    expect(toolBlock.type).toBe('message')
    expect(toolBlock.role).toBe('tool')

    // 关键断言：渲染出的内容块中，工具结果只包含 1 个 tool_result 块，避免上部散装文本 + 下部卡片的双重重复输出
    expect(toolBlock.content).toHaveLength(1)
    expect(toolBlock.content[0]).toMatchObject({
      type: 'tool_result',
      content: 'components.json\npackage.json\nsrc\ntailwind.config.js',
    })
  })

  it('properly associates tool messages into conversation turns in grouper', () => {
    const rendered = renderRequest(requestBodyWithTool, undefined, 'openai')
    const grouped = groupRenderBlocksIntoTurns(rendered.blocks)

    expect(grouped.turns).toHaveLength(1)
    const firstTurn = grouped.turns[0]
    expect(firstTurn).toBeDefined()
    expect(firstTurn?.assistant).toBeDefined()

    // 轮次的 assistant 应当包含 tool_use 和 tool_result 两个内容块
    const assistantBlocks = firstTurn?.assistant?.content ?? []
    expect(assistantBlocks.some(b => b.type === 'tool_use')).toBe(true)
    expect(assistantBlocks.some(b => b.type === 'tool_result')).toBe(true)
  })

  it('renders reasoning from reasoning_content in OpenAI response', () => {
    const responseBody = {
      model: 'o1-preview',
      choices: [
        {
          message: {
            role: 'assistant',
            reasoning_content: 'I need to check the file system first.',
            content: 'Done!',
          },
        },
      ],
    }

    const rendered = renderResponse(responseBody, undefined, 'openai')
    const assistantMsg = rendered.blocks[0] as MessageRenderBlock
    expect(assistantMsg).toBeDefined()
    expect(assistantMsg.role).toBe('assistant')
    expect(assistantMsg.badges?.some(b => b.label === '思考')).toBe(true)

    const reasoningBlock = assistantMsg.content.find(b => b.type === 'collapsible')
    expect(reasoningBlock).toBeDefined()
    if (reasoningBlock && reasoningBlock.type === 'collapsible') {
      expect(reasoningBlock.title).toContain('思考过程')
    }
  })

  it('renders reasoning from streaming delta in OpenAI response', () => {
    const streamPayload = {
      metadata: { stream: true },
      chunks: [
        {
          choices: [
            {
              index: 0,
              delta: { reasoning_content: 'Reasoning step 1... ' },
            },
          ],
        },
        {
          choices: [
            {
              index: 0,
              delta: { reasoning_content: 'Reasoning step 2.' },
            },
          ],
        },
        {
          choices: [
            {
              index: 0,
              delta: { content: 'Actual response.' },
            },
          ],
        },
      ],
    }

    const rendered = renderResponse(streamPayload, undefined, 'openai')
    const assistantMsg = rendered.blocks[0] as MessageRenderBlock
    expect(assistantMsg).toBeDefined()
    expect(assistantMsg.badges?.some(b => b.label === '思考')).toBe(true)

    const reasoningBlock = assistantMsg.content.find(b => b.type === 'collapsible')
    expect(reasoningBlock).toBeDefined()

    const textBlock = assistantMsg.content.find(b => b.type === 'text')
    if (textBlock && textBlock.type === 'text') {
      expect(textBlock.content).toBe('Actual response.')
    }
  })
})

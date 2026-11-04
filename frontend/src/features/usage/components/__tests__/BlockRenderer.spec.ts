import { afterEach, describe, it, expect } from 'vitest'
import { createApp, h, type App } from 'vue'
import BlockRenderer from '../RequestDetailDrawer/BlockRenderer.vue'
import type { RenderBlock } from '../../conversation'

const mountedApps: Array<{ app: App, root: HTMLElement }> = []

afterEach(() => {
  for (const { app, root } of mountedApps.splice(0)) {
    app.unmount()
    root.remove()
  }
})

function mountBlockRenderer(blocks: RenderBlock[]) {
  const root = document.createElement('div')
  document.body.appendChild(root)
  const app = createApp({
    render: () => h(BlockRenderer, { blocks }),
  })
  app.mount(root)
  mountedApps.push({ app, root })
  return root
}

describe('BlockRenderer with AI Elements', () => {
  it('renders tool_use block using ToolCard', () => {
    const blocks: RenderBlock[] = [
      {
        type: 'tool_use',
        toolName: 'bash',
        toolId: 'call_123',
        input: '{"command":"ls -la"}',
      },
    ]

    const root = mountBlockRenderer(blocks)

    expect(root.textContent).toContain('bash')
    expect(root.textContent).toContain('#call_123')
    expect(root.textContent).toContain('ls -la')
  })

  it('renders tool_result block with expandable output and line count', () => {
    const blocks: RenderBlock[] = [
      {
        type: 'tool_result',
        content: 'file1.txt\nfile2.txt\nfile3.txt',
        isError: false,
      },
    ]

    const root = mountBlockRenderer(blocks)

    expect(root.textContent).toContain('工具结果')
    expect(root.textContent).toContain('file1.txt')
    expect(root.textContent).toContain('file3.txt')
  })

  it('renders thinking collapsible block using ReasoningCard', () => {
    const blocks: RenderBlock[] = [
      {
        type: 'collapsible',
        title: '思考过程 (30 字符)',
        className: 'thinking-block',
        defaultOpen: true,
        content: [
          {
            type: 'code',
            code: 'This is the reasoning process.',
          },
        ],
      },
    ]

    const root = mountBlockRenderer(blocks)

    expect(root.textContent).toContain('思考过程')
    expect(root.textContent).toContain('This is the reasoning process.')
  })

  it('renders tool role message without redundant outer card when content is tool blocks', () => {
    const blocks: RenderBlock[] = [
      {
        type: 'message',
        role: 'tool',
        roleLabel: 'Tool',
        content: [
          {
            type: 'tool_result',
            content: 'components.json\npackage.json',
          },
        ],
      },
    ]

    const root = mountBlockRenderer(blocks)

    // 应该直接渲染内层工具卡片，不出现两层独立的卡片头
    expect(root.textContent).toContain('工具结果')
    expect(root.textContent).toContain('components.json')
  })

  it('trims trailing whitespace in text blocks and skips empty text blocks to eliminate whitespace gaps', () => {
    const blocks: RenderBlock[] = [
      {
        type: 'text',
        content: 'Hello world with trailing newlines.\n\n\n\n',
      },
      {
        type: 'text',
        content: '   \n\n   ',
      },
      {
        type: 'tool_use',
        toolName: 'read',
        toolId: 'call_1',
        input: '{"path":"/tmp/test.txt"}',
      },
    ]

    const root = mountBlockRenderer(blocks)
    const preTags = root.querySelectorAll('pre')
    // 应该只有 1 个正文文本 pre（空文本块被过滤），且其内容被 trimEnd
    const textPre = Array.from(preTags).find(p => p.textContent?.includes('Hello world'))
    expect(textPre).toBeDefined()
    expect(textPre?.textContent).toBe('Hello world with trailing newlines.')
  })
})

// @vitest-environment node
import { readFile } from 'node:fs/promises'
import { runInNewContext } from 'node:vm'
import { describe, expect, it } from 'vitest'
import { compileThemeBootstrap, startupHtmlPlugin } from '../startup-html-plugin'
import type { IndexHtmlTransformContext } from 'vite'

describe('HTML 首屏构建', () => {
  it('生成自包含同步脚本，不需要 Vue、body 或外部请求', async () => {
    const code = await compileThemeBootstrap()
    for (const [stored, system, expected] of [
      ['dark', false, 'dark'], ['light', true, 'light'], [null, true, 'dark'],
    ] as const) {
      const root = { dataset: {}, classList: { toggle: () => {} } }
      runInNewContext(code, {
        window: { localStorage: { getItem: () => stored }, matchMedia: () => ({ matches: system }) },
        document: { documentElement: root },
      })
      expect(root.dataset).toMatchObject({ theme: expected })
    }
    expect(code).not.toMatch(/\bimport\s|fetch\(|createApp/)
  })

  it('开发和生产共用的注入发生在资源标签之前，并提供静态占位', async () => {
    const template = await readFile(new URL('../../index.html', import.meta.url), 'utf8')
    const hook = startupHtmlPlugin().transformIndexHtml
    if (!hook || typeof hook === 'function' || !('handler' in hook)) throw new Error('missing HTML hook')
    const html = await hook.handler(template, {} as IndexHtmlTransformContext)
    expect(typeof html).toBe('string')
    const output = html as string
    expect(output.indexOf('id="aether-theme-bootstrap"')).toBeLessThan(output.indexOf('<link'))
    expect(output).toContain('id="aether-startup-style"')
    expect(output).toContain('class="startup-screen"')
    expect(output).not.toContain('<!-- aether:startup')
    expect(output).toContain(':root:not([data-theme])')
  })
})

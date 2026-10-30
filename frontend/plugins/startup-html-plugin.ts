import { readFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import path from 'node:path'
import { build, type Plugin } from 'vite'

const root = path.resolve(import.meta.dirname, '..')
const bootstrapPath = path.join(root, 'src/theme/bootstrap.ts')
const criticalPath = path.join(root, 'src/startup/critical.css')
const placeholderPath = path.join(root, 'src/startup/placeholder.html')

export async function compileThemeBootstrap(): Promise<string> {
  const result = await build({
    configFile: false,
    root,
    logLevel: 'silent',
    build: {
      write: false,
      minify: true,
      lib: { entry: bootstrapPath, name: 'AetherStartup', formats: ['iife'] },
    },
  })
  const outputs = (Array.isArray(result) ? result : [result])
    .flatMap(bundle => 'output' in bundle ? bundle.output : [])
  const script = outputs.find(output => output.type === 'chunk')
  if (!script || script.type !== 'chunk') throw new Error('主题初始化脚本编译失败')
  return script.code.replace(/<\/script/gi, '<\\/script')
}

export function startupCspSources(html: string) {
  const script = html.match(/<script id="aether-theme-bootstrap">([\s\S]*?)<\/script>/)?.[1]
  const style = html.match(/<style id="aether-startup-style">([\s\S]*?)<\/style>/)?.[1]
  if (!script || !style) throw new Error('HTML 缺少首屏初始化资源')
  const hash = (value: string) => `'sha256-${createHash('sha256').update(value).digest('base64')}'`
  return { scriptSrc: [hash(script)], styleSrc: [hash(style)] }
}

export function startupHtmlPlugin(): Plugin {
  let script: Promise<string> | undefined
  return {
    name: 'aether-startup-html',
    transformIndexHtml: {
      order: 'pre',
      async handler(html) {
        script ??= compileThemeBootstrap()
        const [code, css, placeholder] = await Promise.all([
          script, readFile(criticalPath, 'utf8'), readFile(placeholderPath, 'utf8'),
        ])
        return html
          .replace('<!-- aether:startup-head -->', `<script id="aether-theme-bootstrap">${code}</script>\n<style id="aether-startup-style">${css}</style>`)
          .replace('<!-- aether:startup-placeholder -->', placeholder)
      },
    },
    generateBundle: {
      order: 'post',
      handler(_options, bundle) {
        const html = bundle['index.html']
        if (html?.type !== 'asset') throw new Error('构建缺少 index.html')
        this.emitFile({
          type: 'asset',
          fileName: 'startup-csp.json',
          source: JSON.stringify(startupCspSources(String(html.source)), null, 2),
        })
      },
    },
    configureServer(server) {
      server.watcher.add([bootstrapPath, criticalPath, placeholderPath])
    },
    handleHotUpdate(ctx) {
      if (ctx.file.startsWith(path.join(root, 'src/theme/')) ||
          ctx.file.startsWith(path.join(root, 'src/startup/'))) {
        script = undefined
        ctx.server.ws.send({ type: 'full-reload' })
      }
    },
  }
}

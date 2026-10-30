import { readFile } from 'node:fs/promises'
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

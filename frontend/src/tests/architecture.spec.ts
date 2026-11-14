import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import { dirname, join, relative, resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

/**
 * 模块图护栏：从 src 的入口做一次全图分析。
 *
 * 两条断言分别对应两类残留——没有任何生产代码引用的模块，以及只被测试引用的模块。
 * 入口白名单必须显式列出并写明理由，避免护栏长成噪音源。
 */

const root = process.cwd()
const srcDir = resolve(root, 'src')

const SOURCE_EXTENSIONS = ['.ts', '.vue', '.js']
const RESOLVE_SUFFIXES = ['', '.ts', '.vue', '.js', '/index.ts']

/** 被 src 之外的配置引用、因此允许零入边的文件。 */
const ENTRY_FILES = new Set([
  'src/main.ts', // 应用入口
  'src/App.vue',
  'src/theme/bootstrap.ts', // 被 plugins/startup-html-plugin.ts 注入 <head>
])

/** 被 src 之外的配置引用、因此允许零入边的目录。 */
const ENTRY_DIRECTORIES = [
  'src/router/routes/', // 路由懒加载入口，由 router/index.ts 之外的约定装配
  'src/views/', // 被 router/routes 引用
  'src/tests/', // 测试工具（readSource 等），只被 spec 引用
]

/** 四种引用边：静态 import/export、副作用 import、动态 import、Vite worker/asset。 */
const REFERENCE_PATTERNS = [
  /(?:import|export)[^;]*?from\s*['"]([^'"]+)['"]/g,
  /import\s*['"]([^'"]+)['"]/g,
  /import\(\s*['"]([^'"]+)['"]\s*\)/g,
  /new URL\(\s*['"]([^'"]+)['"]\s*,\s*import\.meta\.url\s*\)/g,
]

function isTestFile(file: string): boolean {
  return /__tests__|\.spec\.|\.test\./.test(file)
}

function isDeclarationFile(file: string): boolean {
  return file.endsWith('.d.ts')
}

function walk(directory: string, acc: string[] = []): string[] {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const full = join(directory, entry.name)
    if (entry.isDirectory()) walk(full, acc)
    else if (SOURCE_EXTENSIONS.some(extension => entry.name.endsWith(extension))) acc.push(full)
  }
  return acc
}

function resolveSpecifier(fromFile: string, specifier: string): string | null {
  let base: string
  if (specifier.startsWith('@/')) base = join(srcDir, specifier.slice(2))
  else if (specifier.startsWith('.')) base = resolve(dirname(fromFile), specifier)
  else return null

  for (const suffix of RESOLVE_SUFFIXES) {
    const candidate = base + suffix
    if (existsSync(candidate) && statSync(candidate).isFile()) return candidate
  }
  return null
}

function relativePosix(file: string): string {
  return relative(root, file).split('\\').join('/')
}

function isEntry(file: string): boolean {
  const path = relativePosix(file)
  return ENTRY_FILES.has(path) || ENTRY_DIRECTORIES.some(directory => path.startsWith(directory))
}

const allFiles = walk(srcDir)
const productionFiles = allFiles.filter(file => !isTestFile(file) && !isDeclarationFile(file))

const importedByProduction = new Set<string>()
const importedByTests = new Set<string>()

for (const file of allFiles) {
  const source = readFileSync(file, 'utf8')
  const bucket = isTestFile(file) ? importedByTests : importedByProduction
  for (const pattern of REFERENCE_PATTERNS) {
    for (const match of source.matchAll(pattern)) {
      const resolved = resolveSpecifier(file, match[1])
      if (resolved) bucket.add(resolved)
    }
  }
}

describe('module graph', () => {
  it('has no orphan modules', () => {
    const orphans = productionFiles
      .filter(file => !isEntry(file) && !importedByProduction.has(file))
      .map(relativePosix)
    expect(orphans).toEqual([])
  })

  it('has no modules referenced only by tests', () => {
    const testOnly = productionFiles
      .filter(file => !isEntry(file) && !importedByProduction.has(file) && importedByTests.has(file))
      .map(relativePosix)
    expect(testOnly).toEqual([])
  })
})

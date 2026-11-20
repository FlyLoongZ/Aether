#!/usr/bin/env node
/**
 * Fails the build when the i18n dictionary is duplicated into a second chunk.
 *
 * The dictionary is ~666 KB raw (204 KB gzip) and is preloaded on first paint,
 * so an accidental inline copy is a real regression, not a cosmetic one. The
 * body-document worker used to carry a verbatim copy (720 KB) because three
 * parser helpers imported `translate`; the source guard in
 * `src/features/usage/__tests__/worker-bundle.spec.ts` blocks that known path,
 * and this check catches any new one by looking at the emitted bytes.
 *
 * Usage: `node scripts/check-i18n-chunks.mjs [dist/assets]`
 */
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'

// Default-locale-only value: seeing it in a chunk means that chunk contains the
// statically bundled dictionary. It must appear exactly once — a second copy is
// the regression this guard exists for.
const DEFAULT_LOCALE_MARKER = '收起侧边栏'
// Non-default locale value: it may exist in exactly one lazily loaded chunk, and
// that chunk must not be preloaded from `index.html`.
const ON_DEMAND_LOCALE_MARKER = 'Collapse sidebar'

const assets = resolve(process.argv[2] ?? 'dist/assets')

function jsFiles(directory) {
  return readdirSync(directory).filter(name => name.endsWith('.js'))
}

function chunksContaining(directory, marker) {
  return jsFiles(directory)
    .map(name => join(directory, name))
    .filter(file => readFileSync(file, 'utf8').includes(marker))
}

let defaultLocaleChunks
let onDemandLocaleChunks
let indexHtml
try {
  defaultLocaleChunks = chunksContaining(assets, DEFAULT_LOCALE_MARKER)
  onDemandLocaleChunks = chunksContaining(assets, ON_DEMAND_LOCALE_MARKER)
  indexHtml = readFileSync(resolve(assets, '..', 'index.html'), 'utf8')
} catch (error) {
  console.error(`check-i18n-chunks: cannot read ${assets}: ${error.message}`)
  process.exit(1)
}

const problems = []
if (defaultLocaleChunks.length !== 1) {
  problems.push(
    `the default-locale dictionary appears in ${defaultLocaleChunks.length} chunks, expected exactly 1:\n` +
      defaultLocaleChunks.map(file => `  - ${describe(file)}`).join('\n'),
  )
}
// Exactly one: zero means the bundle name or the marker changed (the guard would
// silently stop guarding), and more than one means a duplicate copy.
if (onDemandLocaleChunks.length !== 1) {
  problems.push(
    `the on-demand locale dictionary appears in ${onDemandLocaleChunks.length} chunks, expected exactly 1:\n` +
      (onDemandLocaleChunks.length === 0
        ? '  (none — the guard is looking at the wrong artifacts)'
        : onDemandLocaleChunks.map(file => `  - ${describe(file)}`).join('\n')),
  )
}
// The previous version only flagged an on-demand chunk that was *both* preloaded
// and not the default chunk, so turning the dynamic import into a static one
// (merging the two dictionaries into the entry chunk) passed every check. Compare
// the two sets directly instead.
if (onDemandLocaleChunks.length === 1 && defaultLocaleChunks.includes(onDemandLocaleChunks[0])) {
  problems.push(
    `the on-demand locale dictionary is bundled into the default-locale chunk ${describe(onDemandLocaleChunks[0])}: ` +
      'the non-default locale must stay behind a dynamic import',
  )
}
for (const file of onDemandLocaleChunks) {
  const name = file.split('/').pop()
  if (indexHtml.includes(name)) {
    problems.push(`the on-demand locale chunk ${name} is preloaded from index.html`)
  }
}

if (problems.length > 0) {
  console.error(
    `check-i18n-chunks: ${problems.join('\n')}\n` +
      'Pass localized labels in instead of importing @/i18n from a separately bundled entry (worker, second app), ' +
      'and keep non-default locales behind a dynamic import.',
  )
  process.exit(1)
}

console.log(
  `check-i18n-chunks: default dictionary in ${defaultLocaleChunks.length} chunk(s), on-demand locale in ${onDemandLocaleChunks.length}, OK`,
)

function describe(file) {
  return `${file.replace(`${resolve(assets, '..')}/`, '')} (${statSync(file).size} bytes)`
}

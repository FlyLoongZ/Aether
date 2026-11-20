import { readFileSync, readdirSync } from 'node:fs'
import { basename, resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { messages, type MessageKey } from '@/i18n/messages'

// Keys that are defined but never referenced. Deleting them is safe only while
// this list stays empty: every key is expected to appear as an exact quoted
// literal somewhere under `src/`. There is no dynamic key construction in the
// codebase (no `t(\`…\`)`, no `'a.' + b`), so a static scan is sufficient.
//
// If a genuine case appears, add it here **with a reason** so the exception is
// visible in review instead of silently exempting a whole prefix.
const ALLOWED_UNUSED_KEYS: ReadonlyMap<MessageKey, string> = new Map()

const root = resolve(import.meta.dirname, '../../..')
const dictionaryFiles = new Set([
  'messages.ts', 'ui-messages.ts', 'overview-messages.ts', 'provider-messages.ts',
  'feedback-messages.ts', 'user-messages.ts',
])

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = resolve(directory, entry.name)
    if (entry.isDirectory()) return sourceFiles(file)
    // Tests count as references: a key used only by a test is still used.
    return /\.(ts|vue)$/.test(entry.name) && !(directory.endsWith('/i18n') && dictionaryFiles.has(basename(file)))
      ? [file]
      : []
  })
}

/**
 * Every quoted literal in the source tree, as one set.
 *
 * Scanning per key would compile thousands of regexes per file; collecting the
 * literals once keeps this test in the millisecond range. Matching is exact, so a
 * key is not "referenced" merely because another key starts with it.
 */
function quotedLiterals(sources: readonly string[]): Set<string> {
  const literals = new Set<string>()
  const quoted = /(['"`])([^'"`\n\\]*)\1/gu
  for (const source of sources) {
    for (const match of source.matchAll(quoted)) literals.add(match[2])
  }
  return literals
}

describe('unused message keys', () => {
  it('reports every key that no source file references', () => {
    const literals = quotedLiterals(
      sourceFiles(resolve(root, 'src')).map(file => readFileSync(file, 'utf8')),
    )
    const unused = (Object.keys(messages['zh-CN']) as MessageKey[])
      .filter(key => !literals.has(key))
      .filter(key => !ALLOWED_UNUSED_KEYS.has(key))

    expect(
      unused.sort(),
      'these i18n keys are defined but never referenced; delete them or add them to ALLOWED_UNUSED_KEYS with a reason',
    ).toEqual([])
  })

  it('keeps the documented exceptions honest', () => {
    for (const [key, reason] of ALLOWED_UNUSED_KEYS) {
      expect(reason.trim(), `exception for ${key} needs a reason`).not.toBe('')
    }
  })
})

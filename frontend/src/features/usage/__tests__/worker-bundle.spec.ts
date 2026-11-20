import { readFileSync, readdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'

// The body-document worker is bundled as its own entry. A value import of the
// i18n module from anything it reaches inlines the whole dictionary into that
// chunk: both locales, ~665 KB, shipped again next to the 666 KB main i18n
// chunk. Labels are passed in through the worker protocol instead, so this guard
// keeps the only known path from coming back.
const root = resolve(import.meta.dirname, '../../../..')

// `utils/` also holds main-thread helpers that legitimately translate, so the
// directory is only checked for the files the worker actually reaches.
const workerReachableFiles = 'src/features/usage/utils/(body-document|json-viewer)'
const workerReachableDirectories = ['src/features/usage/conversation']

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = resolve(directory, entry.name)
    if (entry.isDirectory()) return sourceFiles(file)
    return /\.ts$/.test(entry.name) && !/\.(test|spec)\.ts$/.test(entry.name) ? [file] : []
  })
}

function workerReachableSources(): string[] {
  const utils = sourceFiles(resolve(root, 'src/features/usage/utils')).filter(file =>
    new RegExp(`${workerReachableFiles}\\.ts$`, 'u').test(file.replace(`${root}/`, '')))
  return [
    ...workerReachableDirectories.flatMap(directory => sourceFiles(resolve(root, directory))),
    ...utils,
  ]
}

/** A type-only import is free; only value imports pull the dictionary in. */
const i18nValueImport = /^\s*import\s+(?!type\b)[^;]*from\s+'@\/i18n(?:\/messages)?'/mu

describe('body-document worker bundle', () => {
  it('keeps the i18n dictionary out of the worker-reachable modules', () => {
    const offenders = workerReachableSources()
      .filter(file => i18nValueImport.test(readFileSync(file, 'utf8')))
      .map(file => file.replace(`${root}/`, ''))

    expect(
      offenders,
      'these modules are reachable from the body-document worker and must receive localized labels instead of importing @/i18n',
    ).toEqual([])
  })
})

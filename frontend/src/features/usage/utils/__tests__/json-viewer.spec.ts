import { describe, expect, it, vi } from 'vitest'
import { getRawTextChunk, JsonPageReader, RAW_TEXT_CHUNK_SIZE } from '../json-viewer'

describe('lazy JSON pages', () => {
  it('reuses the traversal cursor for consecutive chunks and safely reloads evicted chunks', () => {
    const reads = vi.fn()
    const data = new Proxy(new Array(1000).fill('value'), {
      get(target, key, receiver) {
        if (typeof key === 'string' && /^\d+$/.test(key)) reads(key)
        return Reflect.get(target, key, receiver)
      },
    })
    const reader = new JsonPageReader(data, { pageSize: 50, expandDepth: 999 })
    for (let page = 0; page < 20; page += 1) {
      expect(reader.read(page).lines[0].lineNumber).toBe(page * 50 + 1)
    }
    expect(reads).toHaveBeenCalledTimes(1000)
    expect(reader.read(19).lines[0].lineNumber).toBe(951)
    expect(reads).toHaveBeenCalledTimes(1000)
    expect(reader.read(0).lines[0].lineNumber).toBe(1)
    expect(reader.read(20).lines[1]?.lineNumber).toBe(1002)
    expect(reader.read(20).hasNext).toBe(false)
    expect(reader.read(21).lines).toEqual([])
  })

  it.each(['中文🙂', '"\\\n\r\t', '\u0000\b\f', '\ud800', '<img src=x>'])('streams complete escaped keys and strings for %j', text => {
    const key = `key-${text.repeat(100)}`
    const value = text.repeat(1000)
    const reader = new JsonPageReader({ [key]: value }, { pageSize: 3, stringChunkSize: 31 })
    const lines = []
    for (let page = 0; page < 10_000; page += 1) {
      const chunk = reader.read(page)
      lines.push(...chunk.lines)
      if (!chunk.hasNext) break
    }
    const tokens = lines.filter(line => line.lineNumber === 2).flatMap(line => line.tokens ?? [])
    expect(tokens.map(token => token.text).join('')).toBe(`${JSON.stringify(key)}: ${JSON.stringify(value)}`)
    expect(new Set(lines.map(line => line.id)).size).toBe(lines.length)
    expect(tokens.every(token => token.text.length <= 31 && !/[\ud800-\udbff]$/.test(token.text))).toBe(true)
    expect(lines.filter(line => line.lineNumber === 2 && !line.continuation)).toHaveLength(1)
  })

  it('keeps raw text Unicode pairs complete across automatic chunk boundaries', () => {
    const value = `${'x'.repeat(RAW_TEXT_CHUNK_SIZE - 1)  }🙂${  '中'.repeat(RAW_TEXT_CHUNK_SIZE)  }RAW-END`
    const first = getRawTextChunk(value)
    const second = getRawTextChunk(value, 1)
    const last = getRawTextChunk(value, 2)
    expect(first.text.endsWith('🙂')).toBe(true)
    expect(first.text + second.text + last.text).toBe(value)
    expect(last.hasNext).toBe(false)
  })
})

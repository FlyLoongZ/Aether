import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

/// Read a repository-relative source file from a Vitest spec.
///
/// Twelve specs assert on `*.vue` source text (lazy-import shape, Tailwind
/// classes, watcher ordering). They each opened the file themselves with
/// `readFileSync(resolve(process.cwd(), path), 'utf8')`; keep the decoding in
/// one place so a move to `import.meta.dirname` only changes one file.
export function readSource(path: string): string {
  return readFileSync(resolve(process.cwd(), path), 'utf8')
}

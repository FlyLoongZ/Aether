import { importWithRetry } from './importRetry'

const CHINESE_CHAR_REGEX = /[\u3400-\u9fff]/u

type PinyinModule = typeof import('pinyin-pro')

let pinyinMod: PinyinModule | null = null
let pinyinLoading: Promise<boolean> | null = null

/** 拼音词典是否已可用（未加载时搜索仍可用，只是不支持拼音/首字母匹配）。 */
export function isPinyinLoaded(): boolean {
  return pinyinMod !== null
}

/**
 * 按需加载拼音词典。
 * @returns 加载成功后为 true；加载失败时为 false（匹配逻辑会自动退回纯文本）。
 */
export function loadPinyin(): Promise<boolean> {
  if (pinyinMod) return Promise.resolve(true)
  if (pinyinLoading) return pinyinLoading

  // 复用路由懒加载的同一套机制：有界重试 + 失败后驱逐缓存条目，
  // 这样一次网络抖动不会让拼音匹配在本会话内永久失效。
  pinyinLoading = importWithRetry(() => import('pinyin-pro'), 3, 'pinyin-pro')
    .then((mod) => {
      pinyinMod = mod
      return true
    })
    .catch(() => {
      pinyinLoading = null
      return false
    })
  return pinyinLoading
}

function normalizeSearchText(value: string): string {
  return value.normalize('NFKC').toLocaleLowerCase().replace(/\s+/g, ' ').trim()
}

function compactSearchText(value: string): string {
  return normalizeSearchText(value).replace(/\s+/g, '')
}

function addSearchTerm(terms: Set<string>, value: string): void {
  const normalizedValue = normalizeSearchText(value)
  if (!normalizedValue) {
    return
  }

  terms.add(normalizedValue)

  const compactValue = normalizedValue.replace(/\s+/g, '')
  if (compactValue && compactValue !== normalizedValue) {
    terms.add(compactValue)
  }
}

function getPinyinSearchTerms(value: string): string[] {
  if (!CHINESE_CHAR_REGEX.test(value) || !pinyinMod) {
    return []
  }

  const fullPinyin = pinyinMod.pinyin(value, {
    toneType: 'none',
    type: 'array',
  })
  const initials = pinyinMod.pinyin(value, {
    pattern: 'first',
    toneType: 'none',
    type: 'array',
  })

  return [
    fullPinyin.join(' '),
    fullPinyin.join(''),
    initials.join(' '),
    initials.join(''),
  ]
}

export function buildSearchTerms(
  ...values: Array<string | null | undefined>
): string[] {
  const terms = new Set<string>()

  for (const rawValue of values) {
    const value = rawValue?.trim()
    if (!value) {
      continue
    }

    addSearchTerm(terms, value)

    for (const pinyinTerm of getPinyinSearchTerms(value)) {
      addSearchTerm(terms, pinyinTerm)
    }
  }

  return Array.from(terms)
}

export function matchesSearchQuery(
  query: string,
  ...values: Array<string | null | undefined>
): boolean {
  const normalizedQuery = normalizeSearchText(query)
  if (!normalizedQuery) {
    return true
  }

  const compactQuery = compactSearchText(normalizedQuery)
  const queryVariants =
    compactQuery && compactQuery !== normalizedQuery
      ? [normalizedQuery, compactQuery]
      : [normalizedQuery]

  const searchTerms = buildSearchTerms(...values)
  return searchTerms.some((term) =>
    queryVariants.some((variant) => term.includes(variant)),
  )
}

import type { OverviewAmount } from '@/api/overview'
import { getI18nLocale, translate } from '@/i18n'

export function count(value: number | null | undefined): string {
  return value == null ? '-' : new Intl.NumberFormat(getI18nLocale(), { maximumFractionDigits: 2 }).format(value)
}

export function attributionLabel(kind: string | null | undefined): string {
  if (kind === 'employee') return translate('overview.memberAccount')
  if (kind === 'standalone') return translate('overview.standaloneBalanceKey')
  return !kind || kind === 'unknown' ? translate('chart.unknown') : kind
}

export function money(amount: OverviewAmount | null | undefined): string {
  if (amount?.value == null) return '-'
  const number = Number(amount.value)
  if (!Number.isFinite(number)) return '-'
  return new Intl.NumberFormat(getI18nLocale(), { style: 'currency', currency: amount.currency || 'USD', currencyDisplay: 'narrowSymbol', minimumFractionDigits: 2, maximumFractionDigits: 6 }).format(number)
}

export function percent(value: number | null | undefined): string {
  return value == null ? '-' : `${(value * 100).toFixed(2)}%`
}

export function timestamp(value: string | null | undefined, timezone?: string): string {
  return !value ? '-' : new Intl.DateTimeFormat(getI18nLocale(), { dateStyle: 'short', timeStyle: 'short', timeZone: timezone }).format(new Date(value))
}

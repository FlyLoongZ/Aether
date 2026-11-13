import type { OverviewAmount } from '@/api/overview'
import { translate } from '@/i18n'

export function amountValue(amount: OverviewAmount | null | undefined): number | null {
  if (amount?.value == null || amount.status === 'unknown') return null
  const value = Number(amount.value)
  return Number.isFinite(value) ? value : null
}

export function amountStatus(amount: OverviewAmount | null | undefined): string {
  if (amountValue(amount) === null) return translate('overview.amountUnknown')
  if (amount?.status === 'known_subtotal') return translate('overview.knownSubtotal')
  if (amount?.status === 'estimated_subtotal') return translate('overview.estimateWithinKnownCoverage')
  if (amount?.status === 'estimated') return translate('overview.amountEstimated')
  return ''
}

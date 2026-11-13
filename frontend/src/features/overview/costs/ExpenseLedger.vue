<template>
  <section
    class="space-y-4"
    data-provider-expenses
  >
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div>
        <h2 class="text-sm font-semibold">
          {{ t('overview.rechargesAndRenewals') }}
        </h2>
        <p class="mt-1 text-xs text-muted-foreground">
          {{ t('overview.recordedExpensesByPaymentDateVoidedEntriesExcluded') }}
        </p>
      </div>
      <div class="flex gap-2">
        <Button
          variant="outline"
          size="sm"
          :disabled="exporting || !data?.total"
          @click="exportReport"
        >
          <Download class="mr-1.5 h-3.5 w-3.5" />{{ t('overview.exportExpenses') }}
        </Button>
        <Button
          size="sm"
          :disabled="!canRecord"
          @click="emit('record')"
        >
          <Plus class="mr-1.5 h-3.5 w-3.5" />{{ t('overview.recordExpense') }}
        </Button>
      </div>
    </div>
    <p
      v-if="actionError"
      class="text-sm text-destructive"
      role="alert"
    >
      {{ actionError }}
    </p>
    <Card class="min-w-0 overflow-hidden">
      <div class="overflow-x-auto px-4 sm:px-5">
        <table class="w-full min-w-[760px] text-sm">
          <thead class="border-b text-xs text-muted-foreground">
            <tr>
              <th class="py-3 text-left font-medium">
                {{ t('overview.providerType') }}
              </th>
              <th class="px-3 py-3 text-right font-medium">
                {{ t('overview.amountPaid') }}
              </th>
              <th class="px-3 py-3 text-left font-medium">
                {{ t('overview.paymentDate') }}
              </th>
              <th class="px-3 py-3 text-left font-medium">
                {{ t('ui.planPeriod') }}
              </th>
              <th class="px-3 py-3 text-left font-medium">
                {{ t('overview.referenceAndNote') }}
              </th>
              <th class="py-3 text-right font-medium">
                {{ t('overview.action') }}
              </th>
            </tr>
          </thead>
          <tbody class="divide-y">
            <tr
              v-for="item in data?.items || []"
              :key="item.id"
            >
              <td class="max-w-44 break-words py-3 font-medium">
                {{ item.provider_name }}<p class="mt-1 text-xs font-normal text-muted-foreground">
                  {{ kindLabel(item.kind) }}
                </p>
              </td>
              <td class="whitespace-nowrap px-3 py-3 text-right tabular-nums">
                {{ amount(item) }}<p class="mt-1 text-xs text-muted-foreground">
                  {{ item.currency }}
                </p>
              </td>
              <td class="whitespace-nowrap px-3 py-3 text-xs">
                {{ timestamp(item.paid_at, range.timezone) }}
              </td>
              <td class="px-3 py-3 text-xs text-muted-foreground">
                {{ item.period_start ? timestamp(item.period_start, range.timezone) : '-' }}<br>{{ item.period_end ? timestamp(item.period_end, range.timezone) : '-' }}
              </td>
              <td class="max-w-56 break-words px-3 py-3 text-xs">
                {{ item.external_reference || '-' }}<p
                  v-if="item.note"
                  class="mt-1 text-muted-foreground"
                >
                  {{ item.note }}
                </p>
              </td>
              <td class="py-3 text-right">
                <Button
                  variant="ghost"
                  size="sm"
                  :aria-label="`${t('ui.void')} ${item.provider_name} ${amount(item)}`"
                  @click="voiding = item"
                >
                  {{ t('ui.void') }}
                </Button>
              </td>
            </tr>
            <tr v-if="!data?.items.length">
              <td
                colspan="6"
                class="py-12 text-center text-muted-foreground"
              >
                {{ loading ? t('overview.loadingExpenses') : data ? t('overview.noRecordedExpensesInThisRange') : t('overview.expenseRecordsUnavailable') }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <div
        v-if="data"
        class="px-4 pb-3 sm:px-5"
      >
        <OverviewPagination
          :total="data.total"
          :offset="data.offset"
          :limit="data.limit"
          :loading="loading"
          @page="value => emit('page', value)"
          @size="value => emit('size', value)"
        />
      </div>
    </Card>
    <Dialog
      :open="Boolean(voiding)"
      :title="t('overview.voidExpenseRecord')"
      :persistent="saving"
      @update:open="value => !value && !saving && (voiding = null)"
    >
      <p class="py-3 text-sm">
        {{ t('overview.voidThisRecordItWillRemainInTheAuditHistoryAndBeExcludedFromExpenseTotals') }}
      </p>
      <p
        v-if="voiding"
        class="pb-3 font-medium"
      >
        {{ voiding.provider_name }} · {{ amount(voiding) }}
      </p>
      <p
        v-if="voidError"
        class="pb-3 text-sm text-destructive"
        role="alert"
      >
        {{ voidError }}
      </p>
      <template #footer>
        <Button
          variant="destructive"
          :disabled="saving"
          @click="confirmVoid"
        >
          {{ saving ? t('overview.processing') : t('overview.voidRecord') }}
        </Button>
        <Button
          variant="outline"
          :disabled="saving"
          @click="voiding = null"
        >
          {{ t('common.cancel') }}
        </Button>
      </template>
    </Dialog>
  </section>
</template>

<script setup lang="ts">
import { onScopeDispose, ref, watch } from 'vue'
import { Download, Plus } from '@lucide/vue'
import { Button, Card, Dialog } from '@/components/ui'
import { providerFinanceApi, type ProviderExpense, type ProviderExpenseKind, type ProviderExpenses } from '@/api/providerFinance'
import type { OverviewRange } from '@/api/overview'
import OverviewPagination from '../components/OverviewPagination.vue'
import { money, timestamp } from '../format'
import { useI18n } from '@/i18n'
const props = defineProps<{ data: ProviderExpenses | null; range: OverviewRange; offset: number; loading: boolean; canRecord: boolean }>()
const emit = defineEmits<{ record: []; page: [offset: number]; size: [limit: number]; changed: [] }>()
const { t } = useI18n()
const exporting = ref(false)
const actionError = ref('')
const voidError = ref('')
const voiding = ref<ProviderExpense | null>(null)
const saving = ref(false)
let controller: AbortController | null = null
watch(voiding, () => { voidError.value = '' })
watch(() => JSON.stringify(props.range), () => { actionError.value = '' })
function amount(item: ProviderExpense) { return money({ value: item.amount, currency: item.currency, basis: 'provider_expense', status: 'known' }) }
function kindLabel(kind: ProviderExpenseKind) { return kind === 'subscription' ? t('overview.subscriptionRenewal') : kind === 'recharge' ? t('overview.balanceRecharge') : t('overview.otherExpense') }
function message(cause: unknown) {
  const error = cause as { response?: { data?: { detail?: string; message?: string } }; message?: string }
  return error.response?.data?.detail || error.response?.data?.message || error.message || t('overview.actionFailedTryAgain')
}
async function confirmVoid() {
  if (!voiding.value || saving.value) return
  saving.value = true
  try { await providerFinanceApi.void(voiding.value.id); voiding.value = null; emit('changed') }
  catch (cause) { voidError.value = message(cause) }
  finally { saving.value = false }
}
async function exportReport() {
  if (exporting.value) return
  actionError.value = ''
  controller?.abort()
  controller = new AbortController()
  exporting.value = true
  try {
    const blob = await providerFinanceApi.exportExpenses(props.range, controller.signal)
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = `aether-provider-expenses-${props.range.from.slice(0, 10)}.csv`
    anchor.click()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
  } catch (cause) {
    if (controller.signal.aborted) return
    const body = (cause as { response?: { data?: Blob } }).response?.data
    if (body instanceof Blob) {
      try { actionError.value = JSON.parse(await body.text()).detail || message(cause) }
      catch { actionError.value = message(cause) }
    } else actionError.value = message(cause)
  } finally { exporting.value = false }
}
onScopeDispose(() => controller?.abort())
</script>

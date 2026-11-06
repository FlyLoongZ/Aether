<template>
  <div class="space-y-6 pb-8">
    <Card class="overflow-hidden">
      <div class="flex flex-wrap items-center justify-between gap-3 border-b p-4">
        <div>
          <h2 class="text-lg font-semibold">
            {{ t('ui.manualProxyNodes') }}
          </h2>
          <p class="text-sm text-muted-foreground">
            {{ t('ui.manualProxyNodesDescription') }}
          </p>
        </div>
        <div class="flex items-center gap-2">
          <Input
            v-model="searchQuery"
            :placeholder="t('ui.searchProxyNodePlaceholder')"
            class="w-56"
          />
          <Button
            variant="outline"
            :disabled="store.loading"
            @click="refresh"
          >
            {{ t('ui.refresh') }}
          </Button>
          <Button @click="openCreate">
            {{ t('ui.addNode') }}
          </Button>
        </div>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{{ t('ui.nodeNameLabel') }}</TableHead>
            <TableHead>{{ t('ui.nodeProxyAddress') }}</TableHead>
            <TableHead>{{ t('ui.nodeRegion') }}</TableHead>
            <TableHead>{{ t('ui.nodeStatus') }}</TableHead>
            <TableHead>{{ t('ui.nodeLastUsed') }}</TableHead>
            <TableHead>{{ t('ui.nodeRequests') }}</TableHead>
            <TableHead>{{ t('ui.nodeFailures') }}</TableHead>
            <TableHead class="text-right">
              {{ t('ui.nodeActions') }}
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow
            v-for="node in pageItems"
            :key="node.id"
          >
            <TableCell class="font-medium">
              {{ node.name }}
            </TableCell>
            <TableCell class="max-w-[320px] truncate font-mono text-xs">
              {{ node.proxy_url || '-' }}
            </TableCell>
            <TableCell>{{ node.region || '-' }}</TableCell>
            <TableCell>
              <Badge :variant="node.status === 'online' ? 'success' : 'secondary'">
                {{ node.status }}
              </Badge>
            </TableCell>
            <TableCell>{{ node.last_used_at || '-' }}</TableCell>
            <TableCell>{{ node.total_requests }}</TableCell>
            <TableCell>{{ node.failed_requests }}</TableCell>
            <TableCell class="text-right">
              <div class="flex justify-end gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  :disabled="testingId === node.id"
                  @click="testNode(node)"
                >
                  {{ testingId === node.id ? t('ui.testing') : t('ui.test') }}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  @click="openEdit(node)"
                >
                  {{ t('ui.nodeEdit') }}
                </Button>
                <Button
                  variant="destructive"
                  size="sm"
                  @click="removeNode(node)"
                >
                  {{ t('ui.nodeDelete') }}
                </Button>
              </div>
            </TableCell>
          </TableRow>
          <TableRow v-if="!store.loading && pageItems.length === 0">
            <TableCell
              colspan="8"
              class="py-10 text-center text-muted-foreground"
            >
              {{ t('ui.noProxyNodes') }}
            </TableCell>
          </TableRow>
        </TableBody>
      </Table>

      <div class="flex items-center justify-between border-t p-4 text-sm text-muted-foreground">
        <span>{{ t('ui.totalNodes', { count: filteredNodes.length }) }}</span>
        <div class="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            :disabled="currentPage <= 1"
            @click="currentPage -= 1"
          >
            {{ t('ui.pagePrevious') }}
          </Button>
          <span>{{ currentPage }} / {{ totalPages }}</span>
          <Button
            variant="outline"
            size="sm"
            :disabled="currentPage >= totalPages"
            @click="currentPage += 1"
          >
            {{ t('ui.pageNext') }}
          </Button>
        </div>
      </div>
    </Card>

    <Dialog
      :open="dialogOpen"
      @update:open="dialogOpen = $event"
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{{ editingNode ? t('ui.editManualProxyNode') : t('ui.addManualProxyNode') }}</DialogTitle>
          <DialogDescription>{{ t('ui.proxyNodePasswordHint') }}</DialogDescription>
        </DialogHeader>
        <div class="space-y-4">
          <div class="space-y-2">
            <Label for="proxy-node-name">{{ t('ui.nodeNameLabel') }}</Label>
            <Input
              id="proxy-node-name"
              v-model="form.name"
              placeholder="jp-proxy-01"
            />
          </div>
          <div class="space-y-2">
            <Label for="proxy-node-url">{{ t('ui.nodeProxyAddress') }}</Label>
            <Input
              id="proxy-node-url"
              v-model="form.proxy_url"
              :placeholder="t('ui.proxyNodeUrlPlaceholder')"
            />
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div class="space-y-2">
              <Label for="proxy-node-username">{{ t('ui.nodeUsername') }}</Label>
              <Input
                id="proxy-node-username"
                v-model="form.username"
                autocomplete="off"
              />
            </div>
            <div class="space-y-2">
              <Label for="proxy-node-password">{{ t('ui.nodePassword') }}</Label>
              <Input
                id="proxy-node-password"
                v-model="form.password"
                type="password"
                autocomplete="new-password"
              />
            </div>
          </div>
          <div class="space-y-2">
            <Label for="proxy-node-region">{{ t('ui.nodeRegion') }}</Label>
            <Input
              id="proxy-node-region"
              v-model="form.region"
              placeholder="jp"
            />
          </div>
        </div>
        <DialogFooter>
          <Button
            variant="outline"
            :disabled="saving"
            @click="dialogOpen = false"
          >
            {{ t('ui.nodeCancel') }}
          </Button>
          <Button
            :disabled="saving"
            @click="saveNode"
          >
            {{ saving ? t('ui.saving') : t('ui.save') }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import {
  Badge,
  Button,
  Card,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  Input,
  Label,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui'
import { proxyNodesApi, type ProxyNode } from '@/api/proxy-nodes'
import { useProxyNodesStore } from '@/stores/proxy-nodes'
import { useI18n } from '@/i18n'
import { useConfirm } from '@/composables/useConfirm'
import { useToast } from '@/composables/useToast'
import { parseApiError } from '@/utils/errorParser'

const { t } = useI18n()
const store = useProxyNodesStore()
const { confirm } = useConfirm()
const { success, error: toastError } = useToast()

const searchQuery = ref('')
const currentPage = ref(1)
const pageSize = ref(20)
const testingId = ref<string | null>(null)
const dialogOpen = ref(false)
const saving = ref(false)
const editingNode = ref<ProxyNode | null>(null)
const form = ref({ name: '', proxy_url: '', username: '', password: '', region: '' })

const filteredNodes = computed(() => {
  const query = searchQuery.value.trim().toLowerCase()
  if (!query) return store.nodes
  return store.nodes.filter(node =>
    [node.name, node.proxy_url, node.region, node.ip]
      .filter(Boolean)
      .some(value => String(value).toLowerCase().includes(query)),
  )
})

const totalPages = computed(() => Math.max(1, Math.ceil(filteredNodes.value.length / pageSize.value)))
const pageItems = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value
  return filteredNodes.value.slice(start, start + pageSize.value)
})

watch([searchQuery, totalPages], () => {
  if (currentPage.value > totalPages.value) currentPage.value = totalPages.value
})

function resetForm() {
  form.value = { name: '', proxy_url: '', username: '', password: '', region: '' }
}

function openCreate() {
  editingNode.value = null
  resetForm()
  dialogOpen.value = true
}

function openEdit(node: ProxyNode) {
  editingNode.value = node
  form.value = {
    name: node.name,
    proxy_url: node.proxy_url || '',
    username: node.proxy_username || '',
    password: '',
    region: node.region || '',
  }
  dialogOpen.value = true
}

async function refresh() {
  await store.fetchNodes()
}

async function saveNode() {
  if (!form.value.name.trim() || !form.value.proxy_url.trim()) {
    toastError(t('ui.proxyNodeNameRequired'))
    return
  }
  saving.value = true
  try {
    const payload = {
      name: form.value.name.trim(),
      proxy_url: form.value.proxy_url.trim(),
      username: form.value.username.trim() || undefined,
      password: form.value.password || undefined,
      region: form.value.region.trim() || undefined,
    }
    if (editingNode.value) {
      await store.updateManualNode(editingNode.value.id, payload)
      success(t('ui.proxyNodeUpdated'))
    } else {
      await store.createManualNode(payload)
      success(t('ui.proxyNodeAdded'))
    }
    dialogOpen.value = false
    resetForm()
  } catch (err) {
    toastError(parseApiError(err, t('ui.failedToSaveProxyNode')))
  } finally {
    saving.value = false
  }
}

async function removeNode(node: ProxyNode) {
  const ok = await confirm({
    title: t('ui.deleteProxyNodeTitle'),
    message: t('ui.confirmDeleteProxyNode', { name: node.name }),
  })
  if (!ok) return
  try {
    await store.deleteNode(node.id)
    success(t('ui.proxyNodeDeleted'))
  } catch (err) {
    toastError(parseApiError(err, t('ui.failedToDeleteProxyNode')))
  }
}

async function testNode(node: ProxyNode) {
  testingId.value = node.id
  try {
    const result = await proxyNodesApi.testNode(node.id)
    if (result.success) {
      success(`${t('ui.connectivityOk')}${result.latency_ms != null ? ` (${result.latency_ms}ms)` : ''}`)
    } else {
      toastError(result.error || t('ui.connectivityFailed'))
    }
  } catch (err) {
    toastError(parseApiError(err, t('ui.connectivityFailed')))
  } finally {
    testingId.value = null
  }
}

onMounted(refresh)
</script>

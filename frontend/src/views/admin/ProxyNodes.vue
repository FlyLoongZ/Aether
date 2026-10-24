<template>
  <div class="space-y-6 pb-8">
    <Card class="overflow-hidden">
      <div class="flex flex-wrap items-center justify-between gap-3 border-b p-4">
        <div>
          <h2 class="text-lg font-semibold">手动代理节点</h2>
          <p class="text-sm text-muted-foreground">管理 HTTP/SOCKS 代理出口；请求由网关直接转发到该代理。</p>
        </div>
        <div class="flex items-center gap-2">
          <Input v-model="searchQuery" placeholder="搜索名称或地址" class="w-56" />
          <Button variant="outline" :disabled="store.loading" @click="refresh">刷新</Button>
          <Button @click="openCreate">添加节点</Button>
        </div>
      </div>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>名称</TableHead>
            <TableHead>代理地址</TableHead>
            <TableHead>地区</TableHead>
            <TableHead>状态</TableHead>
            <TableHead>最后使用</TableHead>
            <TableHead>请求</TableHead>
            <TableHead>失败</TableHead>
            <TableHead class="text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <TableRow v-for="node in pageItems" :key="node.id">
            <TableCell class="font-medium">{{ node.name }}</TableCell>
            <TableCell class="max-w-[320px] truncate font-mono text-xs">{{ node.proxy_url || '-' }}</TableCell>
            <TableCell>{{ node.region || '-' }}</TableCell>
            <TableCell>
              <Badge :variant="node.status === 'online' ? 'success' : 'secondary'">{{ node.status }}</Badge>
            </TableCell>
            <TableCell>{{ node.last_used_at || '-' }}</TableCell>
            <TableCell>{{ node.total_requests }}</TableCell>
            <TableCell>{{ node.failed_requests }}</TableCell>
            <TableCell class="text-right">
              <div class="flex justify-end gap-2">
                <Button variant="outline" size="sm" :disabled="testingId === node.id" @click="testNode(node)">
                  {{ testingId === node.id ? '测试中' : '测试' }}
                </Button>
                <Button variant="outline" size="sm" @click="openEdit(node)">编辑</Button>
                <Button variant="destructive" size="sm" @click="removeNode(node)">删除</Button>
              </div>
            </TableCell>
          </TableRow>
          <TableRow v-if="!store.loading && pageItems.length === 0">
            <TableCell colspan="8" class="py-10 text-center text-muted-foreground">暂无代理节点</TableCell>
          </TableRow>
        </TableBody>
      </Table>

      <div class="flex items-center justify-between border-t p-4 text-sm text-muted-foreground">
        <span>共 {{ filteredNodes.length }} 个节点</span>
        <div class="flex items-center gap-2">
          <Button variant="outline" size="sm" :disabled="currentPage <= 1" @click="currentPage -= 1">上一页</Button>
          <span>{{ currentPage }} / {{ totalPages }}</span>
          <Button variant="outline" size="sm" :disabled="currentPage >= totalPages" @click="currentPage += 1">下一页</Button>
        </div>
      </div>
    </Card>

    <Dialog :open="dialogOpen" @update:open="dialogOpen = $event">
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{{ editingNode ? '编辑手动代理节点' : '添加手动代理节点' }}</DialogTitle>
          <DialogDescription>代理密码不会通过接口返回；留空表示不修改。</DialogDescription>
        </DialogHeader>
        <div class="space-y-4">
          <div class="space-y-2">
            <Label for="proxy-node-name">名称</Label>
            <Input id="proxy-node-name" v-model="form.name" placeholder="jp-proxy-01" />
          </div>
          <div class="space-y-2">
            <Label for="proxy-node-url">代理地址</Label>
            <Input id="proxy-node-url" v-model="form.proxy_url" placeholder="http://127.0.0.1:8080 或 socks5://..." />
          </div>
          <div class="grid grid-cols-2 gap-4">
            <div class="space-y-2">
              <Label for="proxy-node-username">用户名</Label>
              <Input id="proxy-node-username" v-model="form.username" autocomplete="off" />
            </div>
            <div class="space-y-2">
              <Label for="proxy-node-password">密码</Label>
              <Input id="proxy-node-password" v-model="form.password" type="password" autocomplete="new-password" />
            </div>
          </div>
          <div class="space-y-2">
            <Label for="proxy-node-region">地区</Label>
            <Input id="proxy-node-region" v-model="form.region" placeholder="jp" />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" :disabled="saving" @click="dialogOpen = false">取消</Button>
          <Button :disabled="saving" @click="saveNode">{{ saving ? '保存中' : '保存' }}</Button>
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
import { useConfirm } from '@/composables/useConfirm'
import { useToast } from '@/composables/useToast'
import { parseApiError } from '@/utils/errorParser'

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
    toastError('名称和代理地址不能为空')
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
      success('手动代理节点已更新')
    } else {
      await store.createManualNode(payload)
      success('手动代理节点已创建')
    }
    dialogOpen.value = false
    resetForm()
  } catch (err) {
    toastError(parseApiError(err, '保存代理节点失败'))
  } finally {
    saving.value = false
  }
}

async function removeNode(node: ProxyNode) {
  const ok = await confirm({
    title: '删除代理节点',
    message: `确定删除「${node.name}」？引用该节点的 provider / endpoint / key 配置会被清空。`,
  })
  if (!ok) return
  try {
    await store.deleteNode(node.id)
    success('代理节点已删除')
  } catch (err) {
    toastError(parseApiError(err, '删除代理节点失败'))
  }
}

async function testNode(node: ProxyNode) {
  testingId.value = node.id
  try {
    const result = await proxyNodesApi.testNode(node.id)
    if (result.success) {
      success(`连通性正常${result.latency_ms != null ? `（${result.latency_ms}ms）` : ''}`)
    } else {
      toastError(result.error || '连通性测试失败')
    }
  } catch (err) {
    toastError(parseApiError(err, '连通性测试失败'))
  } finally {
    testingId.value = null
  }
}

onMounted(refresh)
</script>

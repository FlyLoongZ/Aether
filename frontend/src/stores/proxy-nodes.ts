import { translate } from '@/i18n'
import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import {
  proxyNodesApi,
  type ManualProxyNodeCreateRequest,
  type ManualProxyNodeUpdateRequest,
  type ProxyNode,
} from '@/api/proxy-nodes'
import { parseApiError } from '@/utils/errorParser'

export const useProxyNodesStore = defineStore('proxy-nodes', () => {
  const nodes = ref<ProxyNode[]>([])
  const total = ref(0)
  const loading = ref(false)
  const error = ref<string | null>(null)
  const fetched = ref(false)

  const onlineNodes = computed(() => nodes.value.filter(node => node.status === 'online'))

  async function fetchNodes(params?: { status?: string }) {
    loading.value = true
    error.value = null
    try {
      const data = await proxyNodesApi.listProxyNodes({ ...params, limit: 1000 })
      nodes.value = data.items
      total.value = data.total
      fetched.value = true
    } catch (err: unknown) {
      error.value = parseApiError(err, translate('ui.failedToLoadProxyNodes'))
    } finally {
      loading.value = false
    }
  }

  async function ensureLoaded() {
    if (!fetched.value && !loading.value) {
      await fetchNodes()
    }
  }

  async function createManualNode(payload: ManualProxyNodeCreateRequest) {
    loading.value = true
    error.value = null
    try {
      const result = await proxyNodesApi.createManualNode(payload)
      await fetchNodes()
      return result
    } catch (err: unknown) {
      error.value = parseApiError(err, translate('ui.failedToCreateManualProxyNode'))
      throw err
    } finally {
      loading.value = false
    }
  }

  async function updateManualNode(nodeId: string, payload: ManualProxyNodeUpdateRequest) {
    loading.value = true
    error.value = null
    try {
      const result = await proxyNodesApi.updateManualNode(nodeId, payload)
      await fetchNodes()
      return result
    } catch (err: unknown) {
      error.value = parseApiError(err, translate('ui.failedToUpdateManualProxyNode'))
      throw err
    } finally {
      loading.value = false
    }
  }

  async function deleteNode(nodeId: string) {
    loading.value = true
    error.value = null
    try {
      await proxyNodesApi.deleteProxyNode(nodeId)
      nodes.value = nodes.value.filter(node => node.id !== nodeId)
      total.value = Math.max(0, total.value - 1)
    } catch (err: unknown) {
      error.value = parseApiError(err, translate('ui.failedToDeleteProxyNode'))
      throw err
    } finally {
      loading.value = false
    }
  }

  return {
    nodes,
    total,
    loading,
    error,
    fetched,
    onlineNodes,
    fetchNodes,
    ensureLoaded,
    createManualNode,
    updateManualNode,
    deleteNode,
  }
})

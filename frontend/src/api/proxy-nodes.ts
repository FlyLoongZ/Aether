import apiClient from './client'

export interface ProxyNode {
  id: string
  name: string
  ip: string
  port: number
  region: string | null
  status: 'online' | 'offline'
  proxy_url?: string | null
  proxy_username?: string | null
  has_proxy_password?: boolean
  registered_by: string | null
  last_used_at: string | null
  total_requests: number
  avg_latency_ms: number | null
  failed_requests: number
  dns_failures: number
  stream_errors: number
  created_at: string | null
  updated_at: string | null
}

export interface ProxyNodeListResponse {
  items: ProxyNode[]
  total: number
  skip: number
  limit: number
}

export interface ManualProxyNodeCreateRequest {
  name: string
  proxy_url: string
  username?: string
  password?: string
  region?: string
}

export interface ManualProxyNodeUpdateRequest {
  name?: string
  proxy_url?: string
  username?: string
  password?: string
  region?: string
}

export interface ProxyNodeTestResult {
  success: boolean
  latency_ms: number | null
  exit_ip: string | null
  error: string | null
  probe_url: string
  timeout_secs: number
}

export interface ProxyNodeMutationResponse {
  node_id: string
  node: ProxyNode
}

export const proxyNodesApi = {
  async listProxyNodes(params?: { status?: string; skip?: number; limit?: number }): Promise<ProxyNodeListResponse> {
    const { data } = await apiClient.get<ProxyNodeListResponse>('/admin/proxy-nodes', { params })
    return data
  },

  async getProxyNode(nodeId: string): Promise<{ node: ProxyNode }> {
    const { data } = await apiClient.get<{ node: ProxyNode }>(`/admin/proxy-nodes/${encodeURIComponent(nodeId)}`)
    return data
  },

  async createManualNode(payload: ManualProxyNodeCreateRequest): Promise<ProxyNodeMutationResponse> {
    const { data } = await apiClient.post<ProxyNodeMutationResponse>('/admin/proxy-nodes/manual', payload)
    return data
  },

  async updateManualNode(nodeId: string, payload: ManualProxyNodeUpdateRequest): Promise<ProxyNodeMutationResponse> {
    const { data } = await apiClient.patch<ProxyNodeMutationResponse>(
      `/admin/proxy-nodes/${encodeURIComponent(nodeId)}`,
      payload,
    )
    return data
  },

  async deleteProxyNode(nodeId: string): Promise<{ message: string; node_id: string }> {
    const { data } = await apiClient.delete<{ message: string; node_id: string }>(
      `/admin/proxy-nodes/${encodeURIComponent(nodeId)}`,
    )
    return data
  },

  async testNode(nodeId: string): Promise<ProxyNodeTestResult> {
    const { data } = await apiClient.post<ProxyNodeTestResult>(
      `/admin/proxy-nodes/${encodeURIComponent(nodeId)}/test`,
    )
    return data
  },

  async testProxyUrl(payload: { proxy_url: string; username?: string; password?: string }): Promise<ProxyNodeTestResult> {
    const { data } = await apiClient.post<ProxyNodeTestResult>('/admin/proxy-nodes/test-url', payload)
    return data
  },
}

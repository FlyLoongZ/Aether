import client from './client'
import type { RoutingGroupConfig } from '@/features/routing/utils/routingPolicy'

export interface RoutingGroupRecord {
  id: string
  name: string
  description?: string | null
  enabled: boolean
  is_system_default: boolean
  sort_order: number
  config_json: RoutingGroupConfig
  version: number
  created_at: number
  updated_at: number
  published_at?: number | null
}

export interface RoutingGroupListResponse {
  items: RoutingGroupRecord[]
  total: number
}

export interface RoutingGroupCreateRequest {
  id?: string
  name: string
  description?: string | null
  enabled?: boolean
  is_system_default?: boolean
  sort_order?: number
  config_json?: RoutingGroupConfig
}

export interface RoutingGroupUpdateRequest {
  expected_version?: number
  name?: string
  description?: string | null
  enabled?: boolean
  is_system_default?: boolean
  sort_order?: number
  config_json?: RoutingGroupConfig
  version?: number
  published_at?: number | null
}

export async function listRoutingGroups(): Promise<RoutingGroupListResponse> {
  const response = await client.get<RoutingGroupListResponse>('/api/admin/routing/groups')
  return response.data
}

export async function createRoutingGroup(data: RoutingGroupCreateRequest): Promise<RoutingGroupRecord> {
  const response = await client.post<RoutingGroupRecord>('/api/admin/routing/groups', data)
  return response.data
}

export async function updateRoutingGroup(
  groupId: string,
  data: RoutingGroupUpdateRequest
): Promise<RoutingGroupRecord> {
  const response = await client.patch<RoutingGroupRecord>(`/api/admin/routing/groups/${groupId}`, data)
  return response.data
}

export async function deleteRoutingGroup(groupId: string): Promise<void> {
  await client.delete(`/api/admin/routing/groups/${groupId}`)
}

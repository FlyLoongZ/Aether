import type { CandidateRecord } from '@/api/requestTrace'

export const TIMELINE_STATUS: CandidateRecord['status'][] = [
  'success',
  'failed',
  'skipped',
  'cancelled',
  'pending',
  'streaming',
  'available',
  'unused',
  'stream_interrupted',
]

const PROVIDER_TYPE_LIKE_NAMES = new Set<string>([
  'codex',
  'kiro',
  'antigravity',
  'claude_code',
  'claude code',
  'gemini_cli',
  'gemini cli',
  'oauth',
  'api_key',
  'api key',
])

const toInt = (value: unknown, defaultValue = 0): number => {
  const num = Number(value)
  return Number.isFinite(num) ? Math.trunc(num) : defaultValue
}

export const makeAttemptKey = (candidateIndex: number, retryIndex: number): string => {
  return `${candidateIndex}:${retryIndex}`
}

export const isPoolParticipatedCandidate = (candidate: CandidateRecord): boolean => {
  return TIMELINE_STATUS.includes(candidate.status)
}

export const isAttemptedCandidate = (
  candidate: Pick<CandidateRecord, 'status' | 'started_at'>,
): boolean => {
  switch (candidate.status) {
    case 'streaming':
    case 'success':
    case 'failed':
    case 'cancelled':
    case 'stream_interrupted':
      return true
    case 'pending':
      return Boolean(candidate.started_at)
    case 'available':
    case 'unused':
    case 'skipped':
    default:
      return false
  }
}

export function buildPoolGroupVisibleAttempts(
  attempts: CandidateRecord[],
): CandidateRecord[] {
  return attempts.filter(isPoolParticipatedCandidate)
}

const toTimestampMs = (value: unknown): number | null => {
  if (typeof value !== 'string' || !value.trim()) return null
  const timestamp = new Date(value).getTime()
  return Number.isFinite(timestamp) ? timestamp : null
}

/**
 * Scheduling order is the ranking the scheduler assigned (candidate_index then
 * retry_index). It is always available, so it doubles as the stable fallback
 * for historical records that never captured any timing field.
 */
export const compareCandidatesBySchedulingOrder = (
  a: CandidateRecord,
  b: CandidateRecord,
): number => {
  if (a.candidate_index !== b.candidate_index) {
    return a.candidate_index - b.candidate_index
  }
  if (a.retry_index !== b.retry_index) {
    return a.retry_index - b.retry_index
  }
  const aCreatedAt = toTimestampMs(a.created_at) ?? 0
  const bCreatedAt = toTimestampMs(b.created_at) ?? 0
  return aCreatedAt - bCreatedAt
}

const resolveExecutionStartedAtMs = (candidate: CandidateRecord): number | null => {
  // Real execution wall-clock only; created_at is a scheduling-order fallback
  // and is applied later by compareCandidatesBySchedulingOrder.
  return toTimestampMs(candidate.started_at)
}

const toNonNegativeInt = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    return Math.trunc(value)
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value)
    if (Number.isFinite(parsed) && parsed >= 0) return Math.trunc(parsed)
  }
  return null
}

/**
 * Request-scoped monotonic index of an attempt that actually reached the
 * upstream, persisted by the gateway inside `extra_data`. It only exists for
 * HTTP candidate-loop attempts; WebSocket turns and historical records omit
 * it, so callers must always keep a started_at / scheduling fallback.
 */
export const candidateExecutionIndex = (
  candidate: Pick<CandidateRecord, 'extra_data'>,
): number | null => {
  const extra = candidate.extra_data
  if (!extra || typeof extra !== 'object' || Array.isArray(extra)) return null
  return toNonNegativeInt((extra as Record<string, unknown>).execution_index)
}

export type CandidateExecutionOrderMode = 'execution_index' | 'started_at'

/**
 * Pick one sort key for the whole candidate set so every pair is compared with
 * the same key. `execution_index` is only safe when every attempted record
 * carries a unique non-negative ordinal (the gateway guarantees this per
 * request); as soon as one is missing or duplicated (WebSocket turns,
 * historical rows) the set is incomplete and we fall back to started_at.
 * Mixing the two keys inside one pairwise comparator would be nontransitive.
 */
export const resolveCandidateExecutionOrderMode = (
  candidates: CandidateRecord[],
): CandidateExecutionOrderMode => {
  const seen = new Set<number>()
  let hasAttempted = false

  for (const candidate of candidates) {
    if (!isAttemptedCandidate(candidate)) continue
    hasAttempted = true
    const index = candidateExecutionIndex(candidate)
    if (index == null || seen.has(index)) return 'started_at'
    seen.add(index)
  }

  return hasAttempted ? 'execution_index' : 'started_at'
}

/**
 * Execution order for one fixed mode (lexicographic key, so it is a total
 * order). Attempts that actually reached a provider come first; within them the
 * mode's key wins (execution_index, else started_at), then scheduling metadata.
 * Candidates that were never attempted (skipped/available/unused/pending
 * without a start) stay in a separate trailing block so they cannot be falsely
 * interleaved into the execution sequence.
 */
export const compareCandidatesByExecutionOrder = (
  a: CandidateRecord,
  b: CandidateRecord,
  mode: CandidateExecutionOrderMode,
): number => {
  const aAttempted = isAttemptedCandidate(a)
  const bAttempted = isAttemptedCandidate(b)
  if (aAttempted !== bAttempted) {
    return aAttempted ? -1 : 1
  }
  if (aAttempted) {
    if (mode === 'execution_index') {
      const aIndex = candidateExecutionIndex(a)
      const bIndex = candidateExecutionIndex(b)
      if (aIndex !== bIndex) {
        if (aIndex == null) return 1
        if (bIndex == null) return -1
        return aIndex - bIndex
      }
    }
    const aMs = resolveExecutionStartedAtMs(a)
    const bMs = resolveExecutionStartedAtMs(b)
    if (aMs !== bMs) {
      if (aMs == null) return 1
      if (bMs == null) return -1
      return aMs - bMs
    }
  }
  return compareCandidatesBySchedulingOrder(a, b)
}

export const sortCandidatesByExecutionOrder = (
  candidates: CandidateRecord[],
  mode: CandidateExecutionOrderMode = resolveCandidateExecutionOrderMode(candidates),
): CandidateRecord[] => {
  return [...candidates].sort((a, b) => compareCandidatesByExecutionOrder(a, b, mode))
}

export const parseTimelineStatus = (value: unknown): CandidateRecord['status'] | null => {
  if (typeof value !== 'string') return null
  const normalized = value.trim().toLowerCase()
  if ((TIMELINE_STATUS as string[]).includes(normalized)) {
    return normalized as CandidateRecord['status']
  }
  return null
}

export const extractPoolGroupId = (
  candidate: Pick<CandidateRecord, 'extra_data' | 'provider_id'>,
): string | null => {
  const extra = candidate.extra_data
  if (!extra || typeof extra !== 'object' || Array.isArray(extra)) return null
  const raw = extra as Record<string, unknown>

  for (const key of ['pool_group_id', 'candidate_group_id']) {
    const value = raw[key]
    if (typeof value === 'string') {
      const text = value.trim()
      if (text) return text
    }
  }

  const routingTrace = raw.routing_trace
  if (routingTrace && typeof routingTrace === 'object' && !Array.isArray(routingTrace)) {
    const poolExpansion = (routingTrace as Record<string, unknown>).pool_expansion
    if (Array.isArray(poolExpansion)) {
      for (const item of poolExpansion) {
        if (!item || typeof item !== 'object' || Array.isArray(item)) continue
        const value = (item as Record<string, unknown>).pool_group_id
        if (typeof value !== 'string') continue
        const text = value.trim()
        if (text) return text
      }
    }
  }

  if (raw.pool_key_index !== undefined && raw.pool_key_index !== null) {
    const providerId = String(candidate.provider_id || '').trim()
    if (providerId) return providerId
  }

  return null
}

export function buildPoolParticipatedCandidates(
  rawTimeline: CandidateRecord[],
  attempts: unknown,
  requestId?: string | null,
): CandidateRecord[] {
  const fromTrace = rawTimeline.filter(
    candidate => extractPoolGroupId(candidate) !== null && isPoolParticipatedCandidate(candidate),
  )
  const fromAudit = buildPoolAttemptCandidatesFromAudit(rawTimeline, attempts, requestId)

  if (fromTrace.length === 0) return fromAudit
  if (fromAudit.length === 0) return fromTrace

  const traceKeys = new Set(
    fromTrace.map(candidate => makeAttemptKey(candidate.candidate_index, candidate.retry_index)),
  )
  const merged = [...fromTrace]
  for (const candidate of fromAudit) {
    const key = makeAttemptKey(candidate.candidate_index, candidate.retry_index)
    if (!traceKeys.has(key)) {
      merged.push(candidate)
    }
  }

  return merged.sort((a, b) => {
    if (a.candidate_index !== b.candidate_index) {
      return a.candidate_index - b.candidate_index
    }
    return a.retry_index - b.retry_index
  })
}

export function buildPoolAttemptCandidatesFromAudit(
  rawTimeline: CandidateRecord[],
  attempts: unknown,
  requestId?: string | null,
): CandidateRecord[] {
  if (!Array.isArray(attempts) || attempts.length === 0) return []

  const providerNameById = new Map<string, string>()
  for (const candidate of rawTimeline) {
    const providerId = String(candidate.provider_id || '').trim()
    const providerName = String(candidate.provider_name || '').trim()
    if (!providerId || !providerName) continue
    if (!providerNameById.has(providerId)) {
      providerNameById.set(providerId, providerName)
    }
  }

  const traceMap = new Map<string, CandidateRecord>()
  for (const candidate of rawTimeline) {
    traceMap.set(makeAttemptKey(candidate.candidate_index, candidate.retry_index), candidate)
  }

  return attempts
    .map((item, index) => {
      if (!item || typeof item !== 'object' || Array.isArray(item)) return null
      const raw = item as Record<string, unknown>
      const candidateIndex = toInt(raw.candidate_index, index)
      const retryIndex = toInt(raw.retry_index, 0)
      const key = makeAttemptKey(candidateIndex, retryIndex)
      const fromTrace = traceMap.get(key)
      const parsedStatus = parseTimelineStatus(raw.status)

      if (!fromTrace && parsedStatus === null) {
        return null
      }

      const merged: CandidateRecord = fromTrace
        ? { ...fromTrace }
        : {
            id: `pool-${requestId || 'unknown'}-${candidateIndex}-${retryIndex}-${index}`,
            request_id: requestId || '',
            candidate_index: candidateIndex,
            retry_index: retryIndex,
            provider_id: undefined,
            provider_name: undefined,
            endpoint_id: undefined,
            key_id: undefined,
            key_name: undefined,
            status: 'failed',
            is_cached: false,
            created_at: new Date(0).toISOString(),
          }

      if (parsedStatus !== null) {
        merged.status = parsedStatus
      }
      if (typeof raw.provider_id === 'string') merged.provider_id = raw.provider_id
      if (typeof raw.provider_name === 'string') merged.provider_name = raw.provider_name
      if (typeof raw.endpoint_id === 'string') merged.endpoint_id = raw.endpoint_id
      if (typeof raw.key_id === 'string') merged.key_id = raw.key_id
      if (typeof raw.key_name === 'string') merged.key_name = raw.key_name
      if (typeof raw.status_code === 'number') merged.status_code = raw.status_code
      if (typeof raw.error_type === 'string') merged.error_type = raw.error_type
      const rawPoolGroupId = typeof raw.pool_group_id === 'string' ? raw.pool_group_id.trim() : ''
      const fallbackPoolGroupId = typeof raw.provider_id === 'string' ? raw.provider_id.trim() : ''
      const finalPoolGroupId = rawPoolGroupId || fallbackPoolGroupId
      if (finalPoolGroupId) {
        merged.extra_data = {
          ...(merged.extra_data || {}),
          pool_group_id: finalPoolGroupId,
        }
      }

      const mergedProviderId = String(merged.provider_id || '').trim()
      if (mergedProviderId) {
        const inferredProviderName = providerNameById.get(mergedProviderId)
        const currentProviderName = String(merged.provider_name || '').trim()
        if (
          inferredProviderName
          && (
            !currentProviderName
            || PROVIDER_TYPE_LIKE_NAMES.has(currentProviderName.toLowerCase())
          )
        ) {
          merged.provider_name = inferredProviderName
        }
      }

      return isPoolParticipatedCandidate(merged) ? merged : null
    })
    .filter((item): item is CandidateRecord => item !== null)
}

export interface CandidateExecutionPartition {
  partition: 'executed' | 'unattempted'
  identity: string
  isPool: boolean
  candidates: CandidateRecord[]
}

const providerIdentityOf = (
  candidate: Pick<CandidateRecord, 'provider_id' | 'provider_name'>,
): string => {
  const providerId = String(candidate.provider_id || '').trim()
  if (providerId) return `provider_id:${providerId}`
  const providerName = String(candidate.provider_name || '').trim().toLowerCase()
  return `provider_name:${providerName || '未知'}`
}

/**
 * Build CONTIGUOUS groups from the executed order itself, not from scheduling
 * order. The whole combined attempt list is sorted once (single mode), then two
 * neighbours merge only when they share the same partition (executed vs
 * unattempted) and the same identity (pool id when present, otherwise provider
 * identity). Because grouping walks the executed list, a re-attempted provider
 * that is interrupted by another provider/pool stays a separate node instead of
 * collapsing across the gap and inverting the real execution order.
 */
export const groupCandidatesByExecutionContiguity = (
  candidates: CandidateRecord[],
  mode: CandidateExecutionOrderMode = resolveCandidateExecutionOrderMode(candidates),
): CandidateExecutionPartition[] => {
  const ordered = sortCandidatesByExecutionOrder(candidates, mode)
  const groups: CandidateExecutionPartition[] = []
  let current: CandidateExecutionPartition | null = null

  for (const candidate of ordered) {
    const partition = isAttemptedCandidate(candidate) ? 'executed' : 'unattempted'
    const poolId = extractPoolGroupId(candidate)
    const isPool = poolId != null
    const identity = isPool ? `pool:${poolId}` : providerIdentityOf(candidate)

    if (current && current.partition === partition && current.identity === identity) {
      current.candidates.push(candidate)
      continue
    }

    current = { partition, identity, isPool, candidates: [candidate] }
    groups.push(current)
  }

  return groups
}

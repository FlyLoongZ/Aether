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

const resolveExecutionSortMs = (candidate: CandidateRecord): number | null => {
  // Actual execution time first; created_at is only a stable fallback for
  // historical records written before started_at was captured.
  return toTimestampMs(candidate.started_at) ?? toTimestampMs(candidate.created_at)
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

/**
 * Execution order: attempts that actually reached a provider come first, in
 * ascending started_at order. When the gateway recorded a real
 * `execution_index` it wins over wall-clock started_at; otherwise we fall back
 * to started_at and finally to scheduling metadata. Candidates that were never
 * attempted (skipped/available/unused/pending without a start) stay in a
 * separate trailing block so they cannot be falsely interleaved into the
 * execution sequence.
 */
export const compareCandidatesByExecutionOrder = (
  a: CandidateRecord,
  b: CandidateRecord,
): number => {
  const aAttempted = isAttemptedCandidate(a)
  const bAttempted = isAttemptedCandidate(b)
  if (aAttempted !== bAttempted) {
    return aAttempted ? -1 : 1
  }
  if (aAttempted) {
    const aIndex = candidateExecutionIndex(a)
    const bIndex = candidateExecutionIndex(b)
    if (aIndex != null && bIndex != null && aIndex !== bIndex) {
      return aIndex - bIndex
    }
    const aMs = resolveExecutionSortMs(a)
    const bMs = resolveExecutionSortMs(b)
    if (aMs != null && bMs != null && aMs !== bMs) {
      return aMs - bMs
    }
    if (aMs != null && bMs == null) return -1
    if (aMs == null && bMs != null) return 1
  }
  return compareCandidatesBySchedulingOrder(a, b)
}

export const sortCandidatesByExecutionOrder = (
  candidates: CandidateRecord[],
): CandidateRecord[] => {
  return [...candidates].sort(compareCandidatesByExecutionOrder)
}

export interface CandidateExecutionGroupKey {
  hasAttempted: boolean
  executionIndex: number | null
  startedAtMs: number | null
  startIndex: number
  endIndex: number
}

/**
 * Groups are still assembled from scheduling order (consecutive same-provider
 * runs), but their placement on the track follows the earliest real execution
 * time of any attempted member. Fully unstarted groups sort after every
 * attempted group and fall back to their scheduling position.
 */
export const resolveCandidateExecutionGroupKey = (
  candidates: CandidateRecord[],
  startIndex: number,
  endIndex: number,
): CandidateExecutionGroupKey => {
  let hasAttempted = false
  let executionIndex: number | null = null
  let startedAtMs: number | null = null

  for (const candidate of candidates) {
    if (!isAttemptedCandidate(candidate)) continue
    hasAttempted = true
    const index = candidateExecutionIndex(candidate)
    if (index != null && (executionIndex == null || index < executionIndex)) {
      executionIndex = index
    }
    const ms = toTimestampMs(candidate.started_at)
    if (ms == null) continue
    if (startedAtMs == null || ms < startedAtMs) {
      startedAtMs = ms
    }
  }

  return { hasAttempted, executionIndex, startedAtMs, startIndex, endIndex }
}

export const compareCandidateExecutionGroupKeys = (
  a: CandidateExecutionGroupKey,
  b: CandidateExecutionGroupKey,
): number => {
  if (a.hasAttempted !== b.hasAttempted) {
    return a.hasAttempted ? -1 : 1
  }
  if (a.executionIndex != null && b.executionIndex != null && a.executionIndex !== b.executionIndex) {
    return a.executionIndex - b.executionIndex
  }
  if (a.startedAtMs != null && b.startedAtMs != null && a.startedAtMs !== b.startedAtMs) {
    return a.startedAtMs - b.startedAtMs
  }
  if (a.startedAtMs != null && b.startedAtMs == null) return -1
  if (a.startedAtMs == null && b.startedAtMs != null) return 1
  if (a.startIndex !== b.startIndex) return a.startIndex - b.startIndex
  return a.endIndex - b.endIndex
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

import { describe, expect, it } from 'vitest'

import type { CandidateRecord } from '@/api/requestTrace'
import {
  buildPoolAttemptCandidatesFromAudit,
  buildPoolGroupVisibleAttempts,
  buildPoolParticipatedCandidates,
  compareCandidatesByExecutionOrder,
  compareCandidateExecutionGroupKeys,
  isAttemptedCandidate,
  resolveCandidateExecutionGroupKey,
  resolveCandidateExecutionOrderMode,
  sortCandidatesByExecutionOrder,
} from '@/features/usage/utils/poolTrace'

function buildCandidate(
  overrides: Partial<CandidateRecord> = {},
): CandidateRecord {
  return {
    id: 'cand-1',
    request_id: 'req-1',
    candidate_index: 0,
    retry_index: 0,
    status: 'failed',
    is_cached: false,
    created_at: '1970-01-01T00:00:00.000Z',
    ...overrides,
  }
}

describe('poolTrace', () => {
  it('keeps pool audit nodes even when they were not attempted', () => {
    const attempts = buildPoolAttemptCandidatesFromAudit([], [
      {
        candidate_index: 0,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-success',
        key_name: 'Success Key',
        status: 'success',
        pool_group_id: 'provider-1',
      },
      {
        candidate_index: 1,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-skipped',
        key_name: 'Skipped Key',
        status: 'skipped',
        pool_group_id: 'provider-1',
      },
      {
        candidate_index: 2,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-available',
        key_name: 'Available Key',
        status: 'available',
        pool_group_id: 'provider-1',
      },
      {
        candidate_index: 3,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-unknown',
        key_name: 'Unknown Key',
        status: 'selected',
        pool_group_id: 'provider-1',
      },
    ], 'req-1')

    expect(attempts).toHaveLength(3)
    expect(attempts[0].key_id).toBe('key-success')
    expect(attempts[0].status).toBe('success')
    expect(attempts[1].key_id).toBe('key-skipped')
    expect(attempts[1].status).toBe('skipped')
    expect(attempts[2].key_id).toBe('key-available')
    expect(attempts[2].status).toBe('available')
  })

  it('preserves real trace attempts even when audit status is non-standard', () => {
    const rawTimeline = [
      buildCandidate({
        id: 'cand-trace-1',
        candidate_index: 4,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-trace',
        key_name: 'Trace Key',
        status: 'failed',
        started_at: '2026-04-19T12:00:00.000Z',
      }),
    ]

    const attempts = buildPoolAttemptCandidatesFromAudit(rawTimeline, [
      {
        candidate_index: 4,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'oauth',
        key_id: 'key-trace',
        key_name: 'Trace Key',
        status: 'selected',
        pool_group_id: 'provider-1',
      },
      {
        candidate_index: 5,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'oauth',
        key_id: 'key-ghost',
        key_name: 'Ghost Key',
        status: 'selected',
        pool_group_id: 'provider-1',
      },
    ], 'req-1')

    expect(attempts).toHaveLength(1)
    expect(attempts[0].id).toBe('cand-trace-1')
    expect(attempts[0].status).toBe('failed')
    expect(attempts[0].provider_name).toBe('Codex反代')
  })

  it('merges audit-only skipped pool nodes when trace only carries partial pool metadata', () => {
    const rawTimeline = [
      buildCandidate({
        id: 'cand-success',
        candidate_index: 1,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-success',
        key_name: 'Success Key',
        status: 'success',
        extra_data: { pool_group_id: 'provider-1' },
        started_at: '2026-04-19T12:00:00.000Z',
      }),
      buildCandidate({
        id: 'cand-skipped',
        candidate_index: 2,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-skipped',
        key_name: 'Skipped Key',
        status: 'skipped',
      }),
    ]

    const attempts = buildPoolParticipatedCandidates(rawTimeline, [
      {
        candidate_index: 1,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-success',
        key_name: 'Success Key',
        status: 'success',
        pool_group_id: 'provider-1',
      },
      {
        candidate_index: 2,
        retry_index: 0,
        provider_id: 'provider-1',
        provider_name: 'Codex反代',
        key_id: 'key-skipped',
        key_name: 'Skipped Key',
        status: 'skipped',
        pool_group_id: 'provider-1',
      },
    ], 'req-1')

    expect(attempts).toHaveLength(2)
    expect(attempts.map(item => item.key_id)).toEqual(['key-success', 'key-skipped'])
    expect(attempts[1].extra_data?.pool_group_id).toBe('provider-1')
  })

  it('infers pool membership from runtime pool key metadata', () => {
    const attempts = buildPoolParticipatedCandidates([
      buildCandidate({
        id: 'cand-pool-skipped',
        candidate_index: 0,
        provider_id: 'provider-1',
        provider_name: 'CodexFree2',
        key_id: 'pool-group',
        key_name: 'CodexFree2',
        status: 'skipped',
        extra_data: { pool_group_id: 'provider-1' },
      }),
      buildCandidate({
        id: 'cand-pool-success',
        candidate_index: 1,
        provider_id: 'provider-1',
        provider_name: 'CodexFree2',
        key_id: 'key-success',
        key_name: 'Success Key',
        status: 'success',
        extra_data: { pool_key_index: 0 },
      }),
    ], null, 'req-1')

    expect(attempts).toHaveLength(2)
    expect(attempts.map(item => item.key_id)).toEqual(['pool-group', 'key-success'])
  })

  it('treats only real execution statuses as attempted', () => {
    expect(isAttemptedCandidate(buildCandidate({ status: 'success' }))).toBe(true)
    expect(isAttemptedCandidate(buildCandidate({ status: 'failed' }))).toBe(true)
    expect(isAttemptedCandidate(buildCandidate({ status: 'cancelled' }))).toBe(true)
    expect(isAttemptedCandidate(buildCandidate({ status: 'streaming' }))).toBe(true)
    expect(isAttemptedCandidate(buildCandidate({ status: 'stream_interrupted' }))).toBe(true)
    expect(isAttemptedCandidate(buildCandidate({ status: 'pending' }))).toBe(false)
    expect(isAttemptedCandidate(buildCandidate({
      status: 'pending',
      started_at: '2026-04-24T12:00:00.000Z',
    }))).toBe(true)
    expect(isAttemptedCandidate(buildCandidate({ status: 'skipped' }))).toBe(false)
    expect(isAttemptedCandidate(buildCandidate({ status: 'available' }))).toBe(false)
    expect(isAttemptedCandidate(buildCandidate({ status: 'unused' }))).toBe(false)
  })

  it('orders attempted candidates by started_at regardless of scheduling rank', () => {
    const rank0Late = buildCandidate({
      id: 'rank-0',
      candidate_index: 0,
      status: 'failed',
      started_at: '2026-05-06T12:00:05.000Z',
    })
    const rank1Early = buildCandidate({
      id: 'rank-1',
      candidate_index: 1,
      status: 'success',
      started_at: '2026-05-06T12:00:01.000Z',
    })

    expect(compareCandidatesByExecutionOrder(rank0Late, rank1Early, 'started_at')).toBeGreaterThan(0)
    expect(sortCandidatesByExecutionOrder([rank0Late, rank1Early], 'started_at').map(item => item.id))
      .toEqual(['rank-1', 'rank-0'])
  })

  it('prefers the persisted execution_index over started_at when both are present', () => {
    const startedEarlierButExecutedSecond = buildCandidate({
      id: 'started-earlier',
      candidate_index: 0,
      status: 'failed',
      started_at: '2026-05-06T12:00:00.000Z',
      extra_data: { execution_index: 1 },
    })
    const startedLaterButExecutedFirst = buildCandidate({
      id: 'started-later',
      candidate_index: 1,
      status: 'success',
      started_at: '2026-05-06T12:00:09.000Z',
      extra_data: { execution_index: 0 },
    })

    expect(sortCandidatesByExecutionOrder([
      startedEarlierButExecutedSecond,
      startedLaterButExecutedFirst,
    ]).map(item => item.id)).toEqual(['started-later', 'started-earlier'])
  })

  it('falls back to started_at when execution_index is absent', () => {
    const noIndexLate = buildCandidate({
      id: 'no-index-late',
      candidate_index: 0,
      status: 'failed',
      started_at: '2026-05-06T12:00:05.000Z',
    })
    const noIndexEarly = buildCandidate({
      id: 'no-index-early',
      candidate_index: 1,
      status: 'success',
      started_at: '2026-05-06T12:00:01.000Z',
    })

    expect(sortCandidatesByExecutionOrder([noIndexLate, noIndexEarly]).map(item => item.id))
      .toEqual(['no-index-early', 'no-index-late'])
  })

  it('keeps unstarted candidates after attempted ones in scheduling order', () => {
    const attempted = buildCandidate({
      id: 'attempted',
      candidate_index: 3,
      status: 'failed',
      started_at: '2026-05-06T12:00:00.000Z',
    })
    const skipped = buildCandidate({ id: 'skipped', candidate_index: 0, status: 'skipped' })
    const available = buildCandidate({ id: 'available', candidate_index: 1, status: 'available' })
    const pending = buildCandidate({ id: 'pending', candidate_index: 2, status: 'pending' })

    expect(sortCandidatesByExecutionOrder([skipped, pending, available, attempted]).map(item => item.id))
      .toEqual(['attempted', 'skipped', 'available', 'pending'])
  })

  it('falls back to scheduling order when every attempted candidate is missing started_at', () => {
    const second = buildCandidate({ id: 'second', candidate_index: 1, status: 'failed', started_at: undefined })
    const first = buildCandidate({ id: 'first', candidate_index: 0, status: 'failed', started_at: undefined })

    expect(sortCandidatesByExecutionOrder([second, first]).map(item => item.id))
      .toEqual(['first', 'second'])
  })

  it('sorts execution group keys by earliest started_at and puts fully unstarted groups last', () => {
    const earlier = buildCandidate({
      id: 'earlier',
      candidate_index: 7,
      status: 'success',
      started_at: '2026-05-06T12:00:01.000Z',
    })
    const later = buildCandidate({
      id: 'later',
      candidate_index: 5,
      status: 'failed',
      started_at: '2026-05-06T12:00:05.000Z',
    })
    const unstarted = buildCandidate({ id: 'unstarted', candidate_index: 1, status: 'skipped' })

    const groups = [
      { id: 'unstarted', key: resolveCandidateExecutionGroupKey([unstarted], 1, 1) },
      { id: 'later', key: resolveCandidateExecutionGroupKey([later], 5, 5) },
      { id: 'earlier', key: resolveCandidateExecutionGroupKey([earlier], 7, 7) },
    ]
    groups.sort((a, b) => compareCandidateExecutionGroupKeys(a.key, b.key, 'started_at'))

    expect(groups.map(group => group.id)).toEqual(['earlier', 'later', 'unstarted'])
  })

  it('selects execution_index only when every attempted record has a unique index', () => {
    const indexed = (id: string, executionIndex: number) => buildCandidate({
      id,
      candidate_index: executionIndex,
      status: 'failed',
      extra_data: { execution_index: executionIndex },
    })

    expect(resolveCandidateExecutionOrderMode([indexed('a', 0), indexed('b', 1)])).toBe('execution_index')
    // A missing WebSocket/historical index makes the whole set incomplete.
    expect(resolveCandidateExecutionOrderMode([
      indexed('a', 0),
      buildCandidate({ id: 'ws', candidate_index: 1, status: 'failed' }),
    ])).toBe('started_at')
    // Duplicate ordinals are not consistent enough to sort by.
    expect(resolveCandidateExecutionOrderMode([indexed('a', 0), indexed('b', 0)])).toBe('started_at')
    // Untried candidates do not affect the decision.
    expect(resolveCandidateExecutionOrderMode([
      indexed('a', 0),
      buildCandidate({ id: 'available', candidate_index: 1, status: 'available' }),
    ])).toBe('execution_index')
  })

  it('avoids a nontransitive mixed sort by falling back to started_at for an incomplete set', () => {
    // Naively mixing keys gives A < B < C but C < A, which corrupts Array.sort.
    const a = buildCandidate({
      id: 'a',
      candidate_index: 0,
      status: 'failed',
      started_at: '2026-05-06T12:00:10.000Z',
      extra_data: { execution_index: 5 },
    })
    const b = buildCandidate({
      id: 'b',
      candidate_index: 1,
      status: 'failed',
      started_at: '2026-05-06T12:00:50.000Z',
    })
    const c = buildCandidate({
      id: 'c',
      candidate_index: 2,
      status: 'failed',
      started_at: '2026-05-06T12:00:90.000Z',
      extra_data: { execution_index: 2 },
    })

    expect(resolveCandidateExecutionOrderMode([a, b, c])).toBe('started_at')
    expect(sortCandidatesByExecutionOrder([c, a, b]).map(item => item.id)).toEqual(['a', 'b', 'c'])
  })

  it('orders execution-indexed groups by ordinal even when started_at disagrees', () => {
    const laterStartEarlierOrdinal = buildCandidate({
      id: 'ordinal-0',
      candidate_index: 1,
      status: 'success',
      started_at: '2026-05-06T12:00:09.000Z',
      extra_data: { execution_index: 0 },
    })
    const earlierStartLaterOrdinal = buildCandidate({
      id: 'ordinal-1',
      candidate_index: 0,
      status: 'failed',
      started_at: '2026-05-06T12:00:00.000Z',
      extra_data: { execution_index: 1 },
    })

    const groups = [
      { id: 'later-ordinal', key: resolveCandidateExecutionGroupKey([earlierStartLaterOrdinal], 0, 0) },
      { id: 'earlier-ordinal', key: resolveCandidateExecutionGroupKey([laterStartEarlierOrdinal], 1, 1) },
    ]
    groups.sort((x, y) => compareCandidateExecutionGroupKeys(x.key, y.key, 'execution_index'))

    expect(groups.map(group => group.id)).toEqual(['earlier-ordinal', 'later-ordinal'])
  })

  it('treats a mixed group as attempted using its earliest started_at', () => {
    const skipped = buildCandidate({ id: 'skipped', candidate_index: 0, status: 'skipped' })
    const success = buildCandidate({
      id: 'success',
      candidate_index: 1,
      status: 'success',
      started_at: '2026-05-06T12:00:02.000Z',
    })

    const key = resolveCandidateExecutionGroupKey([skipped, success], 0, 1)
    expect(key.hasAttempted).toBe(true)
    expect(key.startedAtMs).toBe(new Date('2026-05-06T12:00:02.000Z').getTime())
  })

  it('keeps skipped pool children visible when attempted nodes exist', () => {
    const attempts = buildPoolGroupVisibleAttempts([
      buildCandidate({
        id: 'cand-skipped',
        candidate_index: 0,
        status: 'skipped',
      }),
      buildCandidate({
        id: 'cand-failed',
        candidate_index: 1,
        status: 'failed',
        started_at: '2026-04-24T12:00:00.000Z',
      }),
      buildCandidate({
        id: 'cand-success',
        candidate_index: 2,
        status: 'success',
        started_at: '2026-04-24T12:00:01.000Z',
      }),
    ])

    expect(attempts.map(item => item.id)).toEqual(['cand-skipped', 'cand-failed', 'cand-success'])
  })

  it('keeps all skipped pool children visible', () => {
    const attempts = buildPoolGroupVisibleAttempts([
      buildCandidate({
        id: 'cand-skipped-1',
        candidate_index: 0,
        status: 'skipped',
      }),
      buildCandidate({
        id: 'cand-skipped-2',
        candidate_index: 1,
        status: 'skipped',
      }),
    ])

    expect(attempts.map(item => item.id)).toEqual(['cand-skipped-1', 'cand-skipped-2'])
  })
})

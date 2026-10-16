# Failover Rules

Provider and endpoint configuration can carry a `failover_rules` object that
decides how the gateway reacts to an upstream failure: whether it retries
another candidate, which credential/endpoint scope the failure applies to, and
what the client finally receives once every candidate is exhausted.

```jsonc
{
  "failover_rules": {
    "stop_on_status_codes": [400, 403],
    "continue_on_status_codes": [429],
    "stop_on_transport_errors": false,
    "success_failover_patterns": [
      { "pattern": "capacity.*exhausted", "status_codes": [200] }
    ],
    "error_stop_patterns": [
      { "pattern": "content_policy_violation", "status_codes": [400, 403] }
    ],
    "max_retries": 2,
    "max_transfer_count": 4,
    "max_transfer_timeout_seconds": 60,
    "preserve_last_upstream_error_on_exhaustion": true
  }
}
```

## Status-code rules

| Key | Accepted aliases | Meaning |
| --- | --- | --- |
| `stop_on_status_codes` | `early_stop_status_codes`, `non_retryable_status_codes`, `stop_status_codes` | Stop immediately, do not try another candidate |
| `continue_on_status_codes` | `retryable_status_codes`, `retry_on_status_codes`, `continue_status_codes` | Try the next candidate |

Any other status falls back to the protocol default: `status >= 500` is
retryable, and `status >= 400` is retryable unless the endpoint's API format
disables client-error failover by default (`openai:search` is the only such
format today). `claude:messages` additionally applies an Anthropic-specific
table that keeps `400`/`413` non-retryable while rotating credentials for
`401`/`403`/`429` and moving endpoints for `404`/`529`/`5xx`.

## Pattern rules

`success_failover_patterns` matches a `200` response whose body actually
describes a failure; the gateway then retries another candidate.
`error_stop_patterns` stops failover for a matching error body. Both accept an
optional `status_codes` list; an empty list means "any status". A rule with an
empty `pattern` matches purely on the status list.

`stop_cyber_policy_errors` (default `true`) stops failover when an error body
carries a `cyber_policy` / `cyber_policy_violation` code.

## Transport failures

`stop_on_transport_errors` (default `false`) decides whether a failure that
never produced an upstream HTTP response (connect/TLS/proxy/read timeout, stream
reset, protocol error) stops failover or moves to the next candidate.

When such a failure does reach the client, the status is derived from the
failure class: timeouts return `504`, everything else `502`.

## Exhaustion behavior

`preserve_last_upstream_error_on_exhaustion` (default `true`) controls what the
client receives once every candidate has failed.

| Value | Client receives |
| --- | --- |
| `true` (default) | The last eligible upstream error: the upstream status code, the upstream error body (rebuilt for cross-format plans), and the upstream response headers. When the body is rebuilt, `content-type` / `content-encoding` / `content-length` are realigned to the bytes actually sent; endpoint rules and the shared client safety filter then apply as they do for successful responses |
| `false` | A gateway-synthesized error, historically `503` |

An eligible failure is an upstream HTTP response with `status >= 400`. A `2xx`
response that only matched `success_failover_patterns` is not eligible, because
delivering it under a failure status would look like success to the client. A
`2xx` stream that carries an explicit error event has no upstream error status;
it is returned with the derived error status and a client-format error body.

A transport failure has no upstream response to keep, so it is never preserved:
one that stops failover is projected to `502`/`504` as described above, and one
that merely exhausts the candidates still ends in the synthesized `503`.

This switch only gates the exhaustion path. Explicit stop rules keep their
existing passthrough behavior either way, so a deliberate
`stop_on_status_codes` / `error_stop_patterns` decision is never affected by
turning the switch off.

## Budgets

`max_retries` caps attempts per candidate; `max_transfer_count` and
`max_transfer_timeout_seconds` cap how far a single request may chase other
candidates. Values of `0` mean "no explicit budget".

## Observability

Each failed candidate is recorded separately, so the request detail view can
show the per-candidate upstream response side by side with the response the
client actually received. The terminal usage row records both sides as well:

| Field | Contents |
| --- | --- |
| `response_headers` / `response_body` | The preserved upstream response |
| `client_response_headers` | The header set the gateway assembled for the client on a preserved upstream error (upstream retentions plus endpoint rules); gateway-internal control headers are not part of it |
| `client_response_body` | What the gateway actually sent |
| `status_code` | The real upstream status code, even when the outer transport replied `200` |

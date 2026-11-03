-- One request's captured bodies.
--
-- Each table holds a single exchange, so the row is the whole answer: no slot
-- resolution, and a request whose headers row is already retired still yields
-- its client bodies.
SELECT
  usage_http_bodies.client_request,
  usage_http_bodies.client_request_encoding,
  usage_http_bodies.client_request_truncated,
  usage_http_bodies.provider_request,
  usage_http_bodies.provider_request_encoding,
  usage_http_bodies.provider_request_truncated,
  usage_http_bodies.provider_response,
  usage_http_bodies.provider_response_encoding,
  usage_http_bodies.provider_response_truncated,
  usage_http_bodies.client_response,
  usage_http_bodies.client_response_encoding,
  usage_http_bodies.client_response_truncated
FROM usage_http_bodies
WHERE usage_http_bodies.request_id = $1

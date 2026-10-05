# Usage header capture

Usage HTTP captures preserve original header values for client requests,
provider requests, provider responses, and client responses. Header maps are
validated as JSON objects but their values are not redacted. Capture settings
and existing access controls still apply.

These records can contain credentials such as Authorization, API keys, and
cookies, as well as session identifiers and client metadata. Restrict access
to usage details, database records, exports, and backups accordingly.

Previously stored `[redacted]` values cannot be recovered. Original values
are available only for new captures after deploying this change.

The request detail drawer reads these values directly from the administrator
usage-detail API, including when bodies are not loaded. No frontend setting
can recover header values that were already replaced during capture.

## Display masking

The usage detail drawer masks sensitive header names by default
(`authorization`, `proxy-authorization`, `x-api-key`, `api-key`, `cookie`,
`set-cookie`) as `****`. Clicking a mask reveals that one header in place, in
the compare, formatted, and raw views alike. The copy button is unaffected and
still copies the API values verbatim.

This masking is presentational only. The API response and the database still
contain the original values, so anyone with admin access can read them
through the browser network panel, the API directly, exports, or backups.
Treat the drawer masking as accident prevention, not as an access boundary;
restrict administrator access, exports, and backups accordingly.

//! User-facing detail text for the stateless-policy rejections.
//!
//! Two lanes reject the same server-side state fields: the public HTTP
//! endpoints (`handlers/public/ai_public.rs`) and the Responses WebSocket
//! relay (`handlers/proxy/websocket/responses`). The text lives here so a
//! wording change cannot land on one lane only — that is exactly how the
//! HTTP copy briefly told clients to "omit store" while the WebSocket copy
//! still did.

/// A stateless lane never owns server-side response storage, so the field can
/// only be `false` (or absent, which the upstream interprets on its own).
pub(crate) const STORE_TRUE_NOT_SUPPORTED_DETAIL: &str =
    "store=true is not supported; use store=false";

/// Background responses need server-side polling state this gateway does not own.
pub(crate) const BACKGROUND_TRUE_NOT_SUPPORTED_DETAIL: &str = "background=true is not supported";

/// Continuation by conversation id needs server-side conversation state.
pub(crate) const CONVERSATION_NOT_SUPPORTED_DETAIL: &str = "conversation is not supported";

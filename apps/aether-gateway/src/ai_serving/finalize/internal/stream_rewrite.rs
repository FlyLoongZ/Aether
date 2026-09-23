use serde_json::Value;
use std::sync::Arc;

use crate::ai_serving::{
    maybe_build_ai_surface_stream_rewriter, maybe_build_ai_surface_stream_rewriter_shared,
    AiSurfaceFinalizeError, AiSurfaceStreamRewriter, ResponseHistoryRecord,
};
use crate::GatewayError;

pub(crate) struct LocalStreamRewriter {
    inner: AiSurfaceStreamRewriter,
}

pub(crate) fn maybe_build_local_stream_rewriter(
    report_context: Option<&Value>,
) -> Option<LocalStreamRewriter> {
    maybe_build_ai_surface_stream_rewriter(report_context)
        .map(|inner| LocalStreamRewriter { inner })
}

pub(crate) fn maybe_build_local_stream_rewriter_shared(
    report_context: Arc<Value>,
) -> Option<LocalStreamRewriter> {
    maybe_build_ai_surface_stream_rewriter_shared(report_context)
        .map(|inner| LocalStreamRewriter { inner })
}

impl LocalStreamRewriter {
    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<u8>, GatewayError> {
        self.inner.push_chunk(chunk).map_err(map_surface_error)
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<u8>, GatewayError> {
        self.inner.finish().map_err(map_surface_error)
    }

    pub(crate) fn take_response_history_record(&mut self) -> Option<ResponseHistoryRecord> {
        self.inner.take_response_history_record()
    }
}

fn map_surface_error(error: AiSurfaceFinalizeError) -> GatewayError {
    error.into()
}

#[cfg(test)]
#[path = "../tests_stream.rs"]
mod tests;

//! Hook the usage runtime uses to turn the file-store switch back off.
//!
//! The switch lives in the gateway's system config while the store path lives in
//! the process environment, so the runtime cannot persist the correction itself.
//! The gateway registers a callback that writes `body_file_store_enabled=false`.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, RwLock};

type DisableFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
type DisableFn = Arc<dyn Fn() -> DisableFuture + Send + Sync>;

static DISABLER: LazyLock<RwLock<Option<DisableFn>>> = LazyLock::new(|| RwLock::new(None));

/// Registers the callback that persists `body_file_store_enabled=false`.
pub fn configure_body_file_store_switch_disabler(disabler: DisableFn) {
    *DISABLER
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(disabler);
}

/// Runs the registered callback, if any.
pub(crate) async fn disable_body_file_store_switch() -> Option<Result<(), String>> {
    let disabler = DISABLER
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()?;
    Some((disabler)().await)
}

//! In-memory audit log write repository.
//!
//! Used by tests and by deployments that run without a database: the gateway
//! still records security-relevant admin events instead of dropping them.

use std::sync::{Arc, Mutex};

use aether_data_contracts::repository::audit::{
    AuditLogWriteRepository, NewAdminAuditLog, StoredAdminAuditLog,
};
use aether_data_contracts::DataLayerError;
use async_trait::async_trait;

#[derive(Debug, Default)]
pub struct InMemoryAuditLogWriteRepository {
    entries: Arc<Mutex<Vec<StoredAdminAuditLog>>>,
}

impl InMemoryAuditLogWriteRepository {
    pub fn entries(&self) -> Vec<StoredAdminAuditLog> {
        self.entries.lock().expect("audit log store lock").clone()
    }
}

#[async_trait]
impl AuditLogWriteRepository for InMemoryAuditLogWriteRepository {
    async fn insert_admin_audit_log(&self, log: NewAdminAuditLog) -> Result<(), DataLayerError> {
        let mut entries = self.entries.lock().expect("audit log store lock");
        if entries.iter().any(|entry| entry.id == log.id) {
            // Same idempotency contract as the PostgreSQL implementation.
            return Ok(());
        }
        entries.push(StoredAdminAuditLog {
            id: log.id,
            event_type: log.event_type,
            user_id: log.user_id,
            user_email: None,
            user_username: None,
            description: Some(log.description),
            ip_address: log.ip_address,
            status_code: log.status_code,
            error_message: log.error_message,
            metadata: log.metadata,
            created_at_unix_secs: log.created_at_unix_secs,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryAuditLogWriteRepository;
    use aether_data_contracts::repository::audit::{AuditLogWriteRepository, NewAdminAuditLog};

    fn event(id: &str) -> NewAdminAuditLog {
        NewAdminAuditLog {
            id: id.to_string(),
            event_type: "admin_mutation_completed".to_string(),
            user_id: Some("admin-1".to_string()),
            description: "delete_user user 42".to_string(),
            ip_address: Some("203.0.113.7".to_string()),
            request_id: Some("trace-1".to_string()),
            status_code: Some(200),
            error_message: None,
            metadata: None,
            created_at_unix_secs: 1_710_000_000,
        }
    }

    #[tokio::test]
    async fn insert_is_idempotent_on_id() {
        let repository = InMemoryAuditLogWriteRepository::default();
        repository
            .insert_admin_audit_log(event("audit-1"))
            .await
            .expect("insert");
        repository
            .insert_admin_audit_log(event("audit-1"))
            .await
            .expect("retry");

        let entries = repository.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_type, "admin_mutation_completed");
        assert_eq!(entries[0].user_id.as_deref(), Some("admin-1"));
    }
}

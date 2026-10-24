use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;

use crate::DataLayerError;
use aether_data_contracts::repository::proxy_nodes::{
    new_proxy_node_generation, ProxyNodeManualCreateMutation, ProxyNodeManualUpdateMutation,
    ProxyNodeReadRepository, ProxyNodeTrafficMutation, ProxyNodeWriteRepository, StoredProxyNode,
};

#[derive(Debug, Default)]
pub struct InMemoryProxyNodeRepository {
    nodes: RwLock<BTreeMap<String, StoredProxyNode>>,
}

impl InMemoryProxyNodeRepository {
    pub fn seed<I>(nodes: I) -> Self
    where
        I: IntoIterator<Item = StoredProxyNode>,
    {
        Self {
            nodes: RwLock::new(
                nodes
                    .into_iter()
                    .map(|node| (node.id.clone(), node))
                    .collect(),
            ),
        }
    }

    fn now_unix_secs() -> Option<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
    }
}

#[async_trait]
impl ProxyNodeReadRepository for InMemoryProxyNodeRepository {
    async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, DataLayerError> {
        Ok(self
            .nodes
            .read()
            .expect("proxy nodes lock")
            .values()
            .cloned()
            .collect())
    }

    async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        Ok(self
            .nodes
            .read()
            .expect("proxy nodes lock")
            .get(node_id)
            .cloned())
    }
}

#[async_trait]
impl ProxyNodeWriteRepository for InMemoryProxyNodeRepository {
    async fn compare_and_set_proxy_password(
        &self,
        node_id: &str,
        expected: &str,
        replacement: &str,
    ) -> Result<bool, DataLayerError> {
        let mut nodes = self.nodes.write().expect("proxy nodes lock");
        let Some(node) = nodes.get_mut(node_id) else {
            return Ok(false);
        };
        if node.proxy_password.as_deref() != Some(expected) {
            return Ok(false);
        }
        node.proxy_password = Some(replacement.to_string());
        node.updated_at_unix_secs = Self::now_unix_secs();
        Ok(true)
    }

    async fn create_manual_node(
        &self,
        mutation: &ProxyNodeManualCreateMutation,
    ) -> Result<StoredProxyNode, DataLayerError> {
        let node_id = mutation
            .node_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let mut nodes = self.nodes.write().expect("proxy nodes lock");
        if nodes.contains_key(&node_id) {
            return Err(DataLayerError::InvalidInput(format!(
                "proxy node id already exists: {node_id}"
            )));
        }
        if nodes
            .values()
            .any(|node| node.ip == mutation.ip && node.port == mutation.port)
        {
            return Err(DataLayerError::InvalidInput(
                "proxy node with the same ip and port already exists".to_string(),
            ));
        }
        let node = StoredProxyNode::new(
            node_id.clone(),
            mutation.name.clone(),
            mutation.ip.clone(),
            mutation.port,
            "online".to_string(),
        )?
        .with_manual_proxy_fields(
            Some(mutation.proxy_url.clone()),
            mutation.proxy_username.clone(),
            mutation.proxy_password.clone(),
        )
        .with_runtime_fields(
            mutation.region.clone(),
            mutation.registered_by.clone(),
            None,
            None,
            Self::now_unix_secs(),
            Self::now_unix_secs(),
        )
        .with_node_generation(new_proxy_node_generation());
        nodes.insert(node_id, node.clone());
        Ok(node)
    }

    async fn update_manual_node(
        &self,
        mutation: &ProxyNodeManualUpdateMutation,
    ) -> Result<Option<StoredProxyNode>, DataLayerError> {
        let mut nodes = self.nodes.write().expect("proxy nodes lock");
        let Some(node) = nodes.get_mut(&mutation.node_id) else {
            return Ok(None);
        };
        if let Some(name) = mutation.name.as_ref() {
            node.name = name.clone();
        }
        if let Some(ip) = mutation.ip.as_ref() {
            node.ip = ip.clone();
        }
        if let Some(port) = mutation.port {
            node.port = port;
        }
        if let Some(region) = mutation.region.as_ref() {
            node.region = Some(region.clone());
        }
        if let Some(proxy_url) = mutation.proxy_url.as_ref() {
            node.proxy_url = Some(proxy_url.clone());
        }
        if let Some(username) = mutation.proxy_username.as_ref() {
            node.proxy_username = Some(username.clone());
        }
        if let Some(password) = mutation.proxy_password.as_ref() {
            node.proxy_password = Some(password.clone());
        }
        node.updated_at_unix_secs = Self::now_unix_secs();
        Ok(Some(node.clone()))
    }

    async fn record_traffic(
        &self,
        mutation: &ProxyNodeTrafficMutation,
    ) -> Result<bool, DataLayerError> {
        let Some(expected) = mutation
            .expected_node_generation
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(false);
        };
        let mut nodes = self.nodes.write().expect("proxy nodes lock");
        let Some(node) = nodes.get_mut(&mutation.node_id) else {
            return Ok(false);
        };
        if node.node_generation != expected {
            return Ok(false);
        }
        node.total_requests = node
            .total_requests
            .saturating_add(mutation.total_requests_delta.max(0));
        node.failed_requests = node
            .failed_requests
            .saturating_add(mutation.failed_requests_delta.max(0));
        node.dns_failures = node
            .dns_failures
            .saturating_add(mutation.dns_failures_delta.max(0));
        node.stream_errors = node
            .stream_errors
            .saturating_add(mutation.stream_errors_delta.max(0));
        node.last_used_at_unix_secs = Self::now_unix_secs();
        node.updated_at_unix_secs = Self::now_unix_secs();
        Ok(true)
    }

    async fn delete_node(&self, node_id: &str) -> Result<Option<StoredProxyNode>, DataLayerError> {
        Ok(self
            .nodes
            .write()
            .expect("proxy nodes lock")
            .remove(node_id))
    }

    async fn increment_manual_node_requests(
        &self,
        node_id: &str,
        total_delta: i64,
        failed_delta: i64,
        latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        let mut nodes = self.nodes.write().expect("proxy nodes lock");
        let Some(node) = nodes.get_mut(node_id) else {
            return Ok(());
        };
        node.total_requests = node.total_requests.saturating_add(total_delta.max(0));
        node.failed_requests = node.failed_requests.saturating_add(failed_delta.max(0));
        if let Some(latency_ms) = latency_ms {
            node.avg_latency_ms = Some(latency_ms as f64);
        }
        node.last_used_at_unix_secs = Self::now_unix_secs();
        node.updated_at_unix_secs = Self::now_unix_secs();
        Ok(())
    }
}

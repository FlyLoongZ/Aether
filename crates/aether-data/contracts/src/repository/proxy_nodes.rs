use async_trait::async_trait;

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredProxyNode {
    pub id: String,
    #[serde(default = "new_proxy_node_generation")]
    pub node_generation: String,
    pub name: String,
    pub ip: String,
    pub port: i32,
    pub region: Option<String>,
    pub proxy_url: Option<String>,
    pub proxy_username: Option<String>,
    pub proxy_password: Option<String>,
    pub status: String,
    pub registered_by: Option<String>,
    pub last_used_at_unix_secs: Option<u64>,
    pub total_requests: i64,
    pub avg_latency_ms: Option<f64>,
    pub failed_requests: i64,
    pub dns_failures: i64,
    pub stream_errors: i64,
    pub created_at_unix_ms: Option<u64>,
    pub updated_at_unix_secs: Option<u64>,
}

impl std::fmt::Debug for StoredProxyNode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredProxyNode")
            .field("id", &self.id)
            .field("node_generation", &self.node_generation)
            .field("name", &self.name)
            .field("ip", &self.ip)
            .field("port", &self.port)
            .field("region", &self.region)
            .field("proxy_url", &self.proxy_url.as_ref().map(|_| "[REDACTED]"))
            .field("proxy_username", &self.proxy_username)
            .field(
                "proxy_password",
                &self.proxy_password.as_ref().map(|_| "[REDACTED]"),
            )
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl StoredProxyNode {
    pub fn new(
        id: String,
        name: String,
        ip: String,
        port: i32,
        status: String,
    ) -> Result<Self, crate::DataLayerError> {
        if id.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.id is empty".to_string(),
            ));
        }
        if name.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.name is empty".to_string(),
            ));
        }
        if ip.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.ip is empty".to_string(),
            ));
        }
        if status.trim().is_empty() {
            return Err(crate::DataLayerError::UnexpectedValue(
                "proxy_nodes.status is empty".to_string(),
            ));
        }

        Ok(Self {
            id,
            node_generation: new_proxy_node_generation(),
            name,
            ip,
            port,
            region: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            status,
            registered_by: None,
            last_used_at_unix_secs: None,
            total_requests: 0,
            avg_latency_ms: None,
            failed_requests: 0,
            dns_failures: 0,
            stream_errors: 0,
            created_at_unix_ms: None,
            updated_at_unix_secs: None,
        })
    }

    pub fn with_runtime_fields(
        mut self,
        region: Option<String>,
        registered_by: Option<String>,
        last_used_at_unix_secs: Option<u64>,
        avg_latency_ms: Option<f64>,
        created_at_unix_ms: Option<u64>,
        updated_at_unix_secs: Option<u64>,
    ) -> Self {
        self.region = region;
        self.registered_by = registered_by;
        self.last_used_at_unix_secs = last_used_at_unix_secs;
        self.avg_latency_ms = avg_latency_ms;
        self.created_at_unix_ms = created_at_unix_ms;
        self.updated_at_unix_secs = updated_at_unix_secs;
        self
    }

    pub fn with_manual_proxy_fields(
        mut self,
        proxy_url: Option<String>,
        proxy_username: Option<String>,
        proxy_password: Option<String>,
    ) -> Self {
        self.proxy_url = proxy_url;
        self.proxy_username = proxy_username;
        self.proxy_password = proxy_password;
        self
    }

    pub fn with_node_generation(mut self, node_generation: String) -> Self {
        self.node_generation = node_generation;
        self
    }
}

pub fn new_proxy_node_generation() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProxyNodeTrafficMutation {
    pub node_id: String,
    /// Incarnation fence captured when the request plan selected this node.
    /// Missing fences are rejected by the gateway path so a stale plan cannot
    /// update a node recreated under the same id.
    #[serde(default)]
    pub expected_node_generation: Option<String>,
    pub total_requests_delta: i64,
    pub failed_requests_delta: i64,
    pub dns_failures_delta: i64,
    pub stream_errors_delta: i64,
}

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProxyNodeManualCreateMutation {
    /// Optional caller-selected id used to bind credentials before the row is
    /// inserted. Repositories generate one only for legacy callers that do
    /// not provide it.
    #[serde(default)]
    pub node_id: Option<String>,
    pub name: String,
    pub ip: String,
    pub port: i32,
    pub region: Option<String>,
    pub proxy_url: String,
    pub proxy_username: Option<String>,
    pub proxy_password: Option<String>,
    pub registered_by: Option<String>,
}

impl std::fmt::Debug for ProxyNodeManualCreateMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProxyNodeManualCreateMutation")
            .field("node_id", &self.node_id)
            .field("name", &self.name)
            .field("ip", &self.ip)
            .field("port", &self.port)
            .field("region", &self.region)
            .field("proxy_url", &"[REDACTED]")
            .field("proxy_username", &self.proxy_username)
            .field(
                "proxy_password",
                &self.proxy_password.as_ref().map(|_| "[REDACTED]"),
            )
            .field("registered_by", &self.registered_by)
            .finish()
    }
}

#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProxyNodeManualUpdateMutation {
    pub node_id: String,
    pub name: Option<String>,
    pub ip: Option<String>,
    pub port: Option<i32>,
    pub region: Option<String>,
    pub proxy_url: Option<String>,
    pub proxy_username: Option<String>,
    pub proxy_password: Option<String>,
}

impl std::fmt::Debug for ProxyNodeManualUpdateMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProxyNodeManualUpdateMutation")
            .field("node_id", &self.node_id)
            .field("name", &self.name)
            .field("ip", &self.ip)
            .field("port", &self.port)
            .field("region", &self.region)
            .field("proxy_url", &self.proxy_url.as_ref().map(|_| "[REDACTED]"))
            .field("proxy_username", &self.proxy_username)
            .field(
                "proxy_password",
                &self.proxy_password.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[async_trait]
pub trait ProxyNodeReadRepository: Send + Sync {
    async fn list_proxy_nodes(&self) -> Result<Vec<StoredProxyNode>, crate::DataLayerError>;

    async fn find_proxy_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, crate::DataLayerError>;
}

#[async_trait]
pub trait ProxyNodeWriteRepository: Send + Sync {
    async fn compare_and_set_proxy_password(
        &self,
        node_id: &str,
        expected: &str,
        replacement: &str,
    ) -> Result<bool, crate::DataLayerError>;

    async fn create_manual_node(
        &self,
        mutation: &ProxyNodeManualCreateMutation,
    ) -> Result<StoredProxyNode, crate::DataLayerError>;

    async fn update_manual_node(
        &self,
        mutation: &ProxyNodeManualUpdateMutation,
    ) -> Result<Option<StoredProxyNode>, crate::DataLayerError>;

    async fn record_traffic(
        &self,
        mutation: &ProxyNodeTrafficMutation,
    ) -> Result<bool, crate::DataLayerError>;

    async fn delete_node(
        &self,
        node_id: &str,
    ) -> Result<Option<StoredProxyNode>, crate::DataLayerError>;

    async fn increment_manual_node_requests(
        &self,
        node_id: &str,
        total_delta: i64,
        failed_delta: i64,
        latency_ms: Option<i64>,
    ) -> Result<(), crate::DataLayerError>;
}

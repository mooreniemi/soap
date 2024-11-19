use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tracing::{debug, info};
use uuid::Uuid;

// Node input/output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInput {
    pub inputs: HashMap<String, serde_json::Value>, // source_node -> output_value
    pub params: Option<serde_json::Value>,
    #[serde(default)]
    pub cache_output: Option<bool>,
    #[serde(default)]
    pub use_cached_inputs: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOutput {
    pub value: serde_json::Value,
    pub error: Option<String>,
}

// Cache trait for flexibility
pub trait Cache: Send + Sync {
    fn get(&self, key: &str) -> Option<NodeOutput>;
    fn put(&self, key: &str, value: NodeOutput);
    fn invalidate(&self, key: &str);
}

// Simple in-memory implementation
#[derive(Clone)]
pub struct InMemoryCache {
    store: Arc<DashMap<String, (NodeOutput, SystemTime)>>,
    ttl: Duration,
}

impl InMemoryCache {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            store: Arc::new(DashMap::new()),
            ttl: Duration::from_secs(ttl_seconds),
        }
    }
}

impl Cache for InMemoryCache {
    fn get(&self, key: &str) -> Option<NodeOutput> {
        if let Some(entry) = self.store.get(key) {
            let (output, timestamp) = &*entry;
            if timestamp.elapsed().unwrap() < self.ttl {
                debug!("Cache hit for key {}: {:?}", key, output);
                return Some(output.clone());
            }
            debug!("Cache expired for key {}", key);
            self.store.remove(key);
        } else {
            debug!("Cache miss for key {}", key);
        }
        None
    }

    fn put(&self, key: &str, value: NodeOutput) {
        debug!("Caching value for key {}: {:?}", key, value);
        self.store
            .insert(key.to_string(), (value, SystemTime::now()));
    }

    fn invalidate(&self, key: &str) {
        self.store.remove(key);
    }
}

// New node-specific configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeDefinition {
    depends_on: Vec<String>,
    #[serde(default)]
    use_cached_inputs: Option<bool>,
    #[serde(default)]
    cache_output: Option<bool>,
    #[serde(default)]
    params: Option<serde_json::Value>,
}

// Updated DAG configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DagConfig {
    nodes: HashMap<String, NodeDefinition>,
    request_id: Option<String>,
    output_nodes: Option<Vec<String>>,
    inputs: Option<HashMap<String, serde_json::Value>>, // Add external inputs
}

// Shared state for the orchestrator
#[derive(Clone)]
struct Orchestrator {
    endpoints: Arc<Mutex<HashMap<String, fn(NodeInput) -> NodeOutput>>>,
    cache: Arc<dyn Cache>,
    cache_enabled: bool,
}

impl Orchestrator {
    fn new(cache: Arc<dyn Cache>, cache_enabled: bool) -> Self {
        Self {
            endpoints: Arc::new(Mutex::new(HashMap::new())),
            cache,
            cache_enabled,
        }
    }

    fn register_endpoint(&self, name: String, handler: fn(NodeInput) -> NodeOutput) {
        let mut endpoints = self.endpoints.lock().unwrap();
        endpoints.insert(name, handler);
    }

    // Helper to convert new config format to dependencies map
    fn get_dependencies(config: &DagConfig) -> HashMap<String, Vec<String>> {
        config
            .nodes
            .iter()
            .map(|(node, def)| {
                let deps = def
                    .depends_on
                    .iter()
                    .filter(|_| !def.use_cached_inputs.unwrap_or(false))
                    .cloned()
                    .collect();
                (node.clone(), deps)
            })
            .collect()
    }

    // Add helper method to check if caching is enabled for a node
    fn is_cache_enabled_for_node(&self, node: &str, config: &DagConfig) -> bool {
        if let Some(def) = config.nodes.get(node) {
            def.cache_output.unwrap_or(self.cache_enabled)
        } else {
            false
        }
    }

    // Updated validation
    fn validate(&self, config: &DagConfig) -> Result<(), String> {
        debug!("Validating DAG config: {:?}", config);

        // First check for non-existent dependencies
        for (node, def) in &config.nodes {
            for dep in &def.depends_on {
                let is_valid = config.nodes.contains_key(dep)
                    || config
                        .inputs
                        .as_ref()
                        .map_or(false, |inputs| inputs.contains_key(dep));
                if !is_valid {
                    return Err(format!(
                        "Node {} depends on non-existent node {}",
                        node, dep
                    ));
                }
            }
        }

        // Use get_dependencies() to get actual dependencies (filtering out cached ones)
        let dependencies = Orchestrator::get_dependencies(config);
        debug!("DAG Dependencies: {:?}", dependencies);
        let mut resolved = HashSet::new();

        // Start with external inputs
        if let Some(inputs) = &config.inputs {
            resolved.extend(inputs.keys().cloned());
        }
        debug!("Resolved inputs: {:?}", resolved);

        // Keep resolving until we can't anymore
        loop {
            let resolvable = dependencies
                .iter()
                .filter(|(node, deps)| {
                    !resolved.contains(*node) && deps.iter().all(|dep| resolved.contains(dep))
                })
                .map(|(node, _)| node.clone())
                .collect::<Vec<_>>();
            debug!("Resolvable nodes: {:?}", resolvable);

            if resolvable.is_empty() {
                break;
            }

            resolved.extend(resolvable);
        }

        // Check if all nodes were resolved
        let unresolved: Vec<_> = dependencies
            .keys()
            .filter(|node| !resolved.contains(*node))
            .collect();
        debug!("Unresolved nodes: {:?}", unresolved);

        if !unresolved.is_empty() {
            return Err(format!("Cycle detected among nodes: {:?}", unresolved));
        }

        Ok(())
    }

    /// Executes a DAG configuration with the following features:
    /// - Parallel execution of independent nodes
    /// - Caching of node outputs with TTL
    /// - Support for external inputs
    /// - Dependency-based execution order
    /// - Optional output node filtering
    /// Returns a tuple of (request_id, results) or an error message
    async fn execute(
        &self,
        mut config: DagConfig,
    ) -> Result<(String, HashMap<String, NodeOutput>), String> {
        debug!("Starting DAG execution");
        self.validate(&config)?;

        let request_id = config
            .request_id
            .take()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        debug!("Request ID: {}", request_id);

        let mut completed = HashSet::new();
        let mut results: HashMap<String, NodeOutput> = HashMap::new();

        // First, add all external inputs to results
        if let Some(inputs) = &config.inputs {
            for (node, value) in inputs {
                results.insert(
                    node.clone(),
                    NodeOutput {
                        value: value.clone(),
                        error: None,
                    },
                );
            }
        }
        debug!("Preloaded completed inputs: {:?}", completed);

        while completed.len() < config.nodes.len() {
            let to_execute: Vec<_> = config
                .nodes
                .iter()
                .filter(|(node, _)| !completed.contains(*node))
                .filter(|(_, def)| {
                    def.depends_on.iter().all(|dep| {
                        completed.contains(dep) ||                              // Already executed
                        config.inputs.as_ref().map_or(false, |inputs|         // External input
                            inputs.contains_key(dep)
                        ) ||
                        (def.use_cached_inputs.unwrap_or(false) &&            // Available in cache
                         self.check_cache_available(dep, &config))
                    })
                })
                .map(|(node, def)| (node.clone(), def.clone()))
                .collect();
            debug!("Nodes to execute: {:?}", to_execute);

            if to_execute.is_empty() && completed.len() < config.nodes.len() {
                return Err("Unable to make progress - possible cycle".to_string());
            }

            debug!("Executing nodes in parallel");
            // Execute nodes in parallel
            let mut handles = Vec::new();
            for (node, def) in to_execute {
                let cache = self.cache.clone();
                let endpoints = self.endpoints.clone();
                let results = results.clone();
                let config = config.clone();
                let orchestrator = self.clone(); // Clone the orchestrator

                handles.push(tokio::task::spawn_blocking(move || {
                    // Collect inputs for this node
                    let inputs = orchestrator.collect_node_inputs(&def, &results, &config)?;

                    // Execute the node
                    let endpoints_guard = endpoints.lock().map_err(|e| e.to_string())?;
                    let handler = endpoints_guard
                        .get(&node)
                        .ok_or_else(|| format!("No handler for node {}", node))?;

                    let result = handler(NodeInput {
                        inputs: inputs.clone(),
                        params: def.params.clone(),
                        cache_output: def.cache_output,
                        use_cached_inputs: def.use_cached_inputs,
                    });

                    // Cache result if needed
                    if orchestrator.is_cache_enabled_for_node(&node, &config) {
                        let cache_key = generate_cache_key(&node, &inputs, &def.params);
                        cache.put(&cache_key, result.clone());
                    }

                    Ok::<_, String>((node, result))
                }));
            }

            for handle in handles {
                match handle.await {
                    Ok(Ok((node, result))) => {
                        if let Some(error) = &result.error {
                            return Err(error.clone());
                        }
                        results.insert(node.clone(), result);
                        completed.insert(node);
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(e.to_string()),
                }
            }
        }

        let outputs = if let Some(output_nodes) = config.output_nodes {
            results
                .into_iter()
                .filter(|(node, _)| output_nodes.contains(node))
                .collect()
        } else {
            results
        };
        debug!("Final results: {:?}", outputs);

        Ok((request_id, outputs))
    }

    // Helper method to check if a node's result is available in cache
    fn check_cache_available(&self, node: &str, config: &DagConfig) -> bool {
        // First check if the node was configured to cache its output
        if let Some(def) = config.nodes.get(node) {
            // If cache_output is explicitly set to false, cache is not available
            if def.cache_output == Some(false) {
                debug!("Cache disabled for node {}", node);
                return false;
            }
        }

        // Even if the node exists in cache, we shouldn't use it if caching was disabled
        let cache_key = generate_cache_key(node, &HashMap::new(), &None);
        if !self.cache.get(&cache_key).is_some() {
            debug!("No cached value found for node {}", node);
            return false;
        }

        true
    }

    // Helper to collect inputs for a node from all possible sources
    fn collect_node_inputs(
        &self,
        def: &NodeDefinition,
        results: &HashMap<String, NodeOutput>,
        config: &DagConfig,
    ) -> Result<HashMap<String, serde_json::Value>, String> {
        let mut inputs = HashMap::new();

        // If node has no dependencies but there are external inputs, use them directly
        if def.depends_on.is_empty() {
            if let Some(external_inputs) = &config.inputs {
                inputs.extend(external_inputs.clone());
                return Ok(inputs);
            }
        }

        // Original dependency handling logic
        for dep in &def.depends_on {
            let value = if def.use_cached_inputs.unwrap_or(false) {
                // Try cache first if use_cached_inputs is true
                let dep_inputs = self.collect_dependency_inputs(def, config);
                let cache_key = generate_cache_key(dep, &dep_inputs, &None);

                // Check if the dependency was configured to cache its output
                if let Some(dep_def) = config.nodes.get(dep) {
                    if dep_def.cache_output == Some(false) {
                        return Err(format!(
                            "Node {} requires cached input from {}, but {} has caching disabled",
                            def.depends_on[0], dep, dep
                        ));
                    }
                }

                if let Some(cached) = self.cache.get(&cache_key) {
                    cached.value
                } else {
                    return Err(format!("No cached value available for {}", dep));
                }
            } else if let Some(result) = results.get(dep) {
                // Try results from current execution
                result.value.clone()
            } else if let Some(inputs) = &config.inputs {
                // Try external inputs
                inputs
                    .get(dep)
                    .ok_or_else(|| format!("No input found for {}", dep))?
                    .clone()
            } else {
                return Err(format!("No value found for dependency {}", dep));
            };

            inputs.insert(dep.clone(), value);
        }

        Ok(inputs)
    }

    fn collect_dependency_inputs(
        &self,
        def: &NodeDefinition,
        config: &DagConfig,
    ) -> HashMap<String, serde_json::Value> {
        let mut inputs = HashMap::new();

        // Only collect from external inputs for dependencies
        if let Some(external_inputs) = &config.inputs {
            for dep in &def.depends_on {
                if let Some(value) = external_inputs.get(dep) {
                    inputs.insert(dep.clone(), value.clone());
                }
            }
        }

        inputs
    }
}

// HTTP handler for individual endpoints
async fn endpoint_handler(
    State(orchestrator): State<Orchestrator>,
    Path(name): Path<String>,
    Json(input): Json<NodeInput>,
) -> impl IntoResponse {
    debug!("Endpoint request for node {} with input: {:?}", name, input);

    // Create a single-node DAG configuration
    let config = DagConfig {
        nodes: {
            let mut nodes = HashMap::new();
            nodes.insert(
                name.clone(),
                NodeDefinition {
                    depends_on: vec![],
                    cache_output: input.cache_output,
                    use_cached_inputs: input.use_cached_inputs,
                    params: input.params.clone(),
                },
            );
            nodes
        },
        request_id: None,
        output_nodes: Some(vec![name.clone()]),
        inputs: Some(input.inputs),
    };

    // Execute the single-node DAG
    match orchestrator.execute(config).await {
        Ok((_, results)) => match results.get(&name) {
            Some(result) => Json(result.clone()).into_response(),
            None => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Node execution produced no output",
            )
                .into_response(),
        },
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

#[derive(Debug, Clone, Serialize)]
struct DagResponse {
    request_id: String,
    outputs: HashMap<String, NodeOutput>,
}

async fn compose_handler(
    State(orchestrator): State<Orchestrator>,
    Json(config): Json<DagConfig>,
) -> impl IntoResponse {
    if let Err(err) = orchestrator.validate(&config) {
        return (StatusCode::BAD_REQUEST, err).into_response();
    }

    match orchestrator.execute(config).await {
        Ok((request_id, results)) => {
            // Update to receive tuple
            Json(DagResponse {
                request_id,
                outputs: results,
            })
            .into_response()
        }
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
    }
}

async fn validate_handler(
    State(orchestrator): State<Orchestrator>,
    Json(config): Json<DagConfig>,
) -> impl IntoResponse {
    match orchestrator.validate(&config) {
        Ok(()) => (StatusCode::OK, "Valid DAG configuration").into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err).into_response(),
    }
}

/// Creates endpoint per node, composition endpoint, and validation endpoint
pub fn create_dag_router(
    handlers: HashMap<String, fn(NodeInput) -> NodeOutput>,
    cache: Arc<dyn Cache>,
    cache_enabled: bool,
) -> Router {
    let orchestrator = Orchestrator::new(cache, cache_enabled);

    // Register handlers
    for (name, handler) in handlers {
        orchestrator.register_endpoint(name, handler);
    }

    Router::new()
        .route("/compose", post(compose_handler))
        .route("/validate", post(validate_handler))
        .route(
            "/endpoint/:name",
            post(|state, path, body| endpoint_handler(state, path, body)),
        )
        .with_state(orchestrator)
}

fn generate_cache_key(
    node: &str,
    inputs: &HashMap<String, serde_json::Value>,
    params: &Option<serde_json::Value>,
) -> String {
    let mut hasher = DefaultHasher::new();
    node.hash(&mut hasher);

    let mut sorted_inputs: Vec<_> = inputs.iter().collect();
    sorted_inputs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    for (k, v) in sorted_inputs {
        k.hash(&mut hasher);
        v.to_string().hash(&mut hasher);
    }

    if let Some(p) = params {
        p.to_string().hash(&mut hasher);
    }

    format!("node_{}_{:x}", node, hasher.finish())
}

#[cfg(test)]
mod tests {
    use axum::body::HttpBody;

    use super::*;
    use std::sync::Arc;

    // Handlers for individual endpoints
    fn handler_a(input: NodeInput) -> NodeOutput {
        let base_value = 2; // Keep this at 2 to match tests

        // Check if we should return a string
        if let Some(ref params) = input.params {
            if let Some(return_string) = params.get("return_string") {
                if return_string.as_bool().unwrap_or(false) {
                    return NodeOutput {
                        value: serde_json::json!("string_value"),
                        error: None,
                    };
                }
            }
        }

        // Apply multiplier if present
        let value = if let Some(ref params) = input.params {
            if let Some(multiplier) = params.get("multiplier") {
                if let Some(m) = multiplier.as_f64() {
                    (base_value as f64 * m) as i64
                } else {
                    return NodeOutput {
                        value: serde_json::Value::Null,
                        error: Some(
                            "invalid parameter type: multiplier must be a number".to_string(),
                        ),
                    };
                }
            } else {
                base_value
            }
        } else {
            base_value
        };

        NodeOutput {
            value: serde_json::json!(value),
            error: None,
        }
    }

    fn handler_b(input: NodeInput) -> NodeOutput {
        // First check if we should return a string (for testing error propagation)
        if let Some(ref params) = input.params {
            if let Some(return_string) = params.get("return_string") {
                if return_string.as_bool().unwrap_or(false) {
                    return NodeOutput {
                        value: serde_json::json!("string_value"),
                        error: None,
                    };
                }
            }
        }

        let multiplier = input
            .params
            .as_ref()
            .and_then(|obj| obj.get("multiplier"))
            .and_then(|v| v.as_f64())
            .unwrap_or(2.0);

        // Check if any input is non-numeric
        if input.inputs.values().any(|v| !v.is_number()) {
            return NodeOutput {
                value: serde_json::Value::Null,
                error: Some("expects numeric input".to_string()),
            };
        }

        // Get input value and ensure it's numeric
        let input_value = match input
            .inputs
            .values()
            .next()
            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|n| n as f64)))
        {
            Some(val) => val,
            None => {
                return NodeOutput {
                    value: serde_json::Value::Null,
                    error: Some("expects numeric input".to_string()),
                }
            }
        };

        NodeOutput {
            value: serde_json::json!((input_value * multiplier) as i64),
            error: None,
        }
    }

    fn handler_c(input: NodeInput) -> NodeOutput {
        // C expects exactly one input
        if input.inputs.len() > 1 {
            return NodeOutput {
                value: serde_json::Value::Null,
                error: Some(format!(
                    "Node C expects single input, got {}",
                    input.inputs.len()
                )),
            };
        }

        let value = if input.inputs.is_empty() {
            4 // Default value if no input
        } else {
            let (_, input_value) = input.inputs.iter().next().unwrap();
            match input_value.as_i64() {
                Some(num) => num + 5,
                None => {
                    return NodeOutput {
                        value: serde_json::Value::Null,
                        error: Some(format!(
                            "Node C expects numeric input, got: {}",
                            input_value
                        )),
                    }
                }
            }
        };

        NodeOutput {
            value: serde_json::json!(value),
            error: None,
        }
    }

    fn handler_d(input: NodeInput) -> NodeOutput {
        // Verify all inputs are numeric
        for (name, value) in &input.inputs {
            if !value.is_number() {
                return NodeOutput {
                    value: serde_json::Value::Null,
                    error: Some(format!(
                        "expects numeric input, got {} from {}",
                        value, name
                    )),
                };
            }
        }

        // D multiplies all inputs together
        if input.inputs.is_empty() {
            return NodeOutput {
                value: serde_json::json!(1), // Identity for multiplication
                error: None,
            };
        }

        let mut result = 1;
        for (source_node, value) in input.inputs {
            match value.as_i64() {
                Some(num) => result *= num,
                None => {
                    return NodeOutput {
                        value: serde_json::Value::Null,
                        error: Some(format!(
                            "expects numeric input, got {} from {}",
                            value, source_node
                        )),
                    }
                }
            }
        }

        NodeOutput {
            value: serde_json::json!(result),
            error: None,
        }
    }

    struct MockCache {
        data: Arc<Mutex<HashMap<String, NodeOutput>>>,
    }

    impl Cache for MockCache {
        fn get(&self, key: &str) -> Option<NodeOutput> {
            self.data.lock().unwrap().get(key).cloned()
        }

        fn put(&self, key: &str, value: NodeOutput) {
            self.data.lock().unwrap().insert(key.to_string(), value);
        }

        fn invalidate(&self, key: &str) {
            self.data.lock().unwrap().remove(key);
        }
    }

    #[tokio::test]
    async fn test_cache_behavior() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache.clone(), true);

        // Register handlers
        orchestrator.register_endpoint("A".to_string(), handler_a);
        orchestrator.register_endpoint("B".to_string(), handler_b);

        // First execution - should cache result
        let config = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        cache_output: Some(true),
                        use_cached_inputs: None,
                        params: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let result = orchestrator.execute(config).await.unwrap();
        let first_value = result.1.get("A").unwrap().value.clone();

        // Second execution - should use cached result
        let config2 = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        cache_output: None,
                        use_cached_inputs: Some(true),
                        params: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let result2 = orchestrator.execute(config2).await.unwrap();

        // Results should match
        assert_eq!(first_value, result2.1.get("A").unwrap().value);
    }

    #[tokio::test]
    async fn test_cache_disabled() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache.clone(), true);

        // Register handlers
        orchestrator.register_endpoint("A".to_string(), handler_a);

        // Execute with cache_output: false
        let config = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        cache_output: Some(false),
                        use_cached_inputs: None,
                        params: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let _ = orchestrator.execute(config).await.unwrap();

        // Check cache is empty
        assert_eq!(cache.data.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_error_propagation() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache, true);

        // Register handlers
        orchestrator.register_endpoint("A".to_string(), handler_a);
        orchestrator.register_endpoint("B".to_string(), handler_b);
        orchestrator.register_endpoint("D".to_string(), handler_d);

        // Test case: B returns string, D expects number
        let config = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        params: None,
                        cache_output: None,
                        use_cached_inputs: None,
                    },
                );
                nodes.insert(
                    "B".to_string(),
                    NodeDefinition {
                        depends_on: vec!["A".to_string()],
                        params: Some(serde_json::json!({"return_string": true})),
                        cache_output: None,
                        use_cached_inputs: None,
                    },
                );
                nodes.insert(
                    "D".to_string(),
                    NodeDefinition {
                        depends_on: vec!["B".to_string()],
                        params: None,
                        cache_output: None,
                        use_cached_inputs: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let result = orchestrator.execute(config).await;
        assert!(result.is_err(), "Expected error when B returns string");

        let err = result.unwrap_err();
        assert!(
            err.contains("numeric input") || err.contains("must be numeric"),
            "Error '{}' should mention numeric requirement",
            err
        );
    }

    #[tokio::test]
    async fn test_handler_b_behavior() {
        // Test that handler_b properly validates its inputs
        let input = NodeInput {
            inputs: {
                let mut map = HashMap::new();
                map.insert("A".to_string(), serde_json::json!("not a number"));
                map
            },
            params: None,
            cache_output: None,
            use_cached_inputs: None,
        };

        let result = handler_b(input);
        assert!(
            result.error.is_some(),
            "handler_b should error on non-numeric input"
        );
        assert!(
            result.error.unwrap().contains("numeric input"),
            "Error should mention numeric input"
        );
    }

    #[tokio::test]
    async fn test_handler_d_behavior() {
        // Test that handler_d properly validates its inputs
        let input = NodeInput {
            inputs: {
                let mut map = HashMap::new();
                map.insert("B".to_string(), serde_json::json!("not a number"));
                map.insert("C".to_string(), serde_json::json!(42));
                map
            },
            params: None,
            cache_output: None,
            use_cached_inputs: None,
        };

        let result = handler_d(input);
        assert!(
            result.error.is_some(),
            "handler_d should error on non-numeric input"
        );
        assert!(
            result.error.unwrap().contains("numeric input"),
            "Error should mention numeric input"
        );
    }

    #[tokio::test]
    async fn test_cache_input_values() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache.clone(), true);
        orchestrator.register_endpoint("A".to_string(), handler_a);
        orchestrator.register_endpoint("B".to_string(), handler_b);

        // First execution - cache A's output
        let config1 = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        cache_output: Some(true),
                        use_cached_inputs: None,
                        params: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let result1 = orchestrator.execute(config1).await.unwrap();
        let a_value = result1.1.get("A").unwrap().value.clone();

        // Second execution - B should use cached A value
        let config2 = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        use_cached_inputs: Some(true),
                        cache_output: None,
                        params: None,
                    },
                );
                nodes.insert(
                    "B".to_string(),
                    NodeDefinition {
                        depends_on: vec!["A".to_string()],
                        use_cached_inputs: None,
                        cache_output: None,
                        params: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let result2 = orchestrator.execute(config2).await.unwrap();
        assert_eq!(
            result2.1.get("B").unwrap().value,
            serde_json::json!(4) // B should double A's value of 2
        );
    }

    #[tokio::test]
    async fn test_cache_disabled_with_dependency() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache.clone(), true);
        orchestrator.register_endpoint("A".to_string(), handler_a);
        orchestrator.register_endpoint("B".to_string(), handler_b);

        let config = DagConfig {
            nodes: {
                let mut nodes = HashMap::new();
                nodes.insert(
                    "A".to_string(),
                    NodeDefinition {
                        depends_on: vec![],
                        cache_output: Some(false),
                        use_cached_inputs: None,
                        params: None,
                    },
                );
                nodes.insert(
                    "B".to_string(),
                    NodeDefinition {
                        depends_on: vec!["A".to_string()],
                        use_cached_inputs: Some(true),
                        cache_output: None,
                        params: None,
                    },
                );
                nodes
            },
            request_id: None,
            output_nodes: None,
            inputs: None,
        };

        let result = orchestrator.execute(config).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No cached value"));
    }

    #[tokio::test]
    async fn test_standalone_endpoint() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache.clone(), true);
        orchestrator.register_endpoint("B".to_string(), handler_b);

        let input = NodeInput {
            inputs: {
                let mut map = HashMap::new();
                map.insert("A".to_string(), serde_json::json!(5));
                map
            },
            params: None,
            cache_output: None,
            use_cached_inputs: None,
        };

        let result = handler_b(input);
        assert_eq!(result.value, serde_json::json!(10)); // B should double input of 5
        assert!(result.error.is_none());
    }

    // With extract debug statements
    #[tokio::test]
    async fn test_endpoint_handler() {
        let cache = Arc::new(MockCache {
            data: Arc::new(Mutex::new(HashMap::new())),
        });
        let orchestrator = Orchestrator::new(cache.clone(), true);
        orchestrator.register_endpoint("B".to_string(), handler_b);

        let input = NodeInput {
            inputs: {
                let mut map = HashMap::new();
                map.insert("B".to_string(), serde_json::json!(5));
                map
            },
            params: None,
            cache_output: None,
            use_cached_inputs: None,
        };

        let response = endpoint_handler(State(orchestrator), Path("B".to_string()), Json(input))
            .await
            .into_response();

        // Debug statement for response status
        // Extract the response status first
        let status = response.status();

        // Extract the response body
        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();

        // Debug statements
        println!("Response status: {}", status);
        println!("Response body: {}", String::from_utf8_lossy(&body_bytes));

        // Deserialize the response body
        let output: NodeOutput = serde_json::from_slice(&body_bytes).unwrap();

        // Proceed with assertions
        assert_eq!(status, StatusCode::OK);
        assert_eq!(output.value, serde_json::json!(5));
        assert!(output.error.is_none());
    }
}

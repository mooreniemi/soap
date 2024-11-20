use axum::{
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::{collections::hash_map::DefaultHasher, time::Instant};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
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
    inputs: Option<HashMap<String, serde_json::Value>>,
}

// Shared state for the orchestrator
#[derive(Clone)]
struct Orchestrator {
    // Maybe don't really need DashMap here
    endpoints: Arc<DashMap<String, fn(NodeInput) -> NodeOutput>>,
    cache: Arc<dyn Cache>,
    cache_enabled: bool,
}

impl Orchestrator {
    fn new(cache: Arc<dyn Cache>, cache_enabled: bool) -> Self {
        Self {
            endpoints: Arc::new(DashMap::new()),
            cache,
            cache_enabled,
        }
    }

    fn register_endpoint(&self, name: String, handler: fn(NodeInput) -> NodeOutput) {
        self.endpoints.insert(name, handler);
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

        let start_time = Instant::now();
        // Use get_dependencies() to get actual dependencies (filtering out cached ones)
        let dependencies = Orchestrator::get_dependencies(config);
        let dependencies_duration = Instant::now().duration_since(start_time);
        debug!("DAG Dependencies took {:?}", dependencies_duration);
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
        let start_time = Instant::now();
        self.validate(&config)?;
        let validation_duration = Instant::now().duration_since(start_time);
        debug!("DAG validation took {:?}", validation_duration);

        let request_id = config
            .request_id
            .take()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        debug!("Request ID: {}", request_id);

        let mut completed = HashSet::new();
        let mut results: HashMap<String, NodeOutput> = HashMap::new();

        let start_time = Instant::now();
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
        let preload_duration = Instant::now().duration_since(start_time);
        debug!("Preloaded completed inputs in {:?}", preload_duration);

        let mut iteration = 0;
        // NOTE: basically a topological sort, find what has depends_on fulfilled, that's completed
        // then execute the node with completed inputs - repeat until all nodes are completed
        // (anything that can run in parallel, will run in parallel)
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

            debug!("Executing {} nodes in parallel", to_execute.len());
            let execution_start_time = Instant::now();
            let mut handles = Vec::new();
            for (node, def) in to_execute {
                let cache = self.cache.clone();
                let endpoints = self.endpoints.clone();
                let results = results.clone();
                let config = config.clone();
                let orchestrator = self.clone();

                handles.push(tokio::task::spawn_blocking(move || {
                    // Collect inputs for this node
                    let inputs = orchestrator.collect_node_inputs(&def, &results, &config)?;

                    // Execute the node
                    let handler = endpoints
                        .get(&node)
                        .ok_or_else(|| format!("No handler for node {}", node))?;

                    let individual_execution_start_time = Instant::now();
                    let result = handler(NodeInput {
                        inputs: inputs.clone(),
                        params: def.params.clone(),
                        cache_output: def.cache_output,
                        use_cached_inputs: def.use_cached_inputs,
                    });
                    let individual_execution_duration = Instant::now().duration_since(individual_execution_start_time);
                    debug!("Node {} execution took {:?}", node, individual_execution_duration);

                    // Cache result if needed
                    if orchestrator.is_cache_enabled_for_node(&node, &config) {
                        let cache_start_time = Instant::now();
                        let cache_key = generate_cache_key(&node, &inputs, &def.params);
                        cache.put(&cache_key, result.clone());
                        let cache_duration = Instant::now().duration_since(cache_start_time);
                        debug!("Node {} caching took {:?}", node, cache_duration);
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
            iteration += 1;
            let execution_duration = Instant::now().duration_since(execution_start_time);
            debug!("Iteration {} took {:?}", iteration, execution_duration);
        }

        // NOTE: we kept all accumulated results, but only return the ones that are in the output_nodes list
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
mod tests;
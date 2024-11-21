use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::{join_all, BoxFuture};
use std::time::Instant;
use serde_json::json;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::fs::{OpenOptions, File};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use tokio::io::AsyncWriteExt;

// Placeholder Enums and Types
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
enum ComponentEnum {
    Example(String),
    Transform(String),
    Validation(String),
    Aggregator(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaseComponentInput {
    data: serde_json::Value,
    context: RequestContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestContext {
    request_id: Uuid,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaseComponentOutput {
    state: CompletionState,
    cacheable_data: serde_json::Value,
    context: RequestContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CompletionState {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheConfig {
    current: CurrentCacheConfig,
    history: HistoryCacheConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CurrentCacheConfig {
    cache_output: bool,      // Whether to cache this component's output
    use_cached: bool,        // Whether to use cached outputs from dependencies
    use_history: Option<HistorySource>,  // New field to specify historical output
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum HistoryStorage {
    InMemory,
    File(PathBuf),
}

impl Default for HistoryStorage {
    fn default() -> Self {
        Self::File(PathBuf::from("/tmp/dag_history.jsonl"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryCacheConfig {
    store_output: bool,
    storage: HistoryStorage,
    load_from_history: bool,
}

impl Default for HistoryCacheConfig {
    fn default() -> Self {
        Self {
            store_output: true,
            storage: HistoryStorage::default(),
            load_from_history: false,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            current: CurrentCacheConfig {
                cache_output: true,
                use_cached: true,
                use_history: None,
            },
            history: HistoryCacheConfig {
                store_output: true,  // Always store by default
                storage: HistoryStorage::default(),  // This will use File storage by default
                load_from_history: false,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ComponentNode {
    dependencies: Vec<ComponentEnum>,
    cache_config: CacheConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ComponentExecutionState {
    output: BaseComponentOutput,
    state: CompletionState,
}

// First, let's create a more specific error type
#[derive(Debug, Serialize, Deserialize)]
enum ComponentError {
    ExecutionError(String),
    DependencyFailed(ComponentEnum),
    ComponentNotFound(ComponentEnum),
    CyclicDependency,
}

// First, define the component traits
trait AsyncComponent: Send + Sync {
    fn name(&self) -> ComponentEnum;
    fn execute(
        &self,
        input: BaseComponentInput,
        cache: ComponentCache,
        context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>>;
}

trait BlockingComponent: Send + Sync {
    fn name(&self) -> ComponentEnum;
    fn execute(
        &self,
        input: BaseComponentInput,
        cache: ComponentCache,
        context: RequestContext,
    ) -> Result<BaseComponentOutput, String>;
}

// Then define the execution mode enum
enum ComponentExecutionMode {
    Async(Arc<dyn AsyncComponent>),
    Blocking(Arc<dyn BlockingComponent>),
}

impl ComponentExecutionMode {
    async fn execute(&self, input: BaseComponentInput, cache: ComponentCache, context: RequestContext) -> Result<BaseComponentOutput, String> {
        match self {
            ComponentExecutionMode::Async(component) => {
                let component = component.clone();
                component.execute(input, cache, context).await
            }
            ComponentExecutionMode::Blocking(component) => {
                let component = component.clone();
                tokio::task::spawn_blocking(move || {
                    component.execute(input, cache, context)
                })
                .await
                .map_err(|e| format!("Task panicked: {}", e))?
            }
        }
    }

    fn name(&self) -> ComponentEnum {
        match self {
            ComponentExecutionMode::Async(component) => component.name(),
            ComponentExecutionMode::Blocking(component) => component.name(),
        }
    }
}

// Update the executor to use the new name
struct ComponentsExecutorManager {
    components: Arc<HashMap<ComponentEnum, ComponentExecutionMode>>,
    global_cache_config: CacheConfig,  // Global settings that can be overridden per node
    cache: Option<ComponentCache>,  // Add this field
}

impl ComponentsExecutorManager {
    async fn new(
        components: HashMap<ComponentEnum, ComponentExecutionMode>,
        global_cache_config: Option<CacheConfig>,
        history_storage: Option<HistoryStorage>,
    ) -> Self {
        let mut cache_config = global_cache_config.unwrap_or_default();
        if let Some(storage) = history_storage {
            cache_config.history.storage = storage;
        }

        let cache = ComponentCache::new(cache_config.history.storage.clone());

        // Load history if configured
        if cache_config.history.load_from_history {
            if let Err(e) = cache.load_history_from_file().await {
                println!("⚠️ Failed to load history: {}", e);
            }
        }

        Self {
            components: Arc::new(components),
            global_cache_config: cache_config,
            cache: Some(cache),
        }
    }

    async fn execute(
        &self,
        component_nodes: HashMap<ComponentEnum, ComponentNode>,
        inputs: HashMap<ComponentEnum, BaseComponentOutput>,
        request_id: Uuid,
    ) -> Result<BaseComponentOutput, ComponentError> {
        let cache = self.cache.as_ref()
            .expect("Cache should be initialized")
            .clone();

        // Initialize cache with inputs
        for (component, input) in inputs.iter() {
            cache.store_current(component.clone(), input.clone()).await;
        }

        // Clone what we need from self early
        let components = self.components.clone();
        let global_cache_config = self.global_cache_config.clone();

        let sorted_components = self.topologically_sort_components(&component_nodes)
            .map_err(|_| ComponentError::CyclicDependency)?;

        let mut final_output = None;

        for component_enum in &sorted_components {
            let node = component_nodes.get(component_enum)
                .ok_or_else(|| ComponentError::ComponentNotFound(component_enum.clone()))?;

            // Add debug logging using component name
            if let Some(component) = self.components.get(component_enum) {
                println!("🔄 Processing component: {:?}", component.name());
            }

            // Get inputs for this component from its dependencies
            let component_input = if let Some(deps) = node.dependencies.first() {
                // Use the first dependency's output as input
                cache.get(deps).await
                    .ok_or_else(|| ComponentError::DependencyFailed(deps.clone()))?
                    .into()  // Convert BaseComponentOutput to BaseComponentInput
            } else {
                // If no dependencies, use the provided input if available
                inputs.get(component_enum)
                    .cloned()
                    .map(Into::into)  // Convert to BaseComponentInput if available
                    .unwrap_or_else(|| BaseComponentInput {
                        data: json!({}),
                        context: RequestContext {
                            request_id,
                            timestamp: Utc::now(),
                        },
                    })
            };

            // Check for specific historical output first
            if let Some(history_source) = &node.cache_config.current.use_history {
                if let Some(cache) = self.cache.as_ref() {
                    if let Some(historical_output) = cache.get_historical(
                        history_source.request_id,
                        component_enum
                    ).await {
                        println!("🕰️ Using historical output from request {} for {:#?}",
                            history_source.request_id, component_enum);
                        final_output = Some(historical_output);
                        continue;
                    }
                }
            }

            // Fall back to normal cache/execution logic
            let use_cached = node.cache_config.current.use_cached ||
                global_cache_config.current.use_cached;
            let should_cache_current = node.cache_config.current.cache_output ||
                global_cache_config.current.cache_output;
            let should_store_history = node.cache_config.history.store_output ||
                global_cache_config.history.store_output;

            if use_cached {
                if let Some(cached) = cache.get(component_enum).await {
                    println!("🎯 Using cached output for {:#?}", component_enum);
                    final_output = Some(cached.clone());
                    continue;
                }
            }

            let component_enum = component_enum.clone();
            let cache = cache.clone();
            let components = components.clone();
            let context = RequestContext {
                request_id,
                timestamp: Utc::now(),
            };

            let execution_result = tokio::spawn(async move {
                let component = components
                    .get(&component_enum)
                    .ok_or_else(|| ComponentError::ComponentNotFound(component_enum.clone()))?;

                println!("⚙️ Executing component: {:?}", component.name());

                match component.execute(component_input, cache.clone(), context.clone()).await {
                    Ok(output) => {
                        println!("✅ Component {:?} completed successfully", component.name());
                        // Always store in history if configured
                        if should_store_history {
                            cache.insert(component_enum.clone(), output.clone()).await;
                        }

                        // Only store successful outputs in current cache if configured
                        if should_cache_current && matches!(output.state, CompletionState::Success) {
                            cache.store_current(component_enum.clone(), output.clone()).await;
                        }
                        Ok(output)
                    }
                    Err(e) => {
                        println!("❌ Component {:?} failed: {}", component.name(), e);
                        // Store failure in history
                        let failure_output = BaseComponentOutput {
                            state: CompletionState::Failure,
                            cacheable_data: json!({ "error": e.to_string() }),
                            context,
                        };

                        if should_store_history {
                            cache.insert(component_enum.clone(), failure_output.clone()).await;
                        }

                        Err(ComponentError::ExecutionError(e))
                    }
                }
            })
            .await
            .map_err(|e| ComponentError::ExecutionError(format!("Task join error: {}", e)))??;

            final_output = Some(execution_result);
        }

        final_output.ok_or_else(|| ComponentError::ExecutionError("No components executed".to_string()))
    }

    fn topologically_sort_components(
        &self,
        component_nodes: &HashMap<ComponentEnum, ComponentNode>,
    ) -> Result<Vec<ComponentEnum>, &'static str> {
        // Simple topological sort implementation
        let mut sorted = Vec::new();
        let mut visited = HashMap::new();

        for component in component_nodes.keys() {
            self.visit(component, component_nodes, &mut visited, &mut sorted)?;
        }

        Ok(sorted)
    }

    fn visit(
        &self,
        component: &ComponentEnum,
        nodes: &HashMap<ComponentEnum, ComponentNode>,
        visited: &mut HashMap<ComponentEnum, bool>,
        sorted: &mut Vec<ComponentEnum>,
    ) -> Result<(), &'static str> {
        if let Some(&true) = visited.get(component) {
            return Ok(());
        }
        if let Some(&false) = visited.get(component) {
            return Err("Cycle detected");
        }

        visited.insert(component.clone(), false);
        if let Some(node) = nodes.get(component) {
            for dependency in &node.dependencies {
                self.visit(dependency, nodes, visited, sorted)?;
            }
        }
        visited.insert(component.clone(), true);
        sorted.push(component.clone());

        Ok(())
    }

    // Add this new method
    async fn get_cache_history(&self) -> Vec<(HistoryKey, BaseComponentOutput)> {
        if let Some(cache) = self.cache.as_ref() {
            let history = cache.history.lock().await;
            history.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    async fn get_request_history(&self, request_id: Uuid) -> Vec<(ComponentEnum, BaseComponentOutput)> {
        if let Some(cache) = self.cache.as_ref() {
            // Use get_historical for specific lookups
            let history = cache.history.lock().await;
            history.iter()
                .filter(|((req_id, _), _)| *req_id == request_id)
                .map(|((_, component), output)| (component.clone(), output.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }
}

// Example Component implementations
struct ExampleComponent {
    instance_name: String,
}

impl AsyncComponent for ExampleComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Example(self.instance_name.clone())
    }

    fn execute(
        &self,
        input: BaseComponentInput,
        cache: ComponentCache,
        context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        let instance_name = self.instance_name.clone();

        Box::pin(async move {
            println!("Processing input data: {:?}", input.data);

            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let output = BaseComponentOutput {
                state: CompletionState::Success,
                cacheable_data: json!({
                    "input_data": input.data,
                    "processed_at": input.context.timestamp,
                }),
                context,
            };

            // Use the proper cache methods
            cache.insert(
                ComponentEnum::Example(instance_name),
                output.clone()
            ).await;

            Ok(output)
        })
    }
}

// Add configuration struct
#[derive(Clone, Serialize, Deserialize)]
struct TransformConfig {
    iterations: u64,
    modulo: u64,
}

// Update component to take config
struct TransformComponent {
    instance_name: String,
    config: TransformConfig,
}

impl TransformComponent {
    fn new(instance_name: String, config: TransformConfig) -> Self {
        Self { instance_name, config }
    }
}

impl BlockingComponent for TransformComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Transform(self.instance_name.clone())
    }

    fn execute(&self, _input: BaseComponentInput, _cache: ComponentCache, context: RequestContext) -> Result<BaseComponentOutput, String> {
        let start = Instant::now();

        // Use config values
        let mut result = 0u64;
        for i in 0..self.config.iterations {
            result = result.wrapping_add(i % self.config.modulo);
        }

        println!("⚙️  TransformComponent processing took {:?}", start.elapsed());
        Ok(BaseComponentOutput {
            state: CompletionState::Success,
            cacheable_data: json!({
                "result": result,
                "processing_time": start.elapsed().as_millis()
            }),
            context: context,
        })
    }
}

// Validation Component
struct ValidationComponent {
    instance_name: String,
}

impl AsyncComponent for ValidationComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Validation(self.instance_name.clone())
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
        _cache: ComponentCache,
        _context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        Box::pin(async move {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let result = if rand::random::<bool>() {
                println!("❌ ValidationComponent failed after {:?}", start.elapsed());
                Err("Validation failed".to_string())
            } else {
                println!("⚙️  ValidationComponent processing took {:?}", start.elapsed());
                Ok(BaseComponentOutput {
                    state: CompletionState::Success,
                    cacheable_data: json!({}),
                    context: RequestContext {
                        request_id: Uuid::new_v4(),
                        timestamp: Utc::now(),
                    },
                })
            };
            result
        })
    }
}

// Aggregator Component
struct AggregatorComponent {
    instance_name: String,
}

impl AsyncComponent for AggregatorComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Aggregator(self.instance_name.clone())
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
        _cache: ComponentCache,
        _context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        Box::pin(async move {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            println!("⚙️  AggregatorComponent processing took {:?}", start.elapsed());
            Ok(BaseComponentOutput {
                state: CompletionState::Success,
                cacheable_data: json!({}),
                context: RequestContext {
                    request_id: Uuid::new_v4(),
                    timestamp: Utc::now(),
                },
            })
        })
    }
}

// First, define our cache types
type CurrentCache = Arc<Mutex<HashMap<ComponentEnum, BaseComponentOutput>>>;
type HistoryKey = (Uuid, ComponentEnum);
type HistoryCache = Arc<Mutex<BTreeMap<HistoryKey, BaseComponentOutput>>>;

// Then define our ComponentCache struct
#[derive(Clone)]  // No Serialize/Deserialize here!
struct ComponentCache {
    current: CurrentCache,
    history: HistoryCache,
    storage_config: HistoryStorage,
}

// Implement methods for ComponentCache
impl ComponentCache {
    fn new(storage_config: HistoryStorage) -> Self {  // This is the key fix - no arguments needed
        Self {
            current: Arc::new(Mutex::new(HashMap::new())),
            history: Arc::new(Mutex::new(BTreeMap::new())),
            storage_config,
        }
    }

    async fn store_current(&self, component: ComponentEnum, output: BaseComponentOutput) {
        self.current.lock().await.insert(component, output);
    }

    async fn insert(&self, component: ComponentEnum, output: BaseComponentOutput) {
        // Store in memory caches
        let history_key = (output.context.request_id, component.clone());
        self.history.lock().await.insert(history_key, output.clone());

        // Write to file if configured
        if let HistoryStorage::File(path) = &self.storage_config {
            let entry = HistoryEntry {
                request_id: output.context.request_id,
                component,
                output,
                timestamp: chrono::Utc::now(),
            };

            if let Ok(json) = serde_json::to_string(&entry) {
                // Use tokio's async file operations
                if let Ok(mut file) = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await
                {
                    // Write line and flush asynchronously
                    let _ = file.write_all(json.as_bytes()).await;
                    let _ = file.write_all(b"\n").await;
                    let _ = file.flush().await;
                }
            }
        }
    }

    async fn get(&self, component: &ComponentEnum) -> Option<BaseComponentOutput> {
        self.current.lock().await.get(component).cloned()
    }

    async fn get_historical(&self, request_id: Uuid, component: &ComponentEnum) -> Option<BaseComponentOutput> {
        let history = self.history.lock().await;
        history.get(&(request_id, component.clone())).cloned()
    }

    // Also update history loading to be async
    async fn load_history_from_file(&self) -> io::Result<()> {
        if let HistoryStorage::File(path) = &self.storage_config {
            let contents = tokio::fs::read_to_string(path).await?;
            let mut history = self.history.lock().await;

            for line in contents.lines() {
                if let Ok(entry) = serde_json::from_str::<HistoryEntry>(line) {
                    history.insert(
                        (entry.request_id, entry.component),
                        entry.output
                    );
                }
            }
        }
        Ok(())
    }
}

// Add serializable history entry
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistoryEntry {
    request_id: Uuid,
    component: ComponentEnum,
    output: BaseComponentOutput,
    timestamp: DateTime<Utc>,
}

impl From<BaseComponentOutput> for BaseComponentInput {
    fn from(output: BaseComponentOutput) -> Self {
        BaseComponentInput {
            data: output.cacheable_data,
            context: output.context,
        }
    }
}

impl Default for BaseComponentInput {
    fn default() -> Self {
        Self {
            data: json!({}),
            context: RequestContext {
                request_id: Uuid::new_v4(),
                timestamp: Utc::now(),
            },
        }
    }
}

// Add a history source configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistorySource {
    request_id: Uuid,
    // Optionally add timestamp bounds
    // min_timestamp: Option<DateTime<Utc>>,
    // max_timestamp: Option<DateTime<Utc>>,
}

fn build_dag_from_json(dag_config: serde_json::Value) -> Result<(
    HashMap<ComponentEnum, ComponentExecutionMode>,
    HashMap<ComponentEnum, ComponentNode>
), String> {
    // Define the factory function type
    type ComponentFactory = Box<dyn Fn(String) -> ComponentExecutionMode>;

    // Remove mut since we're not modifying the HashMap
    let component_factory = HashMap::from([
        (
            "example",
            Box::new(|name: String| ComponentExecutionMode::Async(Arc::new(ExampleComponent {
                instance_name: name
            }))) as ComponentFactory
        ),
        (
            "transform",
            Box::new(|name: String| ComponentExecutionMode::Blocking(Arc::new(TransformComponent::new(
                name,
                TransformConfig {
                    iterations: 1_000_000,
                    modulo: 1000,
                }
            )))) as ComponentFactory
        ),
        (
            "validation",
            Box::new(|name: String| ComponentExecutionMode::Async(Arc::new(ValidationComponent {
                instance_name: name
            }))) as ComponentFactory
        ),
        (
            "aggregator",
            Box::new(|name: String| ComponentExecutionMode::Async(Arc::new(AggregatorComponent {
                instance_name: name
            }))) as ComponentFactory
        ),
    ]);

    let mut components = HashMap::new();
    let mut nodes = HashMap::new();

    // Parse the DAG configuration
    let dag = dag_config.as_object().ok_or("Invalid DAG configuration")?;

    for (node_name, node_config) in dag {
        let node_obj = node_config.as_object().ok_or("Invalid node configuration")?;

        // Get component type
        let component_type = node_obj["type"]
            .as_str()
            .ok_or("Component type not specified")?;

        // Create component with instance name
        let component = if let Some(factory_fn) = component_factory.get(component_type) {
            factory_fn(node_name.clone())
        } else {
            return Err(format!("Unknown component type: {}", component_type));
        };

        // Map string to ComponentEnum with instance name
        let component_enum = match component_type {
            "example" => ComponentEnum::Example(node_name.clone()),
            "transform" => ComponentEnum::Transform(node_name.clone()),
            "validation" => ComponentEnum::Validation(node_name.clone()),
            "aggregator" => ComponentEnum::Aggregator(node_name.clone()),
            _ => return Err(format!("Unknown component type: {}", component_type)),
        };

        // Get dependencies - now using node_name for lookup
        let dependencies = node_obj["dependencies"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .map(|dep| {
                let dep_name = dep.as_str().ok_or_else(|| format!("Invalid dependency name"))?;
                // Find the matching component type from the already processed nodes
                dag.get(dep_name)
                    .and_then(|n| n.get("type"))
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| format!("Dependency not found: {}", dep_name))
                    .map(|dep_type| match dep_type {
                        "example" => ComponentEnum::Example(dep_name.to_string()),
                        "transform" => ComponentEnum::Transform(dep_name.to_string()),
                        "validation" => ComponentEnum::Validation(dep_name.to_string()),
                        "aggregator" => ComponentEnum::Aggregator(dep_name.to_string()),
                        _ => panic!("Invalid component type"), // Should never happen as we validated earlier
                    })
            })
            .collect::<Result<Vec<ComponentEnum>, String>>()?;

        // Parse cache config
        let cache_config = node_obj.get("cache")
            .and_then(|c| c.as_object())
            .map(|c| CacheConfig {
                current: CurrentCacheConfig {
                    cache_output: c.get("cache_output").and_then(|v| v.as_bool()).unwrap_or(true),
                    use_cached: c.get("use_cached").and_then(|v| v.as_bool()).unwrap_or(true),
                    use_history: c.get("use_history")
                        .and_then(|v| v.as_object())
                        .map(|v| HistorySource {
                            request_id: Uuid::parse_str(v.get("request_id").and_then(|v| v.as_str()).unwrap()).unwrap(),
                        }),
                },
                history: HistoryCacheConfig {
                    store_output: c.get("store_output").and_then(|v| v.as_bool()).unwrap_or(true),
                    storage: HistoryStorage::File(PathBuf::from("/tmp/dag_history.jsonl")),
                    load_from_history: c.get("load_from_history").and_then(|v| v.as_bool()).unwrap_or(false),
                },
            })
            .unwrap_or_default();

        // Add component and node
        components.insert(component_enum.clone(), component);
        nodes.insert(component_enum, ComponentNode {
            dependencies,
            cache_config,
        });
    }

    Ok((components, nodes))
}

// Updated main function
#[tokio::main]
async fn main() {
    // Define DAG using JSON
    let specific_request = Uuid::parse_str("3cd1e52b-d05c-47c8-9af0-c950256bf52c").unwrap();

    let dag_config = json!({
        "transform_a": {
            "type": "transform",
            "config": {
                "iterations": 1_000_000,
                "modulo": 1000
            },
            "cache": {
                "current": {
                    //"cache_output": true,
                    //"use_cached": true,
                    "use_history": {
                        "request_id": specific_request.to_string()
                    }
                },
                "history": {
                    "store_output": true
                }
            },
            "dependencies": []
        },
        "validation_a": {
            "type": "validation",
            "cache": {
                "current": {
                    "cache_output": true,
                    "use_cached": true
                },
                "history": {
                    "store_output": true
                }
            },
            "dependencies": ["transform_a"]
        },
        "transform_b": {
            "type": "transform",
            "config": {
                "iterations": 500_000,
                "modulo": 500
            },
            "cache": {
                "current": {
                    "cache_output": true,
                    "use_cached": true
                },
                "history": {
                    "store_output": true
                }
            },
            "dependencies": ["transform_a"]
        },
        "aggregator": {
            "type": "aggregator",
            "cache": {
                "current": {
                    "cache_output": true,
                    "use_cached": true
                },
                "history": {
                    "store_output": true
                }
            },
            "dependencies": ["validation_a", "transform_b"]
        }
    });

    // Build DAG from JSON
    let (components, component_nodes) = match build_dag_from_json(dag_config) {
        Ok(dag) => dag,
        Err(e) => {
            println!("❌ Failed to build DAG: {}", e);
            return;
        }
    };

    let executor = ComponentsExecutorManager::new(
        components,
        Some(CacheConfig {
            current: CurrentCacheConfig {
                cache_output: true,
                use_cached: true,
                use_history: Some(HistorySource {
                    request_id: specific_request,
                }),
            },
            history: HistoryCacheConfig {
                store_output: true,
                storage: HistoryStorage::File(PathBuf::from("/tmp/dag_history.jsonl")),
                load_from_history: true,
            },
        }),
        None,  // Use default storage from config
    ).await;
    let inputs = HashMap::new();
    let request_id = Uuid::new_v4();

    match executor.execute(component_nodes, inputs, request_id).await {
        Ok(output) => {
            println!("✅ Execution succeeded with output: {:?}", output);

            // Peek at just this request's history
            println!("\n📜 History for Request {}:", request_id);
            let history = executor.get_cache_history().await;
            for ((req_id, component), output) in history {
                // Only show entries from this request
                if req_id == request_id {
                    println!(
                        "Component: {:?}\n  -> State: {:?}\n  -> Data: {}\n",
                        component,
                        output.state,
                        output.cacheable_data
                    );
                }
            }
        }
        Err(e) => println!("❌ Execution failed: {:?}", e),
    }
}

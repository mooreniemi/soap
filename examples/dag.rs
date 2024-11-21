use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::BoxFuture;
use std::time::Instant;
use serde_json::json;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::io::{self};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use tokio::io::AsyncWriteExt;

/// Some fake Components, all have (String) to allow for different instances
/// This way you can have multiple "Example" components, each with their own configuration
#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
enum ComponentEnum {
    Example(String),
    Transform(String),
    Validation(String),
    Aggregator(String),
}

/// Since we want be able to cache input and output, we enforce data is serializable
/// The request context is used for cache keys
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BaseComponentInput {
    cacheable_data: serde_json::Value,
    context: RequestContext,
    consumed_keys: Vec<String>,  // Keys this component expects in cacheable_data
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
    produced_keys: Vec<String>,  // Keys this component promises to produce in cacheable_data
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
    cache_output: bool,
    use_cached: bool,
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
    use_history: Option<HistorySource>,
}

impl Default for HistoryCacheConfig {
    fn default() -> Self {
        Self {
            store_output: true,
            storage: HistoryStorage::default(),
            load_from_history: false,
            use_history: None,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            current: CurrentCacheConfig {
                cache_output: true,
                use_cached: true,
            },
            history: HistoryCacheConfig {
                store_output: true,
                storage: HistoryStorage::default(),
                load_from_history: false,
                use_history: None,
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

#[derive(Debug)]
enum ComponentError {
    ComponentNotFound(ComponentEnum),
    DependencyFailed(ComponentEnum),
    CyclicDependency,
    ExecutionError(String),
    HistoryNotFound(ComponentEnum, Uuid),
}

trait AsyncComponent: Send + Sync {
    fn name(&self) -> ComponentEnum;
    fn get_consumed_keys(&self) -> Vec<String>;
    fn get_produced_keys(&self) -> Vec<String>;
    fn execute(
        &self,
        input: BaseComponentInput,
        context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>>;
}

trait BlockingComponent: Send + Sync {
    fn name(&self) -> ComponentEnum;
    fn execute(
        &self,
        input: BaseComponentInput,
        context: RequestContext,
    ) -> Result<BaseComponentOutput, String>;
}

/// We don't want different Components to starve each other by not yielding in async
/// Async is really for maximizing throughput, not minimizing latency
/// There's no programatic and perfect way to detect if a Component is not yielding
/// so we just use this enum to explicitly state the execution mode and ask implementers to do the right thing
enum ComponentExecutionMode {
    /// If your component is async (eg. non-blocking IO), use this
    Async(Arc<dyn AsyncComponent>),
    /// If your component is blocking (eg. CPU intensive tasks), use this
    Blocking(Arc<dyn BlockingComponent>),
}

impl ComponentExecutionMode {
    async fn execute(
        &self,
        input: BaseComponentInput,
        context: RequestContext,
    ) -> Result<BaseComponentOutput, String> {
        match self {
            ComponentExecutionMode::Async(component) => {
                let component = component.clone();
                component.execute(input, context).await
            }
            ComponentExecutionMode::Blocking(component) => {
                let component = component.clone();
                tokio::task::spawn_blocking(move || {
                    component.execute(input, context)
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
    cache: Option<ComponentCache>,
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

    /// Executes the DAG by processing components in topologically sorted order
    /// Components at the same level (no dependencies between them) are executed in parallel
    ///
    /// # Arguments
    /// * `component_nodes` - Map of components and their dependency configuration
    /// * `inputs` - Initial inputs for source nodes (nodes with no dependencies)
    /// * `request_id` - Unique identifier for this DAG execution
    ///
    /// # Returns
    /// The output of the final component in the DAG, or an error if execution fails
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

        let components = self.components.clone();

        let sorted_components = self.topologically_sort_components(&component_nodes)
            .map_err(|_| ComponentError::CyclicDependency)?;

        let mut final_output = None;

        // Group components by their "level" (distance from root)
        let mut levels: HashMap<usize, Vec<ComponentEnum>> = HashMap::new();
        let mut component_levels = HashMap::new();

        // Calculate level for each component
        for component in &sorted_components {
            let level = if let Some(node) = component_nodes.get(component) {
                let dep_level = node.dependencies.iter()
                    .map(|dep| component_levels.get(dep).unwrap_or(&0))
                    .max()
                    .unwrap_or(&0);
                dep_level + 1
            } else {
                0
            };
            component_levels.insert(component.clone(), level);
            levels.entry(level).or_default().push(component.clone());
        }

        // Process each level in order (where we get parallelism)
        for level in 0..=*component_levels.values().max().unwrap_or(&0) {
            if let Some(components_at_level) = levels.get(&level) {
                println!("🌟 Processing level {}: {:?}", level, components_at_level);

                let mut level_futures = Vec::new();

                for component_enum in components_at_level {
                    let node = component_nodes.get(component_enum)
                        .ok_or_else(|| ComponentError::ComponentNotFound(component_enum.clone()))?;

                    // Check history requirements - use node config or fall back to global
                    let use_history = node.cache_config.history.use_history
                        .as_ref()
                        .or_else(|| {
                            println!("ℹ️ No node-specific history config for {:#?}, using global", component_enum);
                            self.global_cache_config.history.use_history.as_ref()
                        })
                        .map(|h| h.clone());

                    if let Some(history_source) = use_history {
                        if let Some(historical_output) = cache.get_historical(
                            history_source.request_id,
                            component_enum
                        ).await {
                            println!("🕰️ Using historical output for {:#?}", component_enum);
                            cache.store_current(component_enum.clone(), historical_output.clone()).await;
                            continue;
                        } else if history_source.strict {
                            return Err(ComponentError::HistoryNotFound(
                                component_enum.clone(),
                                history_source.request_id
                            ));
                        }
                    }

                    // Use current cache if enabled
                    if node.cache_config.current.use_cached {
                        if let Some(cached) = cache.get_current(component_enum).await {
                            println!("🎯 Using cached output for {:#?}", component_enum);
                            final_output = Some(cached.clone());
                            continue;
                        }
                    }

                    // Get inputs for this component
                    let component_input = if let Some(deps) = node.dependencies.first() {
                        cache.get_current(deps).await
                            .ok_or_else(|| ComponentError::DependencyFailed(deps.clone()))?
                            .into()
                    } else {
                        inputs.get(&component_enum)
                            .cloned()
                            .map(Into::into)
                            .unwrap_or_default()
                    };

                    // Clone what we need for the spawned task
                    let component_enum = component_enum.clone();
                    let cache = cache.clone();
                    let components = components.clone();
                    let context = RequestContext {
                        request_id,
                        timestamp: Utc::now(),
                    };

                    // Spawn the execution with context
                    let future = tokio::spawn(async move {
                        let component = components
                            .get(&component_enum)
                            .ok_or_else(|| ComponentError::ComponentNotFound(component_enum.clone()))?;

                        println!("⚙️ Executing component: {:?}", component.name());

                        match component.execute(component_input, context).await {
                            Ok(output) => {
                                println!("✅ Component {:?} completed successfully", component.name());
                                cache.store_current(component_enum.clone(), output.clone()).await;
                                cache.store_history(component_enum.clone(), output.clone()).await;
                                Ok((component_enum, output))
                            }
                            Err(e) => {
                                println!("❌ Component {:?} failed: {}", component.name(), e);
                                Err(ComponentError::ExecutionError(e))
                            }
                        }
                    });

                    level_futures.push(future);
                }

                // Wait for all components at this level to complete
                let results = futures::future::join_all(level_futures).await;

                for result in results {
                    match result {
                        Ok(Ok((component, output))) => {
                            final_output = Some(output);
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(ComponentError::ExecutionError(format!("Task join error: {}", e))),
                    }
                }
            }
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

    // Retrieves the entire cache history
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

    // Retrieves history for a specific request by filtering the entire cache history
    async fn get_request_history(&self, request_id: Uuid) -> Vec<(ComponentEnum, BaseComponentOutput)> {
        self.get_cache_history().await.into_iter()
            .filter(|((req_id, _), _)| *req_id == request_id)
            .map(|((_, component), output)| (component, output))
            .collect()
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

    fn get_consumed_keys(&self) -> Vec<String> {
        vec!["input_data".to_string()]
    }

    fn get_produced_keys(&self) -> Vec<String> {
        vec!["processed_data".to_string(), "processing_time".to_string()]
    }

    fn execute(
        &self,
        input: BaseComponentInput,
        context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        let instance_name = self.instance_name.clone();
        let produced_keys = self.get_produced_keys();

        Box::pin(async move {
            // Validate input
            input.validate()?;

            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let output = BaseComponentOutput {
                state: CompletionState::Success,
                cacheable_data: json!({
                    "processed_data": input.cacheable_data["input_data"],
                    "processing_time": start.elapsed().as_millis()
                }),
                context: context.clone(),
                produced_keys: produced_keys.clone(),
            };

            // Validate output
            if let Err(e) = output.validate() {
                return Ok(BaseComponentOutput {
                    state: CompletionState::Failure,
                    cacheable_data: json!({
                        "error": e.to_string()
                    }),
                    context,
                    produced_keys: vec![],  // Failed to produce any keys
                });
            }

            Ok(output)
        })
    }
}

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

    fn execute(
        &self,
        input: BaseComponentInput,
        context: RequestContext,
    ) -> Result<BaseComponentOutput, String> {
        let start = Instant::now();

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
            context,
            produced_keys: vec!["result".to_string(), "processing_time".to_string()],
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

    fn get_consumed_keys(&self) -> Vec<String> {
        vec![]
    }

    fn get_produced_keys(&self) -> Vec<String> {
        vec!["validation_result".to_string()]
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
        context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        Box::pin(async move {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            // NOTE: simulate random failure to make sure that we can handle it
            let result = if rand::random::<bool>() {
                println!("❌ ValidationComponent failed after {:?}", start.elapsed());
                Err("Validation failed".to_string())
            } else {
                println!("⚙️  ValidationComponent processing took {:?}", start.elapsed());
                Ok(BaseComponentOutput {
                    state: CompletionState::Success,
                    cacheable_data: json!({}),
                    context,
                    produced_keys: vec!["validation_result".to_string()],
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

    fn get_consumed_keys(&self) -> Vec<String> {
        vec![]
    }

    fn get_produced_keys(&self) -> Vec<String> {
        vec![]
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
        context: RequestContext,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        Box::pin(async move {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            println!("⚙️  AggregatorComponent processing took {:?}", start.elapsed());
            Ok(BaseComponentOutput {
                state: CompletionState::Success,
                cacheable_data: json!({}),
                context,
                produced_keys: vec![],
            })
        })
    }
}

type CurrentCache = Arc<Mutex<HashMap<ComponentEnum, BaseComponentOutput>>>;
type HistoryKey = (Uuid, ComponentEnum);
type HistoryCache = Arc<Mutex<BTreeMap<HistoryKey, BaseComponentOutput>>>;

#[derive(Clone)]
struct ComponentCache {
    /// Cache for current DAG execution (in memory)
    current: CurrentCache,
    /// Historical cache from previous DAG executions (in memory and file)
    history: HistoryCache,
    /// Configuration for persistent storage
    storage_config: HistoryStorage,
}

// Implement methods for ComponentCache
impl ComponentCache {
    fn new(storage_config: HistoryStorage) -> Self {
        Self {
            current: Arc::new(Mutex::new(HashMap::new())),
            history: Arc::new(Mutex::new(BTreeMap::new())),
            storage_config,
        }
    }

    async fn store_current(&self, component: ComponentEnum, output: BaseComponentOutput) {
        self.current.lock().await.insert(component, output);
    }

    /// Stores component output in both current and historical caches
    /// Also writes to persistent storage if configured
    async fn store_history(&self, component: ComponentEnum, output: BaseComponentOutput) {
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
                if let Ok(mut file) = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await
                {
                    let _ = file.write_all(json.as_bytes()).await;
                    let _ = file.write_all(b"\n").await;
                    let _ = file.flush().await;
                }
            }
        }
    }

    async fn get_current(&self, component: &ComponentEnum) -> Option<BaseComponentOutput> {
        self.current.lock().await.get(component).cloned()
    }

    /// Retrieves historical output for a specific component from a previous DAG execution
    async fn get_historical(&self, request_id: Uuid, component: &ComponentEnum) -> Option<BaseComponentOutput> {
        println!("🔍 Looking for historical output for request {} component {:?}",  // Add this
            request_id, component);

        // First, try to get the historical output from the in-memory cache
        let history = self.history.lock().await;
        if let Some(output) = history.get(&(request_id, component.clone())) {
            println!("✅ Found in memory cache");  // Add this
            return Some(output.clone());
        }
        drop(history); // Release the lock before loading from file

        // If not found in memory, attempt to load from the file
        if let HistoryStorage::File(path) = &self.storage_config {
            if let Ok(contents) = tokio::fs::read_to_string(path).await {
                let mut history = self.history.lock().await;
                for line in contents.lines() {
                    if let Ok(entry) = serde_json::from_str::<HistoryEntry>(line) {
                        println!("✅ Loaded history entry for request {} component {:?}",  // Add this
                            entry.request_id, entry.component);
                        history.insert(
                            (entry.request_id, entry.component.clone()),
                            entry.output.clone(),
                        );
                        // Check if this is the entry we're looking for
                        if entry.request_id == request_id && entry.component == *component {
                            return Some(entry.output);
                        }
                    }
                }
            }
        }

        println!("❌ Not found anywhere");  // Add this
        None
    }

    async fn load_history_from_file(&self) -> io::Result<()> {
        if let HistoryStorage::File(path) = &self.storage_config {
            println!("📖 Loading history from {}", path.display());
            let contents = tokio::fs::read_to_string(path).await?;
            println!("📖 Read {} bytes from file", contents.len());
            let mut history = self.history.lock().await;

            for line in contents.lines() {
                println!("📝 Processing line: {}", line);
                match serde_json::from_str::<HistoryEntry>(line) {
                    Ok(entry) => {
                        println!("✅ Successfully parsed entry for request {} component {:?}",
                            entry.request_id, entry.component);
                        history.insert(
                            (entry.request_id, entry.component),
                            entry.output
                        );
                    }
                    Err(e) => println!("❌ Failed to parse line: {}", e),
                }
            }
            println!("📖 Loaded {} entries into memory", history.len());
        }
        Ok(())
    }
}

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
            cacheable_data: output.cacheable_data,
            context: output.context,
            consumed_keys: output.produced_keys,
        }
    }
}

impl Default for BaseComponentInput {
    fn default() -> Self {
        Self {
            cacheable_data: json!({}),
            context: RequestContext {
                request_id: Uuid::new_v4(),
                timestamp: Utc::now(),
            },
            consumed_keys: vec![],
        }
    }
}

/// Configuration for historical data source
/// Used when a component should use output from a previous DAG execution
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HistorySource {
    /// UUID of the previous DAG execution to pull from
    request_id: Uuid,
    /// If true, DAG execution will fail when historical data isn't found
    /// If false, component will execute normally when history isn't found
    strict: bool,
}

impl Default for HistorySource {
    fn default() -> Self {
        Self {
            request_id: Uuid::new_v4(),
            strict: true,  // Default to strict mode (fail if history not found)
        }
    }
}

// Make validation methods take owned values
impl BaseComponentInput {
    fn validate(&self) -> Result<(), String> {
        let data = self.cacheable_data.as_object()
            .ok_or_else(|| "cacheable_data must be an object".to_string())?;

        for key in &self.consumed_keys {
            if !data.contains_key(key) {
                return Err(format!("Missing required key: {}", key));
            }
        }
        Ok(())
    }
}

impl BaseComponentOutput {
    fn validate(&self) -> Result<(), String> {
        let data = self.cacheable_data.as_object()
            .ok_or_else(|| "cacheable_data must be an object".to_string())?;

        for key in &self.produced_keys {
            if !data.contains_key(key) {
                return Err(format!("Component failed to produce promised key: {}", key));
            }
        }
        Ok(())
    }
}

fn build_dag_from_json(dag_config: serde_json::Value) -> Result<(
    HashMap<ComponentEnum, ComponentExecutionMode>,
    HashMap<ComponentEnum, ComponentNode>
), String> {
    // Define the factory function type
    type ComponentFactory = Box<dyn Fn(String) -> ComponentExecutionMode>;

    // where you register your Components to JSON config names
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

    let dag = dag_config.as_object().ok_or("Invalid DAG configuration")?;

    for (node_name, node_config) in dag {
        let node_obj = node_config.as_object().ok_or("Invalid node configuration")?;

        let component_type = node_obj["type"]
            .as_str()
            .ok_or("Component type not specified")?;

        let component = if let Some(factory_fn) = component_factory.get(component_type) {
            factory_fn(node_name.clone())
        } else {
            return Err(format!("Unknown component type: {}", component_type));
        };

        let component_enum = match component_type {
            "example" => ComponentEnum::Example(node_name.clone()),
            "transform" => ComponentEnum::Transform(node_name.clone()),
            "validation" => ComponentEnum::Validation(node_name.clone()),
            "aggregator" => ComponentEnum::Aggregator(node_name.clone()),
            _ => return Err(format!("Unknown component type: {}", component_type)),
        };

        let dependencies = node_obj["dependencies"]
            .as_array()
            .unwrap_or(&Vec::new())
            .iter()
            .map(|dep| {
                let dep_name = dep.as_str().ok_or_else(|| format!("Invalid dependency name"))?;
                dag.get(dep_name)
                    .and_then(|n| n.get("type"))
                    .and_then(|t| t.as_str())
                    .ok_or_else(|| format!("Dependency not found: {}", dep_name))
                    .map(|dep_type| match dep_type {
                        "example" => ComponentEnum::Example(dep_name.to_string()),
                        "transform" => ComponentEnum::Transform(dep_name.to_string()),
                        "validation" => ComponentEnum::Validation(dep_name.to_string()),
                        "aggregator" => ComponentEnum::Aggregator(dep_name.to_string()),
                        _ => panic!("Invalid component type"),
                    })
            })
            .collect::<Result<Vec<ComponentEnum>, String>>()?;

        let cache_config = node_obj.get("cache")
            .and_then(|c| c.as_object())
            .map(|c| {
                let empty_map = serde_json::Map::new();

                let current = c.get("current")
                    .and_then(|v| v.as_object())
                    .unwrap_or(&empty_map);

                let history = c.get("history")
                    .and_then(|v| v.as_object())
                    .unwrap_or(&empty_map);

                CacheConfig {
                    current: CurrentCacheConfig {
                        cache_output: current.get("cache_output")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        use_cached: current.get("use_cached")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                    },
                    history: HistoryCacheConfig {
                        store_output: history.get("store_output")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(true),
                        storage: HistoryStorage::File(PathBuf::from("/tmp/dag_history.jsonl")),
                        load_from_history: history.get("load_from_history")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        use_history: history.get("use_history")
                            .and_then(|v| v.as_object())
                            .map(|v| HistorySource {
                                request_id: Uuid::parse_str(
                                    v.get("request_id")
                                        .and_then(|v| v.as_str())
                                        .ok_or("Missing request_id in use_history")
                                        .unwrap()
                                ).unwrap(),
                                strict: v.get("strict")
                                    .and_then(|v| v.as_bool())
                                    .unwrap_or(true),
                            }),
                    },
                }
            })
            .unwrap_or_default();

        components.insert(component_enum.clone(), component);
        let component_node = ComponentNode {
            dependencies,
            cache_config,
        };
        println!("Component Node: {:?}", component_node);
        nodes.insert(component_enum, component_node);
    }

    Ok((components, nodes))
}

// Updated main function
#[tokio::main]
async fn main() {
    // Define DAG using JSON (I had this request in my local file, you need to change it)
    let specific_request = Uuid::parse_str("1c8f864b-2431-4813-ae57-e059fd2feed9").unwrap();

    let dag_config = json!({
        "transform_a": {
            "type": "transform",
            "config": {
                "iterations": 1_000_000,
                "modulo": 1000
            },
            "cache": {
                "current": {
                    // when using history, we could populate the output cache...
                    //"cache_output": true,
                    // when using history, we don't want to use the cache
                    //"use_cached": true,
                },
                "history": {
                    "use_history": {
                        "request_id": specific_request.to_string(),
                        "strict": true  // Fail if this history isn't found
                    },
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
            },
            history: HistoryCacheConfig {
                store_output: true,
                storage: HistoryStorage::File(PathBuf::from("/tmp/dag_history.jsonl")),
                load_from_history: true,
                use_history: None,
            },
        }),
        None,
    ).await;
    let inputs = HashMap::new();
    let request_id = Uuid::new_v4();

    match executor.execute(component_nodes, inputs, request_id).await {
        Ok(output) => {
            println!("✅ Execution succeeded with output: {:?}", output);

            // Peek at just this request's history
            println!("\n📜 History for Request {}:", request_id);
            let request_history = executor.get_request_history(request_id).await;
            for (component, output) in request_history {
                println!(
                    "Component: {:?}\n  -> State: {:?}\n  -> Data: {}\n",
                    component,
                    output.state,
                    output.cacheable_data
                );
            }
        }
        Err(e) => println!("❌ Execution failed: {:?}", e),
    }
}

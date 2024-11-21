use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use futures::future::{join_all, BoxFuture};
use std::time::Instant;

// Placeholder Enums and Types
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
enum ComponentEnum {
    Example,
    Transform,
    Validation,
    Aggregator,
}

#[derive(Debug, Clone)]
struct BaseComponentInput;

#[derive(Debug, Clone)]
struct BaseComponentOutput {
    state: CompletionState,
}

#[derive(Debug, Clone, PartialEq)]
enum CompletionState {
    Success,
    Failure,
}

#[derive(Debug, Clone)]
struct ComponentNode {
    dependencies: Vec<ComponentEnum>,
}

#[derive(Debug, Clone)]
struct ComponentExecutionState {
    output: BaseComponentOutput,
    state: CompletionState,
}

// First, let's create a more specific error type
#[derive(Debug)]
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
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>>;
}

trait BlockingComponent: Send + Sync {
    fn name(&self) -> ComponentEnum;
    fn execute(&self, input: BaseComponentInput) -> Result<BaseComponentOutput, String>;
}

// Then define the execution mode enum
enum ComponentExecutionMode {
    Async(Arc<dyn AsyncComponent>),
    Blocking(Arc<dyn BlockingComponent>),
}

impl ComponentExecutionMode {
    async fn execute(&self, input: BaseComponentInput) -> Result<BaseComponentOutput, String> {
        match self {
            ComponentExecutionMode::Async(component) => {
                let component = component.clone();
                component.execute(input).await
            }
            ComponentExecutionMode::Blocking(component) => {
                let component = component.clone();
                tokio::task::spawn_blocking(move || {
                    component.execute(input)
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
}

impl ComponentsExecutorManager {
    fn new(components: HashMap<ComponentEnum, ComponentExecutionMode>) -> Self {
        Self { components: Arc::new(components) }
    }

    async fn execute(
        &self,
        component_nodes: HashMap<ComponentEnum, ComponentNode>,
        inputs: HashMap<ComponentEnum, BaseComponentOutput>,
    ) -> Result<BaseComponentOutput, ComponentError> {
        let total_start = Instant::now();

        let mut component_outputs: HashMap<ComponentEnum, Arc<Mutex<ComponentExecutionState>>> =
            inputs
                .into_iter()
                .map(|(key, output)| {
                    (
                        key,
                        Arc::new(Mutex::new(ComponentExecutionState {
                            output,
                            state: CompletionState::Success,
                        })),
                    )
                })
                .collect();

        let sorted_components = self.topologically_sort_components(&component_nodes)
            .map_err(|_| ComponentError::CyclicDependency)?;
        println!("🔄 Topological sort completed in {:?}", total_start.elapsed());

        for component_enum in &sorted_components {
            if !component_outputs.contains_key(component_enum) {
                let component_start = Instant::now();

                let components = self.components.clone();
                let component_enum_for_spawn = component_enum.clone();
                let component_enum_for_output = component_enum.clone();

                println!("⚡ Starting {:#?}", component_enum);

                let execution_result = tokio::spawn(async move {
                    let component = components
                        .get(&component_enum_for_spawn)
                        .ok_or_else(|| ComponentError::ComponentNotFound(component_enum_for_spawn.clone()))?;

                    match component.execute(BaseComponentInput).await {
                        Ok(output) => Ok(output),
                        Err(e) => Err(ComponentError::ExecutionError(e)),
                    }
                })
                .await;

                let output = match execution_result {
                    Ok(Ok(output)) => {
                        println!("✅ Completed {:#?} in {:?}", component_enum_for_output, component_start.elapsed());
                        output
                    }
                    Ok(Err(e)) => {
                        println!("❌ Component {:#?} failed with error: {:?}", component_enum_for_output, e);
                        return Err(e);
                    }
                    Err(e) => {
                        println!("💥 Component {:#?} panicked: {:?}", component_enum_for_output, e);
                        return Err(ComponentError::ExecutionError(format!(
                            "Component panicked: {}",
                            e
                        )));
                    }
                };

                let state = ComponentExecutionState {
                    output: output.clone(),
                    state: CompletionState::Success,
                };

                component_outputs.insert(component_enum_for_output, Arc::new(Mutex::new(state)));
            }
        }

        let leaf_component = sorted_components
            .last()
            .ok_or_else(|| ComponentError::ExecutionError("No components in DAG".to_string()))?;

        let final_state = component_outputs
            .get(leaf_component)
            .ok_or_else(|| ComponentError::ComponentNotFound(leaf_component.clone()))?
            .lock()
            .await
            .clone();

        println!("🏁 Total execution time: {:?}", total_start.elapsed());
        Ok(final_state.output)
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
}

// Example Component implementations
struct ExampleComponent;
impl AsyncComponent for ExampleComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Example
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        Box::pin(async move {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            println!("⚙️  ExampleComponent processing took {:?}", start.elapsed());
            Ok(BaseComponentOutput {
                state: CompletionState::Success,
            })
        })
    }
}

// Transform Component
struct TransformComponent;
impl BlockingComponent for TransformComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Transform
    }

    fn execute(&self, _input: BaseComponentInput) -> Result<BaseComponentOutput, String> {
        let start = Instant::now();

        // CPU-intensive work
        let mut result = 0u64;
        for i in 0..1_000_000 {
            result = result.wrapping_add(i % 1000);
        }

        println!("⚙️  TransformComponent processing took {:?}", start.elapsed());
        Ok(BaseComponentOutput {
            state: CompletionState::Success,
        })
    }
}

// Validation Component
struct ValidationComponent;
impl AsyncComponent for ValidationComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Validation
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
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
                })
            };
            result
        })
    }
}

// Aggregator Component
struct AggregatorComponent;
impl AsyncComponent for AggregatorComponent {
    fn name(&self) -> ComponentEnum {
        ComponentEnum::Aggregator
    }

    fn execute(
        &self,
        _input: BaseComponentInput,
    ) -> BoxFuture<'static, Result<BaseComponentOutput, String>> {
        Box::pin(async move {
            let start = Instant::now();
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            println!("⚙️  AggregatorComponent processing took {:?}", start.elapsed());
            Ok(BaseComponentOutput {
                state: CompletionState::Success,
            })
        })
    }
}

// Main function
#[tokio::main]
async fn main() {
    let components: HashMap<ComponentEnum, ComponentExecutionMode> = vec![
        (
            ComponentEnum::Example,
            ComponentExecutionMode::Async(Arc::new(ExampleComponent)),
        ),
        (
            ComponentEnum::Transform,
            ComponentExecutionMode::Blocking(Arc::new(TransformComponent)),
        ),
        (
            ComponentEnum::Validation,
            ComponentExecutionMode::Async(Arc::new(ValidationComponent)),
        ),
        (
            ComponentEnum::Aggregator,
            ComponentExecutionMode::Async(Arc::new(AggregatorComponent)),
        ),
    ]
    .into_iter()
    .collect();

    let component_nodes: HashMap<ComponentEnum, ComponentNode> = vec![
        (
            ComponentEnum::Example,
            ComponentNode {
                dependencies: vec![],
            },
        ),
        (
            ComponentEnum::Transform,
            ComponentNode {
                dependencies: vec![ComponentEnum::Example],
            },
        ),
        (
            ComponentEnum::Validation,
            ComponentNode {
                dependencies: vec![ComponentEnum::Transform],
            },
        ),
        (
            ComponentEnum::Aggregator,
            ComponentNode {
                dependencies: vec![ComponentEnum::Transform, ComponentEnum::Validation],
            },
        ),
    ]
    .into_iter()
    .collect();

    let executor = ComponentsExecutorManager::new(components);
    let inputs = HashMap::new();

    match executor.execute(component_nodes, inputs).await {
        Ok(output) => println!("✅ Execution succeeded with output: {:?}", output),
        Err(e) => match e {
            ComponentError::ExecutionError(msg) => println!("❌ Component execution failed: {}", msg),
            ComponentError::DependencyFailed(comp) => println!("❌ Dependency failed for component: {:?}", comp),
            ComponentError::ComponentNotFound(comp) => println!("❌ Component not found: {:?}", comp),
            ComponentError::CyclicDependency => println!("❌ Cyclic dependency detected in DAG"),
        },
    }
}
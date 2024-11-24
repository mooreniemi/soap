use indexmap::IndexMap;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task;

/// Type alias for a component box
type ComponentBox = Arc<Box<dyn Component<Input = Value, Output = Value>>>;

/// Type alias for a component factory
type ComponentFactory = Arc<dyn Fn(Value) -> ComponentBox + Send + Sync>;

/// Component trait
trait Component: Send + Sync + 'static {
    type Input;
    type Output;

    fn configure(config: Value) -> Self
    where
        Self: Sized;

    fn execute(&self, input: Self::Input) -> Self::Output;
}

/// Metadata structure for component validation
#[derive(Debug, Clone)]
struct ComponentMetadata {
    input_type: String,
    output_type: String,
}

/// Node structure
#[derive(Debug, Clone)]
struct Node {
    id: String,
    component_type: String,
    config: Value,
    depends_on: Vec<String>,
    inputs: Option<Value>,
}

/// DAG structure
#[derive(Debug)]
struct DAG {
    nodes: HashMap<String, Node>,
    edges: HashMap<String, Vec<String>>,
}

impl DAG {
    fn from_json(json_config: Value) -> Self {
        let mut dag = DAG {
            nodes: HashMap::new(),
            edges: HashMap::new(),
        };

        if let Some(nodes) = json_config.as_array() {
            for node in nodes {
                let id = node["id"].as_str().unwrap().to_string();
                let component_type = node["component_type"].as_str().unwrap().to_string();
                let config = node["config"].clone();
                let depends_on = node["depends_on"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect();
                let inputs = node.get("inputs").cloned();

                dag.add_node(Node {
                    id,
                    component_type,
                    config,
                    depends_on,
                    inputs,
                });
            }
        }

        dag
    }

    fn add_node(&mut self, node: Node) {
        self.edges.entry(node.id.clone()).or_insert(vec![]);
        for dep in &node.depends_on {
            self.edges
                .entry(dep.clone())
                .or_insert(vec![])
                .push(node.id.clone());
        }
        self.nodes.insert(node.id.clone(), node);
    }

    fn group_nodes_by_level(&self) -> Result<Vec<Vec<String>>, String> {
        let mut in_degree = HashMap::new();
        for (node, deps) in &self.edges {
            in_degree.entry(node).or_insert(0);
            for dep in deps {
                *in_degree.entry(dep).or_insert(0) += 1;
            }
        }

        let mut queue: Vec<String> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(node, _)| node.to_string())
            .collect();

        let mut levels = vec![];
        while !queue.is_empty() {
            let mut next_queue = vec![];
            levels.push(queue.clone());
            for node in queue {
                if let Some(neighbors) = self.edges.get(&node) {
                    for neighbor in neighbors {
                        if let Some(deg) = in_degree.get_mut(neighbor) {
                            *deg -= 1;
                            if *deg == 0 {
                                next_queue.push(neighbor.clone());
                            }
                        }
                    }
                }
            }
            queue = next_queue;
        }

        if levels.iter().flatten().count() == self.nodes.len() {
            Ok(levels)
        } else {
            Err("Cyclic dependency detected".to_string())
        }
    }

    fn validate_metadata(&self, registry: &ComponentRegistry) -> Result<(), String> {
        for (node_id, node) in &self.nodes {
            let metadata = registry.get_metadata(&node.component_type).ok_or_else(|| {
                format!(
                    "Component type '{}' not found for node '{}'",
                    node.component_type, node_id
                )
            })?;

            for dep_id in &node.depends_on {
                let dep_node = self.nodes.get(dep_id).ok_or_else(|| {
                    format!("Dependency '{}' not found for node '{}'", dep_id, node_id)
                })?;

                let dep_metadata =
                    registry
                        .get_metadata(&dep_node.component_type)
                        .ok_or_else(|| {
                            format!(
                                "Component type '{}' not found for dependency '{}'",
                                dep_node.component_type, dep_id
                            )
                        })?;

                if metadata.input_type != dep_metadata.output_type {
                    return Err(format!(
                        "Type mismatch: node '{}' expects input type '{}' but dependency '{}' produces type '{}'",
                        node_id, metadata.input_type, dep_id, dep_metadata.output_type
                    ));
                }
            }
        }
        Ok(())
    }

    async fn validate_and_execute(
        &self,
        registry: Arc<ComponentRegistry>,
        component_cache: Arc<Mutex<ConfiguredComponentCache>>,
    ) {
        if let Err(err) = self.validate_metadata(&registry) {
            println!("Validation error: {}", err);
            return;
        }

        match self.group_nodes_by_level() {
            Ok(levels) => {
                println!("Execution levels: {:?}", levels);

                let mut results: IndexMap<String, Value> = IndexMap::new();

                for level in levels {
                    let mut tasks = vec![];

                    for node_id in level {
                        let node = self.nodes.get(&node_id).unwrap().clone();
                        let registry = Arc::clone(&registry);
                        let component_cache = Arc::clone(&component_cache);
                        let results = results.clone();

                        tasks.push(task::spawn(async move {
                            println!("Executing node: {}", node.id);

                            let component: ComponentBox = {
                                let mut cache = component_cache.lock().unwrap();
                                if let Some(component) = cache.get(&node.id) {
                                    Arc::clone(component)
                                } else if let Some(factory) = registry.get(&node.component_type) {
                                    let new_component = factory(node.config.clone());
                                    cache.insert(node.id.clone(), Arc::clone(&new_component));
                                    new_component
                                } else {
                                    panic!("Unknown component type: {}", node.component_type);
                                }
                            };

                            let input = node
                                .inputs
                                .clone()
                                .or_else(|| {
                                    Some(Value::from(
                                        node.depends_on
                                            .iter()
                                            .filter_map(|dep_id| results.get(dep_id))
                                            .cloned()
                                            .collect::<Vec<_>>(),
                                    ))
                                })
                                .unwrap_or_else(|| Value::Null);

                            let output = component.execute(input);
                            println!("Node {} output: {}", node.id, output);
                            (node.id, output)
                        }));
                    }

                    for t in tasks {
                        let (node_id, output) = t.await.unwrap();
                        results.insert(node_id, output);
                    }
                }

                println!("Final Ordered Results: {:?}", results);
            }
            Err(err) => {
                println!("Error: {}", err);
            }
        }
    }
}

/// Component Registry
struct ComponentRegistry {
    registry: HashMap<String, ComponentFactory>,
    metadata: HashMap<String, ComponentMetadata>,
}

impl ComponentRegistry {
    fn new() -> Self {
        Self {
            registry: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    fn register<C: Component<Input = Value, Output = Value> + 'static>(
        &mut self,
        name: &str,
        factory: ComponentFactory,
        input_type: &str,
        output_type: &str,
    ) {
        self.registry.insert(name.to_string(), factory);
        self.metadata.insert(
            name.to_string(),
            ComponentMetadata {
                input_type: input_type.to_string(),
                output_type: output_type.to_string(),
            },
        );
    }

    fn get(&self, name: &str) -> Option<&ComponentFactory> {
        self.registry.get(name)
    }

    fn get_metadata(&self, name: &str) -> Option<&ComponentMetadata> {
        self.metadata.get(name)
    }
}

/// Configured Component Cache
type ConfiguredComponentCache = HashMap<String, ComponentBox>;

/// Adder component
struct Adder {
    value: i32,
}

impl Component for Adder {
    type Input = Value;
    type Output = Value;

    fn configure(config: Value) -> Self {
        Adder {
            value: config["value"].as_i64().unwrap() as i32,
        }
    }

    fn execute(&self, input: Self::Input) -> Self::Output {
        println!("Adder input: {:?}", input);
        let sum: i32 = input
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_i64().map(|v| v as i32))
            .sum();
        Value::from(sum + self.value)
    }
}

/// StringLengthCounter component
struct StringLengthCounter;

impl Component for StringLengthCounter {
    type Input = Value;
    type Output = Value;

    fn configure(_: Value) -> Self {
        StringLengthCounter
    }

    fn execute(&self, input: Self::Input) -> Self::Output {
        println!("StringLengthCounter input: {:?}", input);
        let len = input.as_str().unwrap_or("").len();
        Value::from(len as i32)
    }
}

#[tokio::main]
async fn main() {
    use serde_json::json;

    let dag_config = json!([
        {
            "id": "string_counter_1",
            "component_type": "StringLengthCounter",
            "config": {},
            "depends_on": [],
            "inputs": "Hello, world!"
        },
        {
            "id": "adder_1",
            "component_type": "Adder",
            "config": { "value": 5 },
            "depends_on": ["string_counter_1"]
        },
        {
            "id": "adder_2",
            "component_type": "Adder",
            "config": { "value": 10 },
            "depends_on": ["adder_1"]
        },
        {
            "id": "adder_3",
            "component_type": "Adder",
            "config": { "value": 15 },
            "depends_on": ["adder_1"]
        },
        {
            "id": "adder_4",
            "component_type": "Adder",
            "config": { "value": 20 },
            "depends_on": ["adder_2", "adder_3"]
        }
    ]);

    let mut registry = ComponentRegistry::new();

    registry.register::<Adder>(
        "Adder",
        Arc::new(|config| Arc::new(Box::new(Adder::configure(config)))),
        "integer",
        "integer",
    );

    registry.register::<StringLengthCounter>(
        "StringLengthCounter",
        Arc::new(|config| Arc::new(Box::new(StringLengthCounter::configure(config)))),
        "string",
        "integer",
    );

    let component_cache: Arc<Mutex<ConfiguredComponentCache>> =
        Arc::new(Mutex::new(HashMap::new()));

    let dag = DAG::from_json(dag_config);

    dag.validate_and_execute(Arc::new(registry), component_cache)
        .await;
}

use indexmap::IndexMap;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Runtime values that flow through the DAG
#[derive(Debug, Clone)]
pub enum Data {
    Null,
    Integer(i32),
    Text(String),
    List(Vec<Data>),
    Json(Value),
    /// A channel for single-consumer asynchronous results, wrapped in an `Arc<Mutex>` for safe sharing.
    OneConsumerChannel(Arc<Mutex<Option<oneshot::Receiver<Data>>>>),
    /// A channel for multi-consumer asynchronous results, inherently clonable.
    MultiConsumerChannel(watch::Receiver<Option<Data>>),
}

/// Type information for validation during DAG construction
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// Represents the absence of input for a component.
    Null,
    Integer,
    Text,
    List(Box<DataType>),
    Json,
    Union(Vec<DataType>),
    /// Represents a single-consumer channel carrying a specific data type.
    OneConsumerChannel(Box<DataType>),
    /// Represents a multi-consumer channel carrying a specific data type.
    MultiConsumerChannel(Box<DataType>),
}

impl DataType {
    /// Determines whether one `DataType` is compatible with another.
    ///
    /// This function checks if a value of the current `DataType` (`self`) can be
    /// safely used as input where the target `DataType` (`other`) is expected.
    /// It supports direct type equivalence, union compatibility, and nested list type compatibility.
    ///
    /// ### Compatibility Rules:
    /// - **Exact Match**: Two data types are directly compatible if they are equal.
    /// - **Union Compatibility**: A `DataType` is compatible with a `DataType::Union` if it is compatible
    ///   with at least one of the types in the union.
    /// - **List Compatibility**: Two `DataType::List` types are compatible if their element types are compatible.
    /// - **Otherwise**: The types are considered incompatible.
    ///
    /// ### Parameters:
    /// - `other`: The target `DataType` to check compatibility against.
    ///
    /// ### Returns:
    /// - `true` if `self` is compatible with `other`.
    /// - `false` otherwise.
    ///
    /// ### Examples:
    /// #### Example 1: Direct Compatibility
    /// ```rust
    /// let a = DataType::Integer;
    /// let b = DataType::Integer;
    /// assert!(a.is_compatible_with(&b)); // true
    /// ```
    ///
    /// #### Example 2: Union Compatibility
    /// ```rust
    /// let source = DataType::Text;
    /// let target = DataType::Union(vec![DataType::Integer, DataType::Text]);
    /// assert!(source.is_compatible_with(&target)); // true
    /// ```
    ///
    /// #### Example 3: List Compatibility
    /// ```rust
    /// let source = DataType::List(Box::new(DataType::Integer));
    /// let target = DataType::List(Box::new(DataType::Integer));
    /// assert!(source.is_compatible_with(&target)); // true
    /// ```
    ///
    /// #### Example 4: Nested List Compatibility
    /// ```rust
    /// let source = DataType::List(Box::new(DataType::List(Box::new(DataType::Text))));
    /// let target = DataType::List(Box::new(DataType::List(Box::new(DataType::Text))));
    /// assert!(source.is_compatible_with(&target)); // true
    /// ```
    ///
    /// #### Example 5: Incompatible Types
    /// ```rust
    /// let source = DataType::Integer;
    /// let target = DataType::Text;
    /// assert!(!source.is_compatible_with(&target)); // false
    /// ```
    pub fn is_compatible_with(&self, other: &DataType) -> bool {
        match (self, other) {
            (a, b) if a == b => true,

            (source_type, DataType::Union(target_types)) => target_types
                .iter()
                .any(|t| source_type.is_compatible_with(t)),

            (DataType::List(a), DataType::List(b)) => a.is_compatible_with(b),

            _ => false,
        }
    }
}

impl Data {
    pub fn as_integer(&self) -> Option<i32> {
        if let Data::Integer(v) = self {
            Some(*v)
        } else {
            None
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        if let Data::Text(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_list(&self) -> Option<&[Data]> {
        if let Data::List(v) = self {
            Some(v)
        } else {
            None
        }
    }

    fn get_type(&self) -> DataType {
        match self {
            Data::Null => DataType::Null,
            Data::Integer(_) => DataType::Integer,
            Data::Text(_) => DataType::Text,
            Data::List(items) => {
                if let Some(first) = items.first() {
                    DataType::List(Box::new(first.get_type()))
                } else {
                    DataType::List(Box::new(DataType::Integer))
                }
            }
            Data::Json(_) => DataType::Json,
            Data::OneConsumerChannel(_) => DataType::List(Box::new(DataType::Integer)),
            Data::MultiConsumerChannel(_) => DataType::List(Box::new(DataType::Integer)),
        }
    }
}

trait Component: Send + Sync + 'static {
    fn configure(config: Value) -> Self
    where
        Self: Sized;

    fn execute(&self, input: Data) -> Data;

    fn input_type(&self) -> DataType;
    fn output_type(&self) -> DataType;
}

struct ComponentRegistry {
    components: HashMap<String, Arc<dyn Fn(Value) -> Box<dyn Component>>>,
}

impl ComponentRegistry {
    fn new() -> Self {
        Self {
            components: HashMap::new(),
        }
    }

    fn register<C: Component + 'static>(&mut self, name: &str) {
        self.components.insert(
            name.to_string(),
            Arc::new(|config| Box::new(C::configure(config)) as Box<dyn Component>),
        );
    }

    fn get(&self, name: &str) -> Option<&Arc<dyn Fn(Value) -> Box<dyn Component>>> {
        self.components.get(name)
    }
}

#[derive(Debug)]
struct NodeIR {
    id: String,
    component_type: String,
    config: Value,
    depends_on: Vec<String>,
    inputs: Option<Data>,
}

#[derive(Debug)]
struct DAGIR {
    nodes: Vec<NodeIR>,
}

impl DAGIR {
    fn from_json(json_config: Value) -> Self {
        let nodes = json_config
            .as_array()
            .unwrap()
            .iter()
            .map(|node| NodeIR {
                id: node["id"].as_str().unwrap().to_string(),
                component_type: node["component_type"].as_str().unwrap().to_string(),
                config: node["config"].clone(),
                depends_on: node["depends_on"]
                    .as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect(),
                inputs: node.get("inputs").map(|v| match v {
                    Value::String(s) => Data::Text(s.clone()),
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Data::Integer(i as i32)
                        } else {
                            panic!("Unsupported number type in inputs");
                        }
                    }
                    Value::Array(arr) => {
                        let data_list = arr
                            .iter()
                            .filter_map(|item| match item {
                                Value::String(s) => Some(Data::Text(s.clone())),
                                Value::Number(n) => n.as_i64().map(|i| Data::Integer(i as i32)),
                                _ => None,
                            })
                            .collect();
                        Data::List(data_list)
                    }
                    Value::Object(_) => Data::Json(v.clone()),
                    _ => panic!("Unsupported input type in JSON configuration"),
                }),
            })
            .collect();

        DAGIR { nodes }
    }
}

struct DAG {
    nodes: Arc<HashMap<String, Box<dyn Component>>>,
    edges: Arc<HashMap<String, Vec<String>>>,
    initial_inputs: Arc<HashMap<String, Data>>,
}

impl DAG {
    /// Constructs a `DAG` instance from its intermediate representation (`DAGIR`) and a component registry.
    ///
    /// This method performs the following tasks:
    /// 1. Creates a mapping of node IDs to components by instantiating each component from its configuration.
    /// 2. Builds the dependency graph (`edges`) based on the `depends_on` relationships.
    /// 3. Validates the input types for each node:
    ///    - Ensures single-dependency nodes accept the output type of their upstream component.
    ///    - Ensures multi-dependency nodes declare an input type compatible with `Data::List`.
    /// 4. Collects any initial inputs provided for nodes.
    ///
    /// ### Validation:
    /// - If a node depends on multiple upstream nodes, it must declare an input type that
    ///   can accept a `Data::List` (directly or via a `DataType::Union` containing `DataType::List`).
    /// - Type mismatches are detected and reported with descriptive error messages.
    ///
    /// ### Errors:
    /// - Returns an error if:
    ///   - A node's component type is not found in the registry.
    ///   - A node's initial input type does not match the declared input type.
    ///   - A node's input type is incompatible with its dependencies.
    ///
    /// ### Parameters:
    /// - `ir`: The intermediate representation of the DAG (`DAGIR`).
    /// - `registry`: A registry of available component types (`ComponentRegistry`).
    ///
    /// ### Returns:
    /// - `Ok(Self)`: The constructed `DAG` instance.
    /// - `Err(String)`: An error message describing the validation failure.
    fn from_ir(ir: DAGIR, registry: &ComponentRegistry) -> Result<Self, String> {
        let mut nodes = HashMap::new();
        let mut edges = HashMap::new();
        let mut initial_inputs = HashMap::new();

        for node in ir.nodes {
            let factory = registry
                .get(&node.component_type)
                .ok_or_else(|| format!("Unknown component type: {}", node.component_type))?;
            let component = factory(node.config);

            if let Some(input) = &node.inputs {
                if !Self::validate_data_type(input, &component.input_type()) {
                    return Err(format!(
                        "Node {} initial input type mismatch. Expected {:?}, got {:?}",
                        node.id,
                        component.input_type(),
                        input.get_type()
                    ));
                }
                initial_inputs.insert(node.id.clone(), input.clone());
            }

            nodes.insert(node.id.clone(), component);
            edges.insert(node.id.clone(), node.depends_on);
        }

        for (node_id, deps) in &edges {
            let target_component = nodes.get(node_id).unwrap();
            let expected_input_type = target_component.input_type();

            if deps.len() == 1 {
                let source_component = nodes.get(&deps[0]).unwrap();
                let source_type = source_component.output_type();

                if !source_type.is_compatible_with(&expected_input_type) {
                    return Err(format!(
                        "Type mismatch: Node {} outputs {:?}, but node {} expects {:?}",
                        deps[0], source_type, node_id, expected_input_type
                    ));
                }
            } else if deps.len() > 1 {
                let accepts_list = match &expected_input_type {
                    DataType::List(_) => true,
                    DataType::Union(types) => types.iter().any(|t| matches!(t, DataType::List(_))),
                    _ => false,
                };

                if !accepts_list {
                    return Err(format!(
                            "Validation error: Node {} depends on multiple nodes ({:?}) but its input type {:?} does not accept a List",
                            node_id, deps, expected_input_type
                        ));
                }

                let expected_element_type = match &expected_input_type {
                    DataType::List(elem_type) => elem_type,
                    DataType::Union(types) => types
                        .iter()
                        .find_map(|t| match t {
                            DataType::List(elem_type) => Some(elem_type),
                            _ => None,
                        })
                        .unwrap(),
                    _ => unreachable!(),
                };

                for dep in deps {
                    let source_component = nodes.get(dep).unwrap();
                    let source_type = source_component.output_type();

                    if !source_type.is_compatible_with(expected_element_type) {
                        return Err(format!(
                                "Type mismatch in list input: Node {} outputs {:?}, but node {} expects elements of type {:?}",
                                dep, source_type, node_id, expected_element_type
                            ));
                    }
                }
            }
        }

        Ok(Self {
            nodes: Arc::new(nodes),
            edges: Arc::new(edges),
            initial_inputs: Arc::new(initial_inputs),
        })
    }

    fn validate_data_type(data: &Data, expected_type: &DataType) -> bool {
        match expected_type {
            DataType::Null => matches!(data, Data::Null),
            DataType::Union(types) => types.iter().any(|t| Self::validate_data_type(data, t)),
            DataType::Integer => matches!(data, Data::Integer(_)),
            DataType::Text => matches!(data, Data::Text(_)),
            DataType::List(element_type) => {
                if let Data::List(items) = data {
                    items
                        .iter()
                        .all(|item| Self::validate_data_type(item, element_type))
                } else {
                    false
                }
            }
            DataType::Json => matches!(data, Data::Json(_)),
            DataType::OneConsumerChannel(_) => matches!(data, Data::OneConsumerChannel(_)),
            DataType::MultiConsumerChannel(_) => matches!(data, Data::MultiConsumerChannel(_)),
        }
    }

    async fn execute(&self) -> Result<(), String> {
        let mut results: IndexMap<String, Data> = IndexMap::new();
        results.extend((*self.initial_inputs).clone());

        let sorted_nodes = self.topological_sort()?;
        let levels = self.group_into_levels(sorted_nodes)?;

        self.execute_levels(levels, results).await
    }

    fn group_into_levels(&self, sorted_nodes: Vec<String>) -> Result<Vec<Vec<String>>, String> {
        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut remaining_nodes: HashSet<String> = sorted_nodes.into_iter().collect();

        while !remaining_nodes.is_empty() {
            let mut current_level = Vec::new();

            let ready_nodes: Vec<_> = remaining_nodes
                .iter()
                .filter(|node| {
                    let deps = self.edges.get(*node).map(|d| d.as_slice()).unwrap_or(&[]);

                    deps.iter().all(|dep| !remaining_nodes.contains(dep))
                })
                .cloned()
                .collect();

            if ready_nodes.is_empty() && !remaining_nodes.is_empty() {
                return Err("Cycle detected in DAG".to_string());
            }

            for node in ready_nodes {
                current_level.push(node.clone());
                remaining_nodes.remove(&node);
            }

            levels.push(current_level);
        }

        Ok(levels)
    }

    async fn execute_levels(
        &self,
        levels: Vec<Vec<String>>,
        mut results: IndexMap<String, Data>,
    ) -> Result<(), String> {
        for (level_idx, level) in levels.iter().enumerate() {
            println!("Executing level {}: {:?}", level_idx, level);

            let level_results = self.execute_level(level, &results).await?;
            results.extend(level_results);
        }

        println!("Final results: {:?}", results);
        Ok(())
    }

    async fn execute_level(
        &self,
        level: &[String],
        results: &IndexMap<String, Data>,
    ) -> Result<Vec<(String, Data)>, String> {
        let nodes = Arc::clone(&self.nodes);
        let edges = Arc::clone(&self.edges);
        let initial_inputs = Arc::clone(&self.initial_inputs);

        let level_results = futures::future::join_all(level.iter().map(|node_id| {
            let node_id = node_id.clone();
            let results = results.clone();
            let nodes = Arc::clone(&nodes);
            let edges = Arc::clone(&edges);
            let initial_inputs = Arc::clone(&initial_inputs);

            tokio::spawn(async move {
                Self::execute_node(&node_id, &results, &nodes, &edges, &initial_inputs)
            })
        }))
        .await;

        level_results
            .into_iter()
            .map(|res| match res {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(format!("Task execution error: {}", e)),
            })
            .collect()
    }

    fn execute_node(
        node_id: &str,
        results: &IndexMap<String, Data>,
        nodes: &HashMap<String, Box<dyn Component>>,
        edges: &HashMap<String, Vec<String>>,
        initial_inputs: &HashMap<String, Data>,
    ) -> Result<(String, Data), String> {
        let component = nodes
            .get(node_id)
            .ok_or_else(|| format!("Node {} not found", node_id))?;

        let expected_input_type = component.input_type();

        let input_data = if expected_input_type == DataType::Null {
            Data::Json(serde_json::Value::Null)
        } else {
            Self::prepare_input_data(
                node_id,
                edges.get(node_id).map(|d| d.as_slice()).unwrap_or(&[]),
                results,
                initial_inputs,
            )?
        };

        if expected_input_type != DataType::Null
            && !Self::validate_data_type(&input_data, &expected_input_type)
        {
            return Err(format!(
                "Runtime type mismatch at node {}. Expected {:?}, got {:?}",
                node_id,
                expected_input_type,
                input_data.get_type()
            ));
        }

        let output = component.execute(input_data);

        if !Self::validate_data_type(&output, &component.output_type()) {
            return Err(format!(
                "Component {} produced invalid output type. Expected {:?}, got {:?}",
                node_id,
                component.output_type(),
                output.get_type()
            ));
        }

        Ok((node_id.to_string(), output))
    }

    fn prepare_input_data(
        node_id: &str,
        deps: &[String],
        results: &IndexMap<String, Data>,
        initial_inputs: &HashMap<String, Data>,
    ) -> Result<Data, String> {
        if deps.is_empty() {
            Ok(initial_inputs
                .get(node_id)
                .cloned()
                .unwrap_or(Data::List(vec![])))
        } else if deps.len() == 1 {
            results
                .get(&deps[0])
                .cloned()
                .ok_or_else(|| format!("Missing output from dependency: {}", deps[0]))
        } else {
            Ok(Data::List(
                deps.iter()
                    .map(|dep| {
                        results
                            .get(dep)
                            .cloned()
                            .ok_or_else(|| format!("Missing output from dependency: {}", dep))
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            ))
        }
    }

    fn topological_sort(&self) -> Result<Vec<String>, String> {
        let mut in_degree = HashMap::new();
        let mut zero_in_degree = vec![];
        let mut sorted = vec![];

        for node in self.nodes.keys() {
            in_degree.insert(node.clone(), 0);
        }

        let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

        for (node, deps) in self.edges.as_ref() {
            *in_degree.entry(node.clone()).or_insert(0) += deps.len() as i32;

            for dep in deps {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(node.clone());
            }
        }

        for (node, &deg) in &in_degree {
            if deg == 0 {
                zero_in_degree.push(node.clone());
            }
        }

        while let Some(node) = zero_in_degree.pop() {
            sorted.push(node.clone());

            if let Some(dependent_nodes) = reverse_deps.get(&node) {
                for dep_node in dependent_nodes {
                    if let Some(deg) = in_degree.get_mut(dep_node) {
                        *deg -= 1;
                        if *deg == 0 {
                            zero_in_degree.push(dep_node.clone());
                        }
                    }
                }
            }
        }

        if sorted.len() != self.nodes.len() {
            return Err("Cyclic dependency detected".to_string());
        }

        Ok(sorted)
    }
}

struct Adder {
    value: i32,
}

impl Component for Adder {
    fn configure(config: Value) -> Self {
        Adder {
            value: config["value"].as_i64().unwrap() as i32,
        }
    }

    fn execute(&self, input: Data) -> Data {
        println!("Adder input: {:?}", input);
        let input_value = match input {
            Data::Integer(v) => v,
            Data::List(list) => list.iter().filter_map(|v| v.as_integer()).sum(),
            _ => 0,
        };

        Data::Integer(input_value + self.value)
    }

    fn input_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Integer,
            DataType::List(Box::new(DataType::Integer)),
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}

struct StringLengthCounter;

impl Component for StringLengthCounter {
    fn configure(_: Value) -> Self {
        StringLengthCounter
    }

    fn execute(&self, input: Data) -> Data {
        let len = input.as_text().unwrap_or("").len();
        Data::Integer(len as i32)
    }

    fn input_type(&self) -> DataType {
        DataType::Text
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }
}

pub struct WildcardProcessor {
    expected_input_keys: HashSet<String>,
    expected_output_keys: HashSet<String>,
}

impl Component for WildcardProcessor {
    fn configure(config: Value) -> Self {
        let expected_input_keys = config["expected_input_keys"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let expected_output_keys = config["expected_output_keys"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        WildcardProcessor {
            expected_input_keys,
            expected_output_keys,
        }
    }

    fn execute(&self, input: Data) -> Data {
        println!("WildcardProcessor input: {:?}", input);
        match input {
            Data::Json(mut value) => {
                let mut fallback_map = serde_json::Map::new();

                let input_object = value.as_object_mut().unwrap_or(&mut fallback_map);

                for key in &self.expected_input_keys {
                    if !input_object.contains_key(key) {
                        return Data::Json(json!({ "error": format!("Missing key: {}", key) }));
                    }
                }

                let mut output_object = serde_json::Map::new();

                for key in &self.expected_output_keys {
                    if let Some(v) = input_object.get(key) {
                        output_object.insert(key.clone(), v.clone());
                    } else {
                        output_object.insert(key.clone(), json!(null));
                    }
                }

                Data::Json(Value::Object(output_object))
            }
            _ => Data::Json(json!({ "error": "Invalid input type, expected JSON" })),
        }
    }

    fn input_type(&self) -> DataType {
        DataType::Json
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}

pub struct FlexibleWildcardProcessor;

impl Component for FlexibleWildcardProcessor {
    fn configure(_: Value) -> Self {
        FlexibleWildcardProcessor
    }

    fn execute(&self, input: Data) -> Data {
        println!("FlexibleWildcardProcessor input: {:?}", input);
        let json_input = match input {
            Data::Null => json!({ "type": "null" }),
            Data::Json(value) => value,
            Data::Integer(i) => json!({ "type": "integer", "value": i }),
            Data::Text(t) => json!({ "type": "text", "value": t }),
            Data::List(list) => {
                let json_list: Vec<_> = list
                    .into_iter()
                    .map(|item| match item {
                        Data::Integer(i) => json!({ "type": "integer", "value": i }),
                        Data::Text(t) => json!({ "type": "text", "value": t }),
                        _ => json!({ "type": "unknown" }),
                    })
                    .collect();
                json!({ "type": "list", "values": json_list })
            }
            Data::OneConsumerChannel(_) => {
                json!({ "type": "one_consumer_channel" })
            }
            Data::MultiConsumerChannel(_) => {
                json!({ "type": "multi_consumer_channel" })
            }
        };

        Data::Json(json_input)
    }

    fn input_type(&self) -> DataType {
        DataType::Union(vec![
            DataType::Json,
            DataType::Integer,
            DataType::Text,
            DataType::List(Box::new(DataType::Union(vec![
                DataType::Integer,
                DataType::Text,
            ]))),
        ])
    }

    fn output_type(&self) -> DataType {
        DataType::Json
    }
}

pub struct LongRunningTask;

impl Component for LongRunningTask {
    fn configure(_: Value) -> Self {
        LongRunningTask
    }

    fn execute(&self, _input: Data) -> Data {
        let (tx, rx) = oneshot::channel();

        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

            let result = Data::Integer(42);
            let _ = tx.send(result);
        });

        Data::OneConsumerChannel(Arc::new(Mutex::new(Some(rx))))
    }

    fn input_type(&self) -> DataType {
        DataType::Null
    }

    fn output_type(&self) -> DataType {
        DataType::OneConsumerChannel(Box::new(DataType::Integer))
    }
}

pub struct ChannelConsumer {
    timeout_secs: Option<u64>,
}

impl Component for ChannelConsumer {
    fn configure(config: Value) -> Self {
        let timeout_secs = config["timeout_secs"].as_u64();
        ChannelConsumer { timeout_secs }
    }

    fn input_type(&self) -> DataType {
        DataType::OneConsumerChannel(Box::new(DataType::Integer))
    }

    fn output_type(&self) -> DataType {
        DataType::Integer
    }

    fn execute(&self, input: Data) -> Data {
        println!("ChannelConsumer input: {:?}", input);
        if let Data::OneConsumerChannel(channel) = input {
            let receiver = channel.clone();
            let timeout_duration = self.timeout_secs.map(Duration::from_secs);

            let result = tokio::task::block_in_place(|| {
                let mut receiver = receiver.blocking_lock();
                if let Some(rx) = receiver.take() {
                    let fut = async move {
                        if let Some(duration) = timeout_duration {
                            timeout(duration, rx).await.map_err(|_| "Timeout exceeded")
                        } else {
                            Ok(rx.await)
                        }
                    };

                    tokio::runtime::Handle::current().block_on(fut)
                } else {
                    Err("Channel already consumed".into())
                }
            });

            match result {
                Ok(Ok(data)) => {
                    println!("ChannelConsumer output: {:?}", data);
                    data
                }
                Ok(Err(_)) | Err(_) => {
                    eprintln!("ChannelConsumer: Timed out or failed to receive data from channel.");
                    Data::Integer(-1)
                }
            }
        } else {
            eprintln!("ChannelConsumer: Invalid input type.");
            Data::Integer(-1)
        }
    }
}

#[tokio::main]
async fn main() {
    let json_config = json!([
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
            "depends_on": ["adder_1", "adder_2"]
        },
        {
            "id": "wildcard_1",
            "component_type": "WildcardProcessor",
            "config": {
                "expected_input_keys": ["key1", "key2"],
                "expected_output_keys": ["key2", "key3"]
            },
            "depends_on": [],
            "inputs": { "key1": "value1", "key2": 42 }
        },
        {
            "id": "wildcard_2",
            "component_type": "WildcardProcessor",
            "config": {
                "expected_input_keys": ["key2", "key3"],
                "expected_output_keys": ["key1"]
            },
            "depends_on": ["wildcard_1"]
        },
        {
            "id": "flexible_wildcard_1",
            "component_type": "FlexibleWildcardProcessor",
            "config": {},
            "depends_on": ["adder_3"],
            "inputs": { "key1": "value1", "key2": 42 }
        },
        {
            "id": "long_task",
            "component_type": "LongRunningTask",
            "config": {},
            "depends_on": []
        },
        {
            "id": "consumer",
            "component_type": "ChannelConsumer",
            "config": {},
            "depends_on": ["long_task"]
        }
    ]);

    let mut registry = ComponentRegistry::new();
    registry.register::<Adder>("Adder");
    registry.register::<StringLengthCounter>("StringLengthCounter");
    registry.register::<WildcardProcessor>("WildcardProcessor");
    registry.register::<FlexibleWildcardProcessor>("FlexibleWildcardProcessor");
    registry.register::<LongRunningTask>("LongRunningTask");
    registry.register::<ChannelConsumer>("ChannelConsumer");

    let dag_ir = DAGIR::from_json(json_config);

    match DAG::from_ir(dag_ir, &registry) {
        Ok(dag) => {
            if let Err(err) = dag.execute().await {
                eprintln!("Execution error: {}", err);
            }
        }
        Err(err) => eprintln!("DAG construction error: {}", err),
    }
}

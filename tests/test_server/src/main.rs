use std::{collections::HashMap, sync::Arc};

use soap::dag_orchestrator::{create_dag_router, InMemoryCache, NodeInput, NodeOutput};
use tracing::info;

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
                    error: Some("invalid parameter type: multiplier must be a number".to_string()),
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

/// The port the test server will run on.
const PORT: u16 = 3039;

/// This test server loads nodes our integration test suite can use to validate the orchestrator.
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let mut handlers: HashMap<String, fn(NodeInput) -> NodeOutput> = HashMap::new();
    handlers.insert("A".to_string(), handler_a as fn(NodeInput) -> NodeOutput);
    handlers.insert("B".to_string(), handler_b as fn(NodeInput) -> NodeOutput);
    handlers.insert("C".to_string(), handler_c as fn(NodeInput) -> NodeOutput);
    handlers.insert("D".to_string(), handler_d as fn(NodeInput) -> NodeOutput);

    let cache = Arc::new(InMemoryCache::new(300)); // 5 minute TTL

    let app = create_dag_router(handlers, cache, true);

    info!("Server running on http://localhost:{PORT}");
    axum::Server::bind(&format!("0.0.0.0:{PORT}").parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

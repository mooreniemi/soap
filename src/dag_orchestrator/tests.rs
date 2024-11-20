mod tests {
    use std::{collections::HashMap, sync::{Arc, Mutex}};

    use axum::{body::HttpBody, extract::{Path, State}, response::IntoResponse, Json};
    use reqwest::StatusCode;

    use crate::dag_orchestrator::{endpoint_handler, Cache, DagConfig, NodeDefinition, NodeInput, NodeOutput, Orchestrator};


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

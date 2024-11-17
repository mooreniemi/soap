use std::time::Instant;
use axum::extract::Json;
use serde::Deserialize;
use tracing::info;
use std::sync::Arc;
use crate::versioned_modules::VersionedModules;
use super::Snake;
use serde_json::Value;

pub struct FeatureEngineeringHandler;

#[derive(Deserialize)]
pub struct FeatureEngineeringInput {
    data: Vec<f64>,
    version: Option<String>,
}

// Simple new method
impl FeatureEngineeringInput {
    pub fn new(data: Vec<f64>) -> Self {
        Self {
            data,
            version: None,
        }
    }

    // Optional: Add a with_version method for chaining
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
}

// Or if you prefer a builder pattern:
#[derive(Default)]
pub struct FeatureEngineeringInputBuilder {
    data: Option<Vec<f64>>,
    version: Option<String>,
}

impl FeatureEngineeringInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn data(mut self, data: Vec<f64>) -> Self {
        self.data = Some(data);
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn build(self) -> FeatureEngineeringInput {
        FeatureEngineeringInput {
            data: self.data.expect("data is required"),
            version: self.version,
        }
    }
}

impl Snake for FeatureEngineeringHandler {
    type Input = FeatureEngineeringInput;

    fn script_name() -> &'static str {
        "feature_engineering"
    }

    fn function_name() -> &'static str {
        "engineer_features"
    }

    async fn handle(
        input: Json<Self::Input>,
        versioned_modules: Arc<VersionedModules>,
    ) -> Json<Value> {
        let total_start = Instant::now();

        let lock_start = Instant::now();
        let (actual_version, modules) = versioned_modules
            .get_module_with_version(Self::script_name(), input.version.clone())
            .expect("Failed to get module");
        let lock_duration = lock_start.elapsed();

        let gil_start = Instant::now();
        let result: Vec<f64> = VersionedModules::call_module_function(
            &modules,
            Self::function_name(),
            (input.data.clone(),),
            &versioned_modules.interpreter_semaphore,
        ).await.expect("Failed to call engineer_features");
        let gil_duration = gil_start.elapsed();

        let total_duration = total_start.elapsed();

        let response = serde_json::json!({
            "metrics": {
                "lock_duration_ms": lock_duration.as_millis(),
                "gil_duration_ms": gil_duration.as_millis(),
                "total_duration_ms": total_duration.as_millis(),
            },
            "version": {
                "requested": input.version,
                "actual": actual_version,
            },
            "result": result,
        });

        info!(
            "Metrics: {}",
            serde_json::to_string(&response["metrics"]).unwrap()
        );
        Json(response)
    }
}
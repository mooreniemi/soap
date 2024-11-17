use std::time::Instant;
use axum::extract::Json;
use serde::Deserialize;
use tracing::info;
use std::sync::Arc;
use crate::versioned_modules::VersionedModules;
use super::Snake;
use serde_json::Value;

pub struct OneHotHandler;

#[derive(Deserialize)]
pub struct OneHotInput {
    pub categories: Vec<String>,
    pub version: Option<String>,
}

impl OneHotInput {
    pub fn new(categories: Vec<String>) -> Self {
        Self {
            categories,
            version: None,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
}

#[derive(Default)]
pub struct OneHotInputBuilder {
    categories: Option<Vec<String>>,
    version: Option<String>,
}

impl OneHotInputBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn categories(mut self, categories: Vec<String>) -> Self {
        self.categories = Some(categories);
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn build(self) -> OneHotInput {
        OneHotInput {
            categories: self.categories.expect("categories is required"),
            version: self.version,
        }
    }
}

impl Snake for OneHotHandler {
    type Input = OneHotInput;

    fn script_name() -> &'static str {
        "one_hot"
    }

    fn function_name() -> &'static str {
        "one_hot_encode"
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
        let result: Vec<Vec<u8>> = VersionedModules::call_module_function(
            &modules,
            Self::function_name(),
            (input.categories.clone(),),
            &versioned_modules.interpreter_semaphore,
        ).await.expect("Failed to call one_hot_encode");
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

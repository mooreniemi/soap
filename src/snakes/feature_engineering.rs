use axum::extract::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use crate::versioned_modules::VersionedModules;
use super::{Snake, VersionedInput, Metrics};
use tracing::{debug, error};

pub struct FeatureEngineeringHandler;

impl Snake for FeatureEngineeringHandler {
    type InputData = Vec<f64>;

    fn script_name() -> &'static str {
        "feature_engineering"
    }

    fn function_name() -> &'static str {
        "engineer_features"
    }

    async fn handle(
        input: Json<VersionedInput<Self::InputData>>,
        versioned_modules: Arc<VersionedModules>,
    ) -> Response {
        let mut metrics = Metrics::new();

        debug!("Processing feature engineering request with input length: {}", input.data.len());

        metrics.start_lock();
        let (actual_version, ref modules) = match versioned_modules
            .get_module_with_version(Self::script_name(), input.version.clone())
        {
            Some(result) => result,
            None => {
                error!("Module not found: {}", Self::script_name());
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to get Python module").into_response();
            }
        };
        metrics.end_lock();

        debug!("Got module version: {}", actual_version);

        metrics.start_gil();
        let result: Vec<f64> = match VersionedModules::call_module_function(
            &modules,
            Self::function_name(),
            (input.data.clone(),),
            &versioned_modules.interpreter_semaphore,
        ).await {
            Ok(result) => result,
            Err(e) => {
                error!("Python error: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Python function call failed").into_response();
            }
        };
        metrics.end_gil();

        debug!("Got result with length: {}", result.len());

        Json(metrics.build_response(result, (input.version.clone(), actual_version))).into_response()
    }
}
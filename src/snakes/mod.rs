pub mod onehot;
pub mod feature_engineering;

use axum::extract::Json;
use std::sync::Arc;
use serde::Deserialize;
use serde_json::Value;
use crate::versioned_modules::VersionedModules;

/// Trait for snake handlers that process Python-based transformations
pub trait Snake {
    /// The input type for this snake handler
    type Input: for<'de> Deserialize<'de>;
    
    /// The name of the script to use
    fn script_name() -> &'static str;

    /// The function name to call in the Python module
    fn function_name() -> &'static str;
    
    /// Process the input and return a JSON response
    async fn handle(
        input: Json<Self::Input>,
        versioned_modules: Arc<VersionedModules>,
    ) -> Json<Value>;
}
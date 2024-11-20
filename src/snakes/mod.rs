pub mod onehot;
pub mod feature_engineering;

use axum::extract::Json;
use std::sync::Arc;
use serde::Deserialize;
use serde_json::Value;
use crate::versioned_modules::VersionedModules;
use std::time::{Instant, Duration};
use tracing::debug;
use axum::response::Response;


/// Trait for snake handlers that process Python-based transformations
pub trait Snake {
    type InputData;

    /// The name of the script to use
    /// Will error if the script does not exist
    fn script_name() -> &'static str;

    /// The name of the function to use inside the script
    /// Will error if this function does not exist
    fn function_name() -> &'static str;

    async fn handle(
        input: Json<VersionedInput<Self::InputData>>,
        versioned_modules: Arc<VersionedModules>,
    ) -> Response;
}

// Common input structure that all snakes share
#[derive(Deserialize)]
pub struct VersionedInput<T> {
    pub data: T,
    pub version: Option<String>,
}

// Common builder pattern
pub struct VersionedInputBuilder<T> {
    data: Option<T>,
    version: Option<String>,
}

impl<T> VersionedInputBuilder<T> {
    pub fn new() -> Self {
        Self {
            data: None,
            version: None,
        }
    }

    pub fn data(mut self, data: T) -> Self {
        self.data = Some(data);
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    pub fn build(self) -> VersionedInput<T> {
        VersionedInput {
            data: self.data.expect("data is required"),
            version: self.version,
        }
    }
}

// Common metrics collection
pub struct Metrics {
    total_start: Instant,
    lock_start: Option<Instant>,
    lock_duration: Option<Duration>,
    gil_start: Option<Instant>,
    gil_duration: Option<Duration>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            total_start: Instant::now(),
            lock_start: None,
            lock_duration: None,
            gil_start: None,
            gil_duration: None,
        }
    }

    pub fn start_lock(&mut self) {
        self.lock_start = Some(Instant::now());
    }

    pub fn end_lock(&mut self) {
        if let Some(start) = self.lock_start {
            self.lock_duration = Some(start.elapsed());
            self.lock_start = None;
        }
    }

    pub fn start_gil(&mut self) {
        self.gil_start = Some(Instant::now());
    }

    pub fn end_gil(&mut self) {
        if let Some(start) = self.gil_start {
            self.gil_duration = Some(start.elapsed());
            self.gil_start = None;
        }
    }

    pub fn build_response(&self, result: impl serde::Serialize, version_info: (Option<String>, String)) -> Value {
        let (requested_version, actual_version) = version_info;
        let total_duration = self.total_start.elapsed();

        let response = serde_json::json!({
            "metrics": {
                "lock_duration_ms": self.lock_duration.unwrap_or_default().as_millis(),
                "gil_duration_ms": self.gil_duration.unwrap_or_default().as_millis(),
                "total_duration_ms": total_duration.as_millis(),
            },
            "version": {
                "requested": requested_version,
                "actual": actual_version,
            },
            "result": result,
        });

        debug!(
            "Metrics: {}",
            serde_json::to_string(&response["metrics"]).unwrap()
        );
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_metrics_total_duration() {
        let metrics = Metrics::new();
        thread::sleep(Duration::from_millis(10));
        let total = metrics.total_start.elapsed();
        assert!(total >= Duration::from_millis(10));
    }

    #[test]
    fn test_metrics_lock_timing() {
        let mut metrics = Metrics::new();

        // Start lock timing
        metrics.start_lock();
        thread::sleep(Duration::from_millis(10));
        metrics.end_lock();

        assert!(metrics.lock_duration.is_some());
        assert!(metrics.lock_duration.unwrap() >= Duration::from_millis(10));

        // Lock duration should not change after ending
        let first_duration = metrics.lock_duration.unwrap();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(metrics.lock_duration.unwrap(), first_duration);
    }

    #[test]
    fn test_metrics_gil_timing() {
        let mut metrics = Metrics::new();

        // Start GIL timing
        metrics.start_gil();
        thread::sleep(Duration::from_millis(10));
        metrics.end_gil();

        assert!(metrics.gil_duration.is_some());
        assert!(metrics.gil_duration.unwrap() >= Duration::from_millis(10));

        // GIL duration should not change after ending
        let first_duration = metrics.gil_duration.unwrap();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(metrics.gil_duration.unwrap(), first_duration);
    }

    #[test]
    fn test_metrics_multiple_operations() {
        let mut metrics = Metrics::new();

        // First operation
        metrics.start_lock();
        thread::sleep(Duration::from_millis(10));
        metrics.end_lock();
        let first_lock = metrics.lock_duration.unwrap();

        // Second operation
        metrics.start_lock();
        thread::sleep(Duration::from_millis(20));
        metrics.end_lock();
        let second_lock = metrics.lock_duration.unwrap();

        // Second duration should be longer
        assert!(second_lock > first_lock);
    }

    #[test]
    fn test_metrics_build_response() {
        let mut metrics = Metrics::new();

        // Simulate some operations
        metrics.start_lock();
        thread::sleep(Duration::from_millis(10));
        metrics.end_lock();

        metrics.start_gil();
        thread::sleep(Duration::from_millis(20));
        metrics.end_gil();

        let response = metrics.build_response(
            "test_result",
            (Some("1.0.0".to_string()), "2.0.0".to_string())
        );

        let metrics_obj = response.get("metrics").unwrap();
        assert!(metrics_obj.get("lock_duration_ms").unwrap().as_u64().unwrap() >= 10);
        assert!(metrics_obj.get("gil_duration_ms").unwrap().as_u64().unwrap() >= 20);
        assert!(metrics_obj.get("total_duration_ms").unwrap().as_u64().unwrap() >= 30);

        assert_eq!(
            response.get("version").unwrap().get("requested").unwrap(),
            "1.0.0"
        );
        assert_eq!(
            response.get("version").unwrap().get("actual").unwrap(),
            "2.0.0"
        );
    }
}
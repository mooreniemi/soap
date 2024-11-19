use axum::error_handling::HandleErrorLayer;
use axum::response::{IntoResponse, Response};
use axum::{BoxError, Json};
use axum::{
    middleware,
};
use soap::dag_orchestrator::create_dag_router;
use soap::snakes::{Snake, VersionedInput};
use soap::versioned_modules::VersionedModules;
use soap::watcher;
use soap::
    snakes::{
        onehot::OneHotHandler,
        feature_engineering::FeatureEngineeringHandler,
    }
;
use tracing::{debug, error, info};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber;
use tokio::signal;
use clap::Parser;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use axum::{
    middleware::Next,
    body::Body,
    http::Request,
};
use tower::ServiceBuilder;
use std::convert::Infallible;
use std::cell::Cell;
use axum::http::StatusCode;
use axum::body::HttpBody;
use std::collections::HashMap;
use soap::dag_orchestrator::{NodeInput, NodeOutput, InMemoryCache};
use std::sync::OnceLock;

thread_local! {
    static REQUEST_START: Cell<Option<Instant>> = Cell::new(None);
}

const REQUEST_BUFFER_SIZE: usize = 1024;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing Python scripts
    #[arg(long, env = "SCRIPTS_DIR")]
    scripts_dir: Option<String>,

    /// Number of Python interpreters to use
    #[arg(long, env = "NUM_INTERPRETERS", default_value = "1")]
    num_interpreters: usize,
}

static VERSIONED_MODULES: OnceLock<Arc<VersionedModules>> = OnceLock::new();

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    info!("Args: {:?}", args);

    // Log Python environment
    // These are set from .cargo/config.toml
    info!("Python home: {}", env::var("PYTHONHOME").unwrap_or_default());
    info!("Python path: {}", env::var("PYTHONPATH").unwrap_or_default());

    // Get scripts directory, falling back to CARGO_MANIFEST_DIR
    let scripts_dir = Arc::new(args.scripts_dir.unwrap_or_else(|| {
        env::var("CARGO_MANIFEST_DIR").expect("Neither SCRIPTS_DIR nor CARGO_MANIFEST_DIR is set")
    }));

    // Load initial modules
    let versioned_modules = Arc::new(VersionedModules::load_modules(
        &scripts_dir,
        args.num_interpreters,
    )?);

    // Set up the global VersionedModules
    if VERSIONED_MODULES.set(versioned_modules.clone()).is_err() {
        return Err("Failed to set global VersionedModules".into());
    }

    // Set up file watcher
    watcher::spawn_file_watcher(
        scripts_dir.clone(),
        versioned_modules.clone(),
    )?;

    // Create handlers map for DAG orchestrator
    let mut handlers: HashMap<String, fn(NodeInput) -> NodeOutput> = HashMap::new();

    // Register handlers as plain functions
    handlers.insert("onehot".to_string(), onehot_handler);
    handlers.insert("feature_engineering".to_string(), feature_engineering_handler);

    // Create cache for DAG orchestrator
    let cache = Arc::new(InMemoryCache::new(300));

    // Create the router using just the DAG orchestrator
    let app = create_dag_router(handlers, cache, true)
        .layer(middleware::from_fn(track_metrics))
        .layer(middleware::from_fn(set_request_start))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("Service is overloaded: {}", error),
                    )
                }))
                .buffer(REQUEST_BUFFER_SIZE)
                .into_inner(),
        );

    // Start the server with graceful shutdown handling
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("Starting server at {}", addr);

    let server = axum::Server::bind(&addr).serve(app.into_make_service());

    // Graceful shutdown
    let graceful = server.with_graceful_shutdown(shutdown_signal());

    // Run the server
    if let Err(e) = graceful.await {
        eprintln!("Server error: {}", e);
    }

    Ok(())
}

// Graceful shutdown signal handler
async fn shutdown_signal() {
    // Wait for Ctrl-C
    signal::ctrl_c()
        .await
        .expect("Failed to install Ctrl-C handler");
    println!("Shutting down...");
}

// Check the queue time and processing time, emitting to logs
pub async fn track_metrics(
    request: Request<Body>,
    next: Next<Body>,
) -> Result<Response, Infallible> {
    let processing_start = Instant::now();
    let now = SystemTime::now();
    let path = request.uri().path().to_owned();

    // Get internal queue times
    let tcp_to_processing = REQUEST_START.with(|start| {
        start.get()
            .map(|arrival_time| processing_start.duration_since(arrival_time))
            .unwrap_or_default()
    });

    // Get client timing information
    let client_timings = request.headers()
        .get("X-Request-Start")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|start_time| {
            let client_start = UNIX_EPOCH + std::time::Duration::from_micros(start_time);
            let total_time = now.duration_since(client_start).unwrap_or_default();
            total_time
        });

    let client_send_time = request.headers()
        .get("X-Client-Send")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map(|send_time| {
            let send = UNIX_EPOCH + std::time::Duration::from_micros(send_time);
            now.duration_since(send).unwrap_or_default()
        });

    // Process the request
    let response = next.run(request).await;
    let processing_time = processing_start.elapsed();

    // Log detailed timing breakdown
    debug!(
        "Request timing for {}:\n  \
         → Request creation to send: {:?}\n  \
         → Network transit time: {:?}\n  \
         → TCP queue time: {:.2}ms\n  \
         → Processing time: {:.2}ms\n  \
         → Total server time: {:.2}ms",
        path,
        client_timings.map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0)),
        client_send_time.map(|d| format!("{:.2}ms", d.as_secs_f64() * 1000.0)),
        tcp_to_processing.as_secs_f64() * 1000.0,
        processing_time.as_secs_f64() * 1000.0,
        (tcp_to_processing + processing_time).as_secs_f64() * 1000.0,
    );

    Ok(response)
}

// Add this before your request handling
pub async fn set_request_start(
    request: Request<Body>,
    next: Next<Body>,
) -> Result<Response, Infallible> {
    let tcp_arrival = Instant::now();
    REQUEST_START.with(|start| {
        start.set(Some(tcp_arrival));
    });

    let response = next.run(request).await;

    REQUEST_START.with(|start| {
        start.set(None);
    });

    Ok(response)
}

// Create standalone functions instead of closures
fn onehot_handler(input: NodeInput) -> NodeOutput {
    let rt = tokio::runtime::Handle::current();

    // Extract version from params if present
    let version = input.params
        .as_ref()
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let data = input.inputs.values()
        .next()
        .and_then(|v| serde_json::from_value::<Vec<String>>(v.clone()).ok())
        .expect("Expected Vec<String> input");

    let result = rt.block_on(OneHotHandler::handle(
        Json(VersionedInput { data, version }),  // Pass through the version
        VERSIONED_MODULES.get().unwrap().clone()
    ));

    // Add debug logging here
    debug!("OneHotHandler result: {:?}", result);

    // Response handling
    let body_bytes = rt.block_on(async {
        if let Some(data) = result.into_response().into_body().data().await {
            data.ok()
        } else {
            None
        }
    });

    // Add debug logging here too
    debug!("Body bytes: {:?}", body_bytes);

    match body_bytes {
        Some(bytes) => match serde_json::from_slice(&bytes) {
            Ok(value) => NodeOutput {
                value,
                error: None,
            },
            Err(e) => NodeOutput {
                value: serde_json::Value::Null,
                error: Some(e.to_string()),
            },
        },
        None => NodeOutput {
            value: serde_json::Value::Null,
            error: Some("Failed to get response data".to_string()),
        },
    }
}

// Similar function for feature_engineering_handler
fn feature_engineering_handler(input: NodeInput) -> NodeOutput {
    let rt = tokio::runtime::Handle::current();

    let version = input.params
        .as_ref()
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .map(String::from);

    // Extract the result array from the onehot output
    let data = input.inputs.values()
        .next()
        .and_then(|v| v.get("result"))  // Extract the "result" field
        .and_then(|arr| arr.as_array())  // Get as array
        .map(|arrays| {
            // Flatten the 2D array into a 1D array of f64s
            arrays.iter()
                .flat_map(|inner| {
                    inner.as_array()
                        .map(|arr| arr.to_vec())
                        .unwrap_or_default()
                })
                .filter_map(|v| v.as_f64())
                .collect::<Vec<f64>>()
        })
        .expect("Expected 2D array input from onehot encoder");

    let result = rt.block_on(FeatureEngineeringHandler::handle(
        Json(VersionedInput { data, version }),
        VERSIONED_MODULES.get().unwrap().clone()
    ));

    // Response handling
    let body_bytes = rt.block_on(async {
        if let Some(data) = result.into_response().into_body().data().await {
            data.ok()
        } else {
            None
        }
    });

    match body_bytes {
        Some(bytes) => match serde_json::from_slice(&bytes) {
            Ok(value) => NodeOutput {
                value,
                error: None,
            },
            Err(e) => NodeOutput {
                value: serde_json::Value::Null,
                error: Some(e.to_string()),
            },
        },
        None => NodeOutput {
            value: serde_json::Value::Null,
            error: Some("Failed to get response data".to_string()),
        },
    }
}
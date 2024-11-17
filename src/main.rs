use axum::error_handling::HandleErrorLayer;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::{BoxError, Json};
use axum::{
    routing::post,
    Router,
    middleware,
};
use serde_json::Value;
use snakes::feature_engineering::FeatureEngineeringHandler;
use snakes::onehot::OneHotHandler;
use snakes::{Snake, VersionedInput};
use tracing::{error, info};
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

mod watcher;

mod versioned_modules;
use versioned_modules::VersionedModules;

mod snakes;

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

    // Set up file watcher
    watcher::spawn_file_watcher(
        scripts_dir.clone(),
        versioned_modules.clone(),
    )?;

    // Define individual endpoints with proper handler signatures
    async fn onehot_endpoint(
        State(versioned_modules): State<Arc<VersionedModules>>,
        input: Json<VersionedInput<Vec<String>>>,
    ) -> Response {
        OneHotHandler::handle(input, versioned_modules).await
    }

    async fn feature_engineering_endpoint(
        State(versioned_modules): State<Arc<VersionedModules>>,
        input: Json<VersionedInput<Vec<f64>>>,
    ) -> Response {
        FeatureEngineeringHandler::handle(input, versioned_modules).await
    }

    // You can compose endpoints! But then you have to handle the input and output types yourself.
    // And with versions, you only choose the first input version...
    // But you can do it!
    // TODO: abstract even this function such that per request you could compose different endpoints...
    async fn compose_endpoint(
        State(versioned_modules): State<Arc<VersionedModules>>,
        input: Json<VersionedInput<Vec<String>>>,
    ) -> Response {
        let onehot_response = onehot_endpoint(
            State(Arc::clone(&versioned_modules)),
            input,
        ).await;
        
        // Check if onehot was successful
        if !onehot_response.status().is_success() {
            return onehot_response;
        }

        // Extract the body and parse it
        let mut body = onehot_response.into_body();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(e) => {
                    error!("Failed to read response body: {}", e);
                    return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read intermediate result").into_response();
                }
            }
        }

        let onehot_json: Value = match serde_json::from_slice(&bytes) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to parse onehot response: {}", e);
                return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to parse intermediate result").into_response();
            }
        };

        // Convert onehot result to feature engineering input
        let onehot_data = match onehot_json.get("result")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_array())
                    .flat_map(|v| v.iter().filter_map(|x| x.as_f64()))
                    .collect::<Vec<_>>()
            })
        {
            Some(data) => data,
            None => {
                error!("Invalid onehot response format");
                return (StatusCode::INTERNAL_SERVER_ERROR, "Invalid intermediate result format").into_response();
            }
        };

        feature_engineering_endpoint(
            State(versioned_modules),
            Json(VersionedInput {
                data: onehot_data,
                version: None,
            }),
        ).await
    }

    // Create the router with the endpoints
    let app = Router::new()
        .route("/onehot", post(onehot_endpoint))
        .route("/feature_engineering", post(feature_engineering_endpoint))
        .route("/compose", post(compose_endpoint))
        .layer(middleware::from_fn(track_metrics))
        .layer(middleware::from_fn(set_request_start))
        .layer(
            ServiceBuilder::new()
                // Handle errors from the buffer middleware
                .layer(HandleErrorLayer::new(|error: BoxError| async move {
                    (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        format!("Service is overloaded: {}", error),
                    )
                }))
                // Add a bounded buffer of 1024 requests
                .buffer(REQUEST_BUFFER_SIZE)
                .into_inner(),
        )
        .with_state(versioned_modules);

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
    info!(
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
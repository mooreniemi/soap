use axum::extract::State;
use axum::Json;
use axum::{
    routing::post,
    Router,
};
use serde_json::Value;
use snakes::feature_engineering::{FeatureEngineeringHandler, FeatureEngineeringInput};
use snakes::onehot::{OneHotHandler, OneHotInput};
use snakes::Snake;
use tracing::info;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber;
use tokio::signal;
use clap::Parser;

mod watcher;

mod versioned_modules;
use versioned_modules::VersionedModules;

mod snakes;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    info!("Args: {:?}", args);

    // Get scripts directory, falling back to CARGO_MANIFEST_DIR
    let scripts_dir = Arc::new(args.scripts_dir.unwrap_or_else(|| {
        env::var("CARGO_MANIFEST_DIR").expect("Neither SCRIPTS_DIR nor CARGO_MANIFEST_DIR is set")
    }));

    // Set Python environment variables
    env::set_var("PYTHON_SYS_EXECUTABLE", "/opt/homebrew/bin/python3.12");
    env::set_var(
        "PYTHONHOME",
        "/opt/homebrew/Cellar/python@3.12/3.12.6/Frameworks/Python.framework/Versions/3.12",
    );
    env::set_var(
        "PYTHONPATH",
        "/opt/homebrew/Cellar/python@3.12/3.12.6/Frameworks/Python.framework/Versions/3.12/lib/python3.12/site-packages",
    );

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
        input: Json<OneHotInput>,
    ) -> Json<Value> {
        OneHotHandler::handle(input, versioned_modules).await
    }

    async fn feature_engineering_endpoint(
        State(versioned_modules): State<Arc<VersionedModules>>,
        input: Json<FeatureEngineeringInput>,
    ) -> Json<Value> {
        FeatureEngineeringHandler::handle(input, versioned_modules).await
    }

    // You can compose endpoints! But then you have to handle the input and output types yourself.
    // And with versions, you only choose the first input version...
    // But you can do it!
    // TODO: abstract even this function such that per request you could compose different endpoints...
    async fn compose_endpoint(
        State(versioned_modules): State<Arc<VersionedModules>>,
        input: Json<OneHotInput>,
    ) -> Json<Value> {
        let onehot_result = onehot_endpoint(
            State(Arc::clone(&versioned_modules)),
            input,
        ).await;
        
        // Convert onehot result to feature engineering input
        let onehot_data = onehot_result.0["result"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|v| v.as_array().unwrap().iter().map(|x| x.as_f64().unwrap()))
            .collect::<Vec<_>>();

        feature_engineering_endpoint(
            State(versioned_modules),
            Json(FeatureEngineeringInput::new(onehot_data)),
        ).await
    }

    // Create the router with the endpoints
    let app = Router::new()
        .route("/onehot", post(onehot_endpoint))
        .route("/feature_engineering", post(feature_engineering_endpoint))
        .route("/compose", post(compose_endpoint))
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


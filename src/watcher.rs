use notify::{Watcher, RecursiveMode};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::channel;
use tracing::{error, info};
use crate::versioned_modules::VersionedModules;

pub fn spawn_file_watcher(
    scripts_dir: Arc<String>,
    versioned_modules: Arc<VersionedModules>,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::spawn(watch_scripts(scripts_dir, versioned_modules));
    Ok(())
}

async fn watch_scripts(scripts_dir: Arc<String>, versioned_modules: Arc<VersionedModules>) {
    let scripts_path = Path::new(&*scripts_dir);
    info!("Watching directory: {}", scripts_path.display());
    
    let (tx, mut rx) = channel(1);

    let mut watcher = notify::recommended_watcher(move |res| {
        match res {
            Ok(event) => {
                if let Err(e) = tx.blocking_send(event) {
                    error!("Failed to send watch event: {}", e);
                }
            }
            Err(e) => error!("Watch error: {}", e),
        }
    }).expect("Failed to create watcher");

    watcher.watch(scripts_path, RecursiveMode::Recursive)
        .expect("Failed to watch directory");

    // Keep watcher alive
    std::mem::forget(watcher);

    // Handle file change events
    let handler_scripts_dir = scripts_dir;
    let handler_modules = versioned_modules;
    
    while let Some(_) = rx.recv().await {
        info!("Received reload signal");
        if let Err(e) = handler_modules.refresh_modules(&handler_scripts_dir) {
            error!("Failed to refresh modules: {}", e);
        }
    }
}
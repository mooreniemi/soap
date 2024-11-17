use notify::{Watcher, RecursiveMode, Event};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};
use crate::versioned_modules::VersionedModules;

pub fn spawn_file_watcher(
    scripts_dir: Arc<String>,
    versioned_modules: Arc<VersionedModules>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::channel(1);
    
    // Spawn file watcher
    let watcher_scripts_dir = scripts_dir.clone();
    tokio::spawn(async move {
        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                info!("File system event detected: {:?}", event);
                if event.kind.is_modify() || event.kind.is_create() || event.kind.is_remove() {
                    info!("Sending reload signal");
                    let _ = tx.blocking_send(());
                }
            }
        }).expect("Failed to create watcher");

        watcher.watch(
            &Path::new(&*watcher_scripts_dir).join("scripts"), 
            RecursiveMode::NonRecursive
        ).expect("Failed to watch directory");

        // Keep watcher alive
        std::mem::forget(watcher);
    });

    // Handle file change events
    let handler_scripts_dir = scripts_dir;
    let handler_modules = versioned_modules;
    
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            info!("Received reload signal");
            if let Err(e) = handler_modules.refresh_modules(&handler_scripts_dir) {
                error!("Failed to refresh modules: {}", e);
            }
        }
    });

    Ok(())
}
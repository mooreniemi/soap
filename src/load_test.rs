use clap::Parser;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Target transactions per second
    #[arg(long, env = "TARGET_TPS", default_value = "100")]
    target_tps: u64,

    /// Duration to run the test in seconds
    #[arg(long, env = "TEST_DURATION_SECS", default_value = "10")]
    duration_secs: u64,

    /// Server URL
    #[arg(long, env = "SERVER_URL", default_value = "http://127.0.0.1:3000/onehot")]
    server_url: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Configuration
    let categories = vec!["cat", "dog", "mouse", "elephant"];
    let request_rate = args.target_tps;
    let total_requests = request_rate * args.duration_secs;

    println!("Starting load test with:");
    println!("  Target TPS: {}", request_rate);
    println!("  Duration: {} seconds", args.duration_secs);
    println!("  Total Requests: {}", total_requests);
    println!("  Server URL: {}", args.server_url);

    // HTTP client
    let client = Client::new();

    // Leaky bucket semaphore to control rate
    let bucket = Arc::new(Semaphore::new(request_rate as usize));

    // Collect latencies and server metrics
    let mut latencies = vec![];
    let mut gil_durations = vec![];
    let mut lock_durations = vec![];
    let mut total_durations = vec![];

    let mut handles = vec![];
    let start_time = Instant::now();

    for _ in 0..total_requests {
        let client = client.clone();
        let bucket = Arc::clone(&bucket);
        let categories = categories.clone();
        let url = args.server_url.clone();

        // Spawn a task for each request
        let handle = tokio::spawn(async move {
            // Acquire a token from the bucket
            let _permit = bucket.acquire().await;

            // Wait for the right pacing
            tokio::time::sleep(Duration::from_millis(1000 / request_rate as u64)).await;

            // Measure latency and process the response
            let start = Instant::now();
            let response = client.post(&url)
                .json(&serde_json::json!({ "categories": categories, "version": "1.0.0" }))
                .send()
                .await;

            let latency = start.elapsed();
            if let Ok(response) = response {
                if let Ok(metrics) = response.json::<ResponseMetrics>().await {
                    Some((latency, metrics))
                } else {
                    None
                }
            } else {
                None
            }
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    for handle in handles {
        if let Ok(Some((latency, metrics))) = handle.await {
            latencies.push(latency.as_millis() as u64);
            gil_durations.push(metrics.metrics.gil_duration_ms);
            lock_durations.push(metrics.metrics.lock_duration_ms);
            total_durations.push(metrics.metrics.total_duration_ms);
        }
    }

    // Calculate and print statistics
    println!("Latency statistics:");
    print_stats("Latency", &latencies);

    println!("\nServer-side metrics:");
    print_stats("GIL duration", &gil_durations);
    print_stats("Lock duration", &lock_durations);
    print_stats("Total duration", &total_durations);
}

// Deserialize server response
#[derive(Deserialize)]
struct Metrics {
    gil_duration_ms: u64,
    lock_duration_ms: u64,
    total_duration_ms: u64,
}

#[derive(Deserialize)]
struct ResponseMetrics {
    metrics: Metrics,
}

// Helper function to calculate and print statistics
fn print_stats(label: &str, data: &[u64]) {
    if data.is_empty() {
        println!("  {}: No data collected.", label);
        return;
    }

    let avg = data.iter().sum::<u64>() as f64 / data.len() as f64;
    let p50 = percentile(data, 50);
    let p90 = percentile(data, 90);
    let p99 = percentile(data, 99);

    println!("  {}:", label);
    println!("    Average: {:.2} ms", avg);
    println!("    p50: {} ms", p50);
    println!("    p90: {} ms", p90);
    println!("    p99: {} ms", p99);
}

// Helper function to calculate percentiles
fn percentile(data: &[u64], p: usize) -> u64 {
    let idx = (data.len() * p) / 100;
    data[idx.min(data.len() - 1)]
}
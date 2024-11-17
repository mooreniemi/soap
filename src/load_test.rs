use clap::Parser;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::collections::HashMap;
use tracing::{info, warn, error};

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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Configuration
    let categories = Arc::new(vec!["cat", "dog", "mouse", "elephant"]);
    let request_rate = args.target_tps;
    let duration_secs = args.duration_secs;
    let total_requests = request_rate * duration_secs;

    info!("Starting load test with:");
    info!("  Target TPS: {}", request_rate);
    info!("  Duration: {} seconds", duration_secs);
    info!("  Total Requests: {}", total_requests);
    info!("  Server URL: {}", args.server_url);

    // Set up rate limiter
    let quota = Quota::per_second(NonZeroU32::new(request_rate as u32).unwrap());
    let limiter = Arc::new(RateLimiter::direct(quota));

    // Create a single client with connection pooling
    let client = Client::builder()
        .pool_max_idle_per_host(100)
        .pool_idle_timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .build()
        .unwrap();
    let client = Arc::new(client);

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(total_requests as usize);

    // Spawn all tasks
    for _ in 0..total_requests {
        let client = Arc::clone(&client);
        let limiter = Arc::clone(&limiter);
        let categories = Arc::clone(&categories);
        let url = args.server_url.clone();

        let handle = tokio::spawn(async move {
            // Wait for rate limiter
            limiter.until_ready().await;

            // Time each step
            let request_created = SystemTime::now();
            let start_time = request_created
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros();

            // Time JSON preparation
            let json_start = Instant::now();
            let request_payload = serde_json::json!({
                "data": &*categories,
                "version": "1.0.0"
            });
            let json_time = json_start.elapsed().as_millis() as u64;

            // Time the network portion (send + receive)
            let network_start = Instant::now();
            let result = client.post(&url)
                .header("X-Request-Start", start_time.to_string())
                .header("X-Client-Send", SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros()
                    .to_string())
                .json(&request_payload)
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    let network_time = network_start.elapsed().as_millis() as u64;

                    match response.json::<ResponseMetrics>().await {
                        Ok(metrics) => Ok((
                            status,
                            LatencyMetrics {
                                total: network_time,
                                json: json_time,
                                network: network_time,
                            },
                            metrics
                        )),
                        Err(_) => Ok((status, LatencyMetrics {
                            total: network_time,
                            json: json_time,
                            network: network_time,
                        }, ResponseMetrics {
                            metrics: Metrics {
                                gil_duration_ms: 0,
                                lock_duration_ms: 0,
                                total_duration_ms: 0,
                            }
                        }))
                    }
                }
                Err(e) => Err(e)
            }
        });

        handles.push(handle);
    }

    // Wait for all requests to complete or timeout
    let timeout = Duration::from_secs(duration_secs + 5); // Add 5s grace period
    let results = tokio::time::timeout(
        timeout,
        futures::future::join_all(handles)
    ).await.unwrap_or_else(|_| {
        warn!("Warning: Load test exceeded timeout!");
        vec![]
    });

    // Process results
    let mut latencies = Vec::with_capacity(total_requests as usize);
    let mut json_times = Vec::with_capacity(total_requests as usize);
    let mut network_times = Vec::with_capacity(total_requests as usize);
    let mut gil_durations = Vec::with_capacity(total_requests as usize);
    let mut lock_durations = Vec::with_capacity(total_requests as usize);
    let mut total_durations = Vec::with_capacity(total_requests as usize);
    let mut status_codes = HashMap::new();
    let mut connection_errors = HashMap::new();
    let mut sample_error = None;

    // Collect metrics and errors in a single pass
    for result in &results {
        match result {
            Ok(Ok((status, latency_metrics, server_metrics))) => {
                if status.is_success() {
                    latencies.push(latency_metrics.total);
                    json_times.push(latency_metrics.json);
                    network_times.push(latency_metrics.network);
                    gil_durations.push(server_metrics.metrics.gil_duration_ms);
                    lock_durations.push(server_metrics.metrics.lock_duration_ms);
                    total_durations.push(server_metrics.metrics.total_duration_ms);
                } else {
                    *status_codes.entry(status.as_u16()).or_insert(0) += 1;
                    if sample_error.is_none() {
                        sample_error = Some(format!("HTTP {}", status.as_u16()));
                    }
                }
            }
            Ok(Err(e)) => {
                let error_type = if e.is_timeout() {
                    "Timeout"
                } else if e.is_connect() {
                    if e.to_string().contains("Connection reset by peer") {
                        "Server Reset Connection"
                    } else {
                        "Connection Failed"
                    }
                } else if e.is_request() {
                    "Invalid Request"
                } else if e.is_body() {
                    "Request Body Error"
                } else if e.is_decode() {
                    "Response Decode Error"
                } else {
                    "Other Error"
                };
                *connection_errors.entry(error_type).or_insert(0) += 1;
                if sample_error.is_none() {
                    sample_error = Some(format!("Connection error: {} ({})", error_type, e));
                }
                warn!("Request failed: {} - {}", error_type, e);
            }
            Err(e) => {
                *connection_errors.entry("Task Error").or_insert(0) += 1;
                error!("Task error: {}", e);
            }
        }
    }

    let test_duration = start_time.elapsed();
    let successful_requests = latencies.len(); // Count of successful requests

    info!("\nTest completed in {:.2} seconds", test_duration.as_secs_f64());
    info!("Request Statistics:");
    info!("  Successful: {}/{} ({:.2}%)",
        successful_requests,
        total_requests,
        (successful_requests as f64 / total_requests as f64) * 100.0
    );

    if !status_codes.is_empty() {
        info!("  Failed requests by status code:");
        for (status, count) in status_codes.iter() {
            info!("    HTTP {}: {} ({:.2}%)",
                status,
                count,
                (*count as f64 / total_requests as f64) * 100.0
            );
        }
    }

    if !connection_errors.is_empty() {
        info!("\nConnection errors:");
        for (error_type, count) in connection_errors.iter() {
            info!("  {}: {} ({:.2}%)",
                error_type,
                count,
                (*count as f64 / total_requests as f64) * 100.0
            );
        }
        if let Some(sample) = &sample_error {
            info!("Sample error: {}", sample);
        }
    }

    info!("\nClient-side timing statistics:");
    info!("  JSON preparation:");
    print_stats("    ", &json_times);
    info!("  Network time:");
    print_stats("    ", &network_times);
    info!("  Total latency:");
    print_stats("    ", &latencies);

    info!("\nServer-side metrics:");
    info!("  GIL duration:");
    print_stats("    ", &gil_durations);
    info!("  Lock duration:");
    print_stats("    ", &lock_durations);
    info!("  Total duration:");
    print_stats("    ", &total_durations);
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

// Add this struct to hold our client-side metrics
#[derive(Debug)]
struct LatencyMetrics {
    total: u64,
    json: u64,
    network: u64,
}

// Helper function to calculate and print statistics
fn print_stats(prefix: &str, values: &[u64]) {
    if values.is_empty() {
        info!("{}No data collected.", prefix);
        return;
    }

    let sum: u64 = values.iter().sum();
    let avg = sum as f64 / values.len() as f64;

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    let p50 = percentile(&sorted, 50);
    let p90 = percentile(&sorted, 90);
    let p99 = percentile(&sorted, 99);

    info!("{}Average: {:.2} ms", prefix, avg);
    info!("{}p50: {} ms", prefix, p50);
    info!("{}p90: {} ms", prefix, p90);
    info!("{}p99: {} ms", prefix, p99);
}

// Helper function to calculate percentiles
fn percentile(data: &[u64], p: usize) -> u64 {
    let idx = (data.len() * p) / 100;
    data[idx.min(data.len() - 1)]
}
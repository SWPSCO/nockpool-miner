mod auth;
mod config;
mod device;
mod hot_loader;
mod jam_loader;
mod key_manager;
mod key_storage;
mod miner;
mod new_job;
mod submission;
mod telemetry;
mod tracer;

use crate::config::Config;
use crate::key_manager::{resolve_mining_key, KeyManager};
use crate::new_job::NockPoolNewJobConsumer;
use crate::submission::{NockPoolSubmissionProvider, NockPoolSubmissionResponseHandler};

use bytes::Bytes;
use clap::Parser;
use quiver::types::{Submission, Target, Template};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    tracer::init();

    let config = Config::parse();

    if config.benchmark {
        tracing::info!("Running benchmark...");
        if let Err(e) = miner::benchmark(config.max_threads, config.benchmark_proofs).await {
            tracing::error!("Error running benchmark: {}", e);
        }
        tracing::info!("Benchmark completed successfully");
        return;
    }

    if config.clear_key {
        tracing::info!("Clearing stored mining key...");
        match KeyManager::new() {
            Ok(key_manager) => match key_manager.clear_stored_key() {
                Ok(()) => {
                    tracing::info!("Stored mining key cleared successfully");
                    tracing::info!("Key was stored at: {}", key_manager.get_key_storage_path());
                }
                Err(e) => {
                    tracing::error!("Failed to clear stored key: {}", e);
                }
            },
            Err(e) => {
                tracing::error!("Failed to initialize key manager: {}", e);
            }
        }
        return;
    }

    // --- Template Provider ---
    let (template_tx, template_rx) = watch::channel(Template::new(
        Bytes::new(),
        Bytes::new(),
        Bytes::new(),
        Bytes::new(),
        Bytes::new(),
    ));
    let new_job_consumer = Arc::new(NockPoolNewJobConsumer::new(template_tx));

    // --- Submission Provider ---
    let initial_submission =
        Submission::new(Target::Pool, Bytes::new(), Bytes::new(), Bytes::new());
    let (submission_tx, submission_rx) = watch::channel(initial_submission);
    let submission_provider = Arc::new(NockPoolSubmissionProvider::new(submission_rx));

    let submission_response_handler = Arc::new(NockPoolSubmissionResponseHandler::new());

    // --- Gather System Info ---
    let device_info = device::get_device_info();
    let gpu_info = device::get_gpu_info();
    let gpu_str = gpu_info.as_deref().unwrap_or("None detected");

    tracing::info!(
        "Starting miner with OS='{}', CPU='{}', RAM='{} GB', GPU='{}'",
        device_info.os,
        device_info.cpu_model,
        device_info.ram_capacity_gb,
        gpu_str
    );

    // --- Set up panic hook for quiver client ---
    let (panic_tx, mut panic_rx) = mpsc::unbounded_channel::<()>();

    std::panic::set_hook(Box::new(move |panic_info| {
        let payload = panic_info
            .payload()
            .downcast_ref::<&str>()
            .unwrap_or(&"Unknown panic");

        // Check if this is a quiver-related panic
        if payload.contains("failed to open submission stream")
            || payload.contains("TimedOut")
            || panic_info
                .location()
                .map_or(false, |l| l.file().contains("quiver"))
        {
            error!("Quiver client panic detected: {}", payload);
            let _ = panic_tx.send(());
        }

        // Print the panic info (preserving normal panic behavior)
        eprintln!(
            "thread '{}' panicked at {}:",
            std::thread::current().name().unwrap_or("<unnamed>"),
            panic_info
                .location()
                .map_or("unknown location".to_string(), |l| format!(
                    "{}:{}",
                    l.file(),
                    l.line()
                ))
        );
        eprintln!("{}", payload);
    }));

    // --- Resolve mining key ---
    let key = match resolve_mining_key(&config).await {
        Ok(key) => key,
        Err(e) => {
            tracing::error!("Failed to resolve mining key: {}", e);
            return;
        }
    };
    let server_address = config.server_address.clone();
    let client_address = config.client_address.clone();
    let insecure = config.insecure.clone();

    // --- Quiver-native telemetry provider ---
    //
    // Telemetry is now transported on the authenticated quiver connection, so
    // it shares the same auth lifecycle as mining traffic and avoids separate
    // HTTP token checks/races.
    let telemetry_provider = Arc::new(telemetry::NockPoolTelemetryProvider::new());

    // 24-hour restart timer
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(24 * 60 * 60)).await;
        println!("restart-miner-now");
    });

    tokio::spawn(async move {
        let mut backoff_ms = 100_u64;
        let max_backoff_ms = 30_000_u64;

        loop {
            // Start the quiver client
            let mut client_handle = tokio::spawn({
                let server_address = server_address.clone();
                let client_address = client_address.clone();
                let key = key.clone();
                let device_info = device_info.clone();
                let new_job_consumer = new_job_consumer.clone();
                let submission_provider = submission_provider.clone();
                let submission_response_handler = submission_response_handler.clone();
                let telemetry_provider = telemetry_provider.clone();

                async move {
                    quiver::client::run_with_telemetry(
                        insecure,
                        server_address,
                        client_address,
                        key,
                        device_info,
                        new_job_consumer,
                        submission_provider,
                        submission_response_handler,
                        Some(telemetry_provider),
                    )
                    .await
                }
            });

            // Wait for either the client to finish or a panic to occur
            let client_result = tokio::select! {
                result = &mut client_handle => {
                    Some(result)
                }
                _ = panic_rx.recv() => {
                    error!("Panic detected in quiver client - triggering reconnection");
                    client_handle.abort();
                    None
                }
            };

            match client_result {
                Some(Ok(Ok(()))) => {
                    info!("Client connection completed successfully, reconnecting immediately");
                    backoff_ms = 100;
                }
                Some(Ok(Err(e))) => {
                    error!("Client connection failed: {}", e);

                    info!("Sleeping for {}ms before reconnecting", backoff_ms);
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                }
                Some(Err(e)) => {
                    error!("Client task failed: {}", e);

                    info!(
                        "Sleeping for {}ms before reconnecting after task failure",
                        backoff_ms
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                }
                None => {
                    // Panic was detected
                    info!(
                        "Sleeping for {}ms before reconnecting after panic",
                        backoff_ms
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);
                }
            }
        }
    });

    // --- Run the miner ---
    if let Err(e) = miner::start(config, template_rx, submission_tx).await {
        error!("Error running miner: {}", e);
    }
}

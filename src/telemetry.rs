use std::env;
use std::fs;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use quiver::telemetry::{TelemetryData, TelemetryProvider};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio::time::{interval_at, Instant, Interval, MissedTickBehavior};

use crate::device::get_device_info_with_proof_rate;
use crate::miner::get_current_mining_threads;

/// Quiver-native telemetry provider used by the miner.
///
/// Scheduling policy intentionally matches legacy behavior:
/// - first sample after 60 seconds,
/// - subsequent samples every 5 minutes.
#[derive(Debug)]
pub struct NockPoolTelemetryProvider {
    interval: Mutex<Interval>,
    binary_hash: Option<String>,
    gpu_info: Option<String>,
    miner_version: String,
}

impl NockPoolTelemetryProvider {
    pub fn new() -> Self {
        let mut interval = interval_at(
            Instant::now() + Duration::from_secs(60),
            Duration::from_secs(300),
        );
        // Skip missed ticks so a temporarily stalled runtime does not emit a burst.
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        Self {
            interval: Mutex::new(interval),
            binary_hash: Self::get_binary_hash(),
            gpu_info: crate::device::get_gpu_info(),
            miner_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Calculate SHA-256 hash of the current miner binary.
    ///
    /// The binary includes embedded zkvm artifacts, so this is equivalent to the
    /// legacy telemetry hash semantics.
    fn get_binary_hash() -> Option<String> {
        if let Ok(exe_path) = env::current_exe() {
            if let Ok(binary_bytes) = fs::read(&exe_path) {
                let mut hasher = Sha256::new();
                hasher.update(&binary_bytes);
                let hash = hasher.finalize();
                return Some(format!("{:x}", hash));
            }
        }
        None
    }
}

#[async_trait]
impl TelemetryProvider for NockPoolTelemetryProvider {
    async fn next_telemetry(&self) -> Result<TelemetryData> {
        // Quiver's client transport asks for the "next" sample. We block until
        // the configured telemetry schedule reaches its next tick.
        let mut interval = self.interval.lock().await;
        interval.tick().await;
        drop(interval);

        let (device_info, proof_rate) = get_device_info_with_proof_rate();

        Ok(TelemetryData {
            device_os: device_info.os,
            device_cpu: device_info.cpu_model,
            device_ram_capacity_gb: device_info.ram_capacity_gb,
            device_proof_rate_per_sec: proof_rate,
            zkvm_jetpack_hash: self.binary_hash.clone(),
            miner_version: self.miner_version.clone(),
            gpu_info: self.gpu_info.clone(),
            num_threads: get_current_mining_threads(),
            sent_at_unix_ms: Utc::now().timestamp_millis(),
        })
    }
}

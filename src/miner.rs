use crate::config::Config;
use crate::hot_loader::HotLibrary;

use kernels::miner::KERNEL;
use nockvm::jets::hot::HotEntry;
use quiver::types::{Submission, Target, Template};

use anyhow::Result;
use bytes::Bytes;
use rand::Rng;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use sysinfo::System;
use tokio::sync::{watch, Mutex};
use tracing::{error, info, warn};

use nockapp::kernel::form::SerfThread;
use nockapp::noun::slab::NounSlab;
use nockapp::noun::AtomExt;
use nockapp::save::SaveableCheckpoint;
use nockapp::utils::NOCK_STACK_SIZE_TINY;
use nockapp::wire::{WireRepr, WireTag};
use nockapp::NounExt;

use nockvm::interpreter::NockCancelToken;
use nockvm::noun::{Atom, D, T};

use zkvm_jetpack::form::PRIME;

use nockvm_macros::tas;

// RAM per mining thread in GB (recommended minimum)
const RAM_PER_THREAD_GB: f64 = 2.1;

// Window duration for proof rate calculation (10 minutes)
const PROOF_RATE_WINDOW_SECONDS: u64 = 600;

#[derive(Debug, Clone)]
pub struct ProofRateTracker {
    proof_timestamps: VecDeque<Instant>,
    window_duration: Duration,
}

impl ProofRateTracker {
    pub fn new() -> Self {
        Self {
            proof_timestamps: VecDeque::new(),
            window_duration: Duration::from_secs(PROOF_RATE_WINDOW_SECONDS),
        }
    }

    pub fn add_proofs(&mut self, count: u32) {
        let now = Instant::now();
        for _ in 0..count {
            self.proof_timestamps.push_back(now);
        }
        self.clean_old_proofs(now);
    }

    fn clean_old_proofs(&mut self, now: Instant) {
        let cutoff = now - self.window_duration;
        while let Some(&front_time) = self.proof_timestamps.front() {
            if front_time < cutoff {
                self.proof_timestamps.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn get_proof_rate(&mut self) -> f64 {
        let now = Instant::now();
        self.clean_old_proofs(now);

        if self.proof_timestamps.is_empty() {
            return 0.0;
        }

        let proof_count = self.proof_timestamps.len() as f64;

        // If we have fewer than the full window of data, scale accordingly
        if let Some(&oldest_time) = self.proof_timestamps.front() {
            let actual_window_seconds = now.duration_since(oldest_time).as_secs_f64();
            if actual_window_seconds > 0.0 {
                proof_count / actual_window_seconds
            } else {
                0.0
            }
        } else {
            0.0
        }
    }
}

// Global proof rate tracker for access across the application
use std::sync::Arc;
static GLOBAL_PROOF_RATE_TRACKER: std::sync::OnceLock<Arc<Mutex<ProofRateTracker>>> =
    std::sync::OnceLock::new();
static GLOBAL_MINING_THREADS: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

pub fn get_current_proof_rate() -> f64 {
    if let Some(tracker) = GLOBAL_PROOF_RATE_TRACKER.get() {
        if let Ok(mut guard) = tracker.try_lock() {
            return guard.get_proof_rate();
        }
    }
    0.0
}

/// Return the active mining thread count once the miner has been initialized.
pub fn get_current_mining_threads() -> Option<u32> {
    GLOBAL_MINING_THREADS.get().copied()
}

static PROOF_INCREMENT: std::sync::OnceLock<u32> = std::sync::OnceLock::new();

/// Get the proof increment based on compiler feature and config
fn get_proof_increment(config: &Config) -> u32 {
    if config.no_gpu {
        // Force CPU mode
        1
    } else if cfg!(feature = "gpu") {
        // Compiled with GPU feature, use 100x multiplier
        100
    } else {
        // CPU build, use normal counting
        1
    }
}

/// Get the stored proof increment value
fn get_proof_increment_cached() -> u32 {
    *PROOF_INCREMENT.get().unwrap_or(&1)
}

pub async fn start(
    config: Config,
    mut template_rx: watch::Receiver<Template>,
    submission_tx: watch::Sender<Submission>,
) -> Result<()> {
    // Calculate and store proof increment based on compiler feature
    let proof_increment = get_proof_increment(&config);
    let _ = PROOF_INCREMENT.set(proof_increment);

    // Log build type and proof rate
    if config.no_gpu {
        info!("GPU mining disabled by --no-gpu flag, using CPU mode (1x proof rate)");
    } else if cfg!(feature = "gpu") {
        info!(
            "GPU build detected, using GPU proof rate multiplier ({}x)",
            proof_increment
        );
    } else {
        info!("CPU build detected, using CPU mode (1x proof rate)");
    }
    let num_threads = {
        let sys = System::new_all();
        let logical_cores = sys.cpus().len() as u32;
        let total_ram_gb = sys.total_memory() / (1024 * 1024 * 1024);
        let ram_based_threads = (total_ram_gb as f64 / RAM_PER_THREAD_GB).floor() as u32;
        let calculated_threads = logical_cores
            .saturating_sub(2)
            .min(ram_based_threads)
            .max(1);
        if let Some(max_threads) = config.max_threads {
            max_threads.min(calculated_threads) as u64
        } else {
            calculated_threads as u64
        }
    };
    info!("mining with {} threads", num_threads);
    // Expose effective thread count to telemetry so quiver telemetry parity
    // matches what we previously reported through the HTTP endpoint.
    let _ = GLOBAL_MINING_THREADS.set(num_threads as u32);

    let mut mining_attempts = tokio::task::JoinSet::<(
        SerfThread<SaveableCheckpoint>,
        u64,
        Result<NounSlab, anyhow::Error>,
    )>::new();

    let network_only = config.network_only;

    if network_only {
        info!("mining for network target only");
    } else {
        info!("mining for pool and network targets");
    }

    let hot_state = {
        #[cfg(target_os = "linux")]
        {
            // Try to load external library first on Linux
            match unsafe { HotLibrary::load_auto() } {
                Ok(lib) => {
                    info!("Successfully loaded external libzkvm_jetpack.so");
                    let hot_state: &[HotEntry] = lib.jets();
                    hot_state.to_vec()
                }
                Err(e) => {
                    info!("External library not found: {}, using built-in", e);
                    zkvm_jetpack::hot::produce_prover_hot_state()
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            // On non-Linux platforms (including macOS), always use built-in
            info!("Using built-in hot state (external libraries disabled on this platform)");
            zkvm_jetpack::hot::produce_prover_hot_state()
        }
    };
    let test_jets_str = std::env::var("NOCK_TEST_JETS").unwrap_or_default();
    let test_jets = nockapp::kernel::boot::parse_test_jets(test_jets_str.as_str());

    let mining_data: Mutex<Option<Template>> = Mutex::new(None);
    let mut cancel_tokens: Vec<NockCancelToken> = Vec::<NockCancelToken>::new();
    let proof_rate_tracker = Arc::new(Mutex::new(ProofRateTracker::new()));

    // Initialize global tracker
    let _ = GLOBAL_PROOF_RATE_TRACKER.set(proof_rate_tracker.clone());

    // Spawn a task to periodically log proof rate
    {
        let proof_rate_tracker_clone = proof_rate_tracker.clone();
        tokio::spawn(async move {
            let start_time = std::time::Instant::now();
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let mut tracker = proof_rate_tracker_clone.lock().await;
                let current_rate = tracker.get_proof_rate();
                // Only show proof rate after first 5 minutes
                if current_rate > 0.0 && start_time.elapsed() >= std::time::Duration::from_secs(300)
                {
                    info!("Current mining rate: {:.4} proofs/sec", current_rate);
                }
            }
        });
    }

    loop {
        tokio::select! {
            mining_result = mining_attempts.join_next(), if !mining_attempts.is_empty() => {
                match mining_result {
                    Some(join_outcome) => match join_outcome {
                        Ok((serf, id, slab_res)) => {
                            let slab = match slab_res {
                                Ok(slab) => slab,
                                Err(e) => {
                                    error!(%id, error = ?e, "mining attempt returned error; restarting");
                                    mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                    continue;
                                }
                            };

                            let result = unsafe { slab.root() };
                            let result_cell = match result.as_cell() {
                                Ok(c) => c,
                                Err(_) => {
                                    // error!(%id, error = ?e, "invalid result noun (expected cell); restarting");
                                    mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                    continue;
                                }
                            };

                            let hed = result_cell.head();

                            if hed.is_atom() && hed.eq_bytes("poke") {
                                //  mining attempt was cancelled. restart with current block header.
                                info!("using new template on thread={id}");
                                mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                continue;
                            }

                            let effect = match hed.as_cell() {
                                Ok(c) => c,
                                Err(e) => {
                                    error!(%id, error = ?e, "invalid effect (expected cell head)");
                                    mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                    continue;
                                }
                            };

                            if effect.head().eq_bytes("miss") {
                                info!("solution did not hit targets on thread={id}, trying again");

                                // Add miss to proof rate tracker so device_proof_rate includes all attempts
                                {
                                    let mut tracker = proof_rate_tracker.lock().await;
                                    tracker.add_proofs(get_proof_increment_cached());
                                }

                                let mut nonce_slab = NounSlab::new();
                                nonce_slab.copy_into(effect.tail());
                                mine(serf, mining_data.lock().await, &mut mining_attempts, Some(nonce_slab), id).await;
                                continue;
                            }

                            let target_type = if effect.head().eq_bytes("pool") {
                                Target::Pool
                            } else if effect.head().eq_bytes("network") {
                                Target::Network
                            } else {
                                warn!("solution found but invalid target on thread={id}: {:?}", effect.head());
                                mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                continue;
                            };

                            if network_only && target_type != Target::Network {
                                info!("solution did not hit network target on thread={id}, trying again");
                                mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                continue;
                            }

                            let success_message = match effect.tail().as_cell() {
                                Ok(c) => c,
                                Err(e) => {
                                    error!(%id, error = ?e, "invalid success message (expected cell)");
                                    mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                    continue;
                                }
                            };

                            // 2
                            let mut commit_slab: NounSlab = NounSlab::new();
                            commit_slab.copy_into(success_message.head());
                            let commit = commit_slab.jam();

                            // 3
                            let success_message_tail = match success_message.tail().as_cell() {
                                Ok(c) => c,
                                Err(e) => {
                                    error!(%id, error = ?e, "invalid success message tail (expected cell)");
                                    mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                                    continue;
                                }
                            };

                            // 6
                            let digest = Bytes::from(success_message_tail.head().as_atom()?.to_le_bytes());

                            // 7
                            let mut proof_slab: NounSlab = NounSlab::new();
                            proof_slab.copy_into(success_message_tail.tail());
                            let proof = proof_slab.jam();

                            let submission = Submission::new(target_type.clone(), commit, digest, proof);

                            // Update proof rate tracker
                            {
                                let mut tracker = proof_rate_tracker.lock().await;
                                tracker.add_proofs(get_proof_increment_cached());
                                let current_rate = tracker.get_proof_rate();
                                info!(
                                    "solution found on thread={id} for target={:?}. Proof size: {:?} KB. Current rate: {:.4} proofs/sec. Submitting to nockpool.",
                                    target_type,
                                    ((submission.proof.len() as f64) / 1024.0 * 100.0).round() / 100.0,
                                    current_rate
                                );
                            }

                            if let Err(e) = submission_tx.send(submission) {
                                error!(%id, error = ?e, "failed to send submission");
                            }

                            mine(serf, mining_data.lock().await, &mut mining_attempts, None, id).await;
                        }
                        Err(e) => {
                            error!(error = ?e, "mining task failed to join");
                            continue;
                        }
                    },
                    None => {
                        warn!("join_next returned None unexpectedly");
                        continue;
                    }
                }
            }
            _ = template_rx.changed() => {
                let template = template_rx.borrow_and_update().clone();

                *(mining_data.lock().await) = Some(template);

                if mining_attempts.is_empty() {
                    let kernel_bytes = Vec::from(KERNEL);
                    info!("Using embedded kernel");
                    let mut init_tasks = tokio::task::JoinSet::<(u64, Result<SerfThread<SaveableCheckpoint>, anyhow::Error>)>::new();
                    for i in 0..num_threads {
                        let kernel = kernel_bytes.clone();
                        let hot_state = hot_state.clone();
                        let test_jets = test_jets.clone();
                        init_tasks.spawn(async move {
                            let res = SerfThread::<SaveableCheckpoint>::new(
                                kernel,
                                None,
                                hot_state,
                                NOCK_STACK_SIZE_TINY,
                                test_jets,
                                Default::default(),
                            )
                            .await
                            .map_err(|e| anyhow::anyhow!(e));
                            (i, res)
                        });
                    }
                    info!("Received nockpool template! Starting {} mining threads", num_threads);
                    while let Some(res) = init_tasks.join_next().await {
                        match res {
                            Ok((i, Ok(serf))) => {
                                cancel_tokens.push(serf.cancel_token.clone());
                                mine(serf, mining_data.lock().await, &mut mining_attempts, None, i).await;
                            }
                            Ok((i, Err(e))) => {
                                error!(thread_index = i, error = ?e, "Could not load mining kernel");
                            }
                            Err(e) => {
                                error!(error = ?e, "kernel init task join error");
                            }
                        }
                    }
                } else {
                    // Mining is already running so cancel all the running attemps
                    // which are mining on the old block.
                    info!("New nockpool template! Restarting {} mining threads", num_threads);
                    for token in &cancel_tokens {
                        token.cancel();
                    }
                }
            },
        }
    }
}
/*
        %template
        version=?(%0 %1 %2)
        commit=block-commitment:t
        nonce=noun-digest:tip5
        network-target=bignum:bignum
        pool-target=bignum:bignum
        pow-len=@
*/
async fn mine(
    serf: SerfThread<SaveableCheckpoint>,
    template: tokio::sync::MutexGuard<'_, Option<Template>>,
    mining_attempts: &mut tokio::task::JoinSet<(
        SerfThread<SaveableCheckpoint>,
        u64,
        Result<NounSlab>,
    )>,
    nonce: Option<NounSlab>,
    id: u64,
) {
    let mut slab = NounSlab::new();
    // let's first deal with the nonce
    let nonce = if let Some(nonce) = nonce {
        nonce
    } else {
        let mut rng = rand::thread_rng();
        let mut nonce_slab: NounSlab = NounSlab::new();
        let first_atom = match Atom::from_value(&mut nonce_slab, rng.gen::<u64>() % PRIME) {
            Ok(atom) => atom.as_noun(),
            Err(e) => {
                error!(%id, error = ?e, "failed to create first nonce atom");
                return;
            }
        };
        let mut nonce_cell = first_atom;
        for _ in 1..5 {
            let nonce_atom = match Atom::from_value(&mut nonce_slab, rng.gen::<u64>() % PRIME) {
                Ok(atom) => atom.as_noun(),
                Err(e) => {
                    error!(%id, error = ?e, "failed to create nonce atom");
                    return;
                }
            };
            nonce_cell = T(&mut nonce_slab, &[nonce_atom, nonce_cell]);
        }
        nonce_slab.set_root(nonce_cell);
        nonce_slab
    };

    // now we deal with the rest of the template noun
    let template_ref = match template.as_ref() {
        Some(t) => t,
        None => {
            warn!(%id, "mining data not initialized; skipping attempt");
            return;
        }
    };

    let version_atom = Atom::from_bytes(&mut slab, (&template_ref.version.clone()).into());
    let commit = match slab.cue_into(template_ref.commit.clone().into()) {
        Ok(v) => v,
        Err(e) => {
            error!(%id, error = ?e, "Failed to cue commit");
            return;
        }
    };
    let nonce = slab.copy_into(unsafe { *(nonce.root()) });
    let network_target = match slab.cue_into(template_ref.network_target.clone().into()) {
        Ok(v) => v,
        Err(e) => {
            error!(%id, error = ?e, "Failed to cue network target");
            return;
        }
    };
    let pool_target = match slab.cue_into(template_ref.pool_target.clone().into()) {
        Ok(v) => v,
        Err(e) => {
            error!(%id, error = ?e, "Failed to cue pool target");
            return;
        }
    };
    let pow_len_atom = Atom::from_bytes(&mut slab, (&template_ref.pow_len.clone()).into());
    let noun = T(
        &mut slab,
        &[
            D(tas!(b"template")),
            version_atom.as_noun(),
            commit,
            nonce,
            network_target,
            pool_target,
            pow_len_atom.as_noun(),
        ],
    );

    slab.set_root(noun);

    let wire = WireRepr::new("miner", 1, vec![WireTag::String("candidate".to_string())]);
    mining_attempts.spawn(async move {
        info!("starting mining attempt on thread={id}");
        let result = serf
            .poke(wire.clone(), slab.clone())
            .await
            .map_err(|e| anyhow::anyhow!(e));
        (serf, id, result)
    });
}

pub async fn benchmark(max_threads: Option<u32>, benchmark_proofs: u32) -> Result<()> {
    let hot_state = {
        #[cfg(target_os = "linux")]
        {
            // Try to load external library first on Linux
            match unsafe { HotLibrary::load_auto() } {
                Ok(lib) => {
                    info!("Successfully loaded external libzkvm_jetpack.so");
                    let hot_state: &[HotEntry] = lib.jets();
                    hot_state.to_vec()
                }
                Err(e) => {
                    info!("External library not found: {}, using built-in", e);
                    zkvm_jetpack::hot::produce_prover_hot_state()
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            // On non-Linux platforms (including macOS), always use built-in
            info!("Using built-in hot state (external libraries disabled on this platform)");
            zkvm_jetpack::hot::produce_prover_hot_state()
        }
    };

    let test_jets_str = std::env::var("NOCK_TEST_JETS").unwrap_or_default();
    let test_jets = nockapp::kernel::boot::parse_test_jets(test_jets_str.as_str());

    let version = hex::decode("0200000000000000")
        .map_err(|e| anyhow::anyhow!("Failed to decode version: {e}"))?;
    let commit = hex::decode("017ee86437eac9dbae690081199671e25cb54ce700ff8cdb259db500063b409e2aa64f968ec7ed801e75db735d82443707").map_err(|e| anyhow::anyhow!("Failed to decode commit: {e}"))?;
    let network_target = hex::decode("81177307ec6aacb01ef04b58dbf601823db967ef80ed5256e50304ab9031bb015f1c808d1d2058623e470ce8cdf54174c07f08c49a068e8f02").map_err(|e| anyhow::anyhow!("Failed to decode network target: {e}"))?;
    let pool_target = hex::decode("81177307ec6aacb01ef04b58dbf601823db967ef80ed5256e50304ab9031bb015f1c808d1d2058623e470ce8cdf54174c07f08c49a068e8f02").map_err(|e| anyhow::anyhow!("Failed to decode pool target: {e}"))?;
    let pow_len = hex::decode("4000000000000000")
        .map_err(|e| anyhow::anyhow!("Failed to decode pow len: {e}"))?;

    let mining_data: Mutex<Option<Template>> = Mutex::new(Some(Template::new(
        Bytes::from(version),
        Bytes::from(commit),
        Bytes::from(network_target),
        Bytes::from(pool_target),
        Bytes::from(pow_len),
    )));

    let num_threads = max_threads.unwrap_or(1);
    info!(
        "Running benchmark with {} threads, {} proofs per thread",
        num_threads, benchmark_proofs
    );

    let mut benchmark_tasks =
        tokio::task::JoinSet::<(u64, Result<tokio::time::Duration, anyhow::Error>)>::new();
    let kernel_bytes = Vec::from(KERNEL);
    info!("Using embedded kernel");
    // Initialize threads first, like the mining code does
    let mut init_tasks =
        tokio::task::JoinSet::<(u32, Result<SerfThread<SaveableCheckpoint>, anyhow::Error>)>::new();
    for thread_id in 0..num_threads {
        let kernel = kernel_bytes.clone();
        let hot_state = hot_state.clone();
        let test_jets = test_jets.clone();
        init_tasks.spawn(async move {
            let res = SerfThread::<SaveableCheckpoint>::new(
                kernel,
                None,
                hot_state,
                NOCK_STACK_SIZE_TINY,
                test_jets,
                Default::default(),
            )
            .await
            .map_err(|e| anyhow::anyhow!(e));
            (thread_id, res)
        });
    }

    // Start timing the entire benchmark
    let benchmark_start = tokio::time::Instant::now();

    // Now spawn benchmark tasks with initialized serf threads
    while let Some(res) = init_tasks.join_next().await {
        match res {
            Ok((thread_id, Ok(serf))) => {
                // Create a local copy of the template for this thread
                let template = mining_data.lock().await.clone();
                benchmark_tasks.spawn(async move {
                    let local_mining_data = Mutex::new(template);
                    let mut proof_times = Vec::new();
                    let mut current_serf = serf;

                    for proof_num in 0..benchmark_proofs {
                        let mut mining_attempts = tokio::task::JoinSet::<(
                            SerfThread<SaveableCheckpoint>,
                            u64,
                            Result<NounSlab>,
                        )>::new();

                        let start = tokio::time::Instant::now();
                        let _ = mine(
                            current_serf,
                            local_mining_data.lock().await,
                            &mut mining_attempts,
                            None,
                            thread_id as u64,
                        )
                        .await;

                        // Get the serf back from the mining attempt
                        loop {
                            tokio::select! {
                                mining_result = mining_attempts.join_next() => {
                                    if let Some(Ok((returned_serf, _id, _result))) = mining_result {
                                        current_serf = returned_serf;
                                        break;
                                    }
                                }
                            }
                        }
                        let elapsed = start.elapsed();
                        info!(
                            "Thread {} generated proof {} in {:?}",
                            thread_id,
                            proof_num + 1,
                            elapsed
                        );
                        proof_times.push(elapsed);
                    }

                    let total_time: tokio::time::Duration = proof_times.iter().sum();
                    (thread_id as u64, Ok(total_time))
                });
            }
            Ok((thread_id, Err(e))) => {
                error!(thread_index = thread_id, error = ?e, "Could not load mining kernel for benchmark");
            }
            Err(e) => {
                error!(error = ?e, "kernel init task join error in benchmark");
            }
        }
    }

    // Collect results from all benchmark tasks
    let mut proof_times = Vec::new();
    while let Some(result) = benchmark_tasks.join_next().await {
        match result {
            Ok((_thread_id, Ok(elapsed))) => {
                proof_times.push(elapsed);
            }
            Ok((thread_id, Err(e))) => {
                error!("Benchmark thread {} failed: {}", thread_id, e);
            }
            Err(e) => {
                error!("Join error: {}", e);
            }
        }
    }

    // End timing the entire benchmark
    let benchmark_end = tokio::time::Instant::now();
    let total_benchmark_time = benchmark_end - benchmark_start;

    // Calculate and display statistics
    if !proof_times.is_empty() {
        let total_time_all_threads: tokio::time::Duration = proof_times.iter().sum();
        let average_time_per_thread = total_time_all_threads / proof_times.len() as u32;
        let min_time = proof_times.iter().min().unwrap();
        let max_time = proof_times.iter().max().unwrap();

        let total_proofs = proof_times.len() as u32 * benchmark_proofs;
        let average_time_per_proof = total_benchmark_time / total_proofs;
        let proofs_per_minute = if total_benchmark_time.as_secs_f64() > 0.0 {
            (total_proofs as f64 * 60.0) / total_benchmark_time.as_secs_f64()
        } else {
            0.0
        };

        info!("Benchmark Results:");
        info!("  PROOFS PER MINUTE: {:.2}", proofs_per_minute);
        info!("  Average time per proof: {:?}", average_time_per_proof);
        info!("  Threads: {}", num_threads);
        info!("  Proofs per thread: {}", benchmark_proofs);
        info!("  Total proofs: {}", total_proofs);
        info!(
            "  Average time per thread (all proofs): {:?}",
            average_time_per_thread
        );
        info!("  Min thread time: {:?}", min_time);
        info!("  Max thread time: {:?}", max_time);
        info!(
            "  Total time across all threads: {:?}",
            total_time_all_threads
        );
        info!(
            "  Total benchmark time (wall clock): {:?}",
            total_benchmark_time
        );
    } else {
        warn!("No proofs were generated during benchmark");
    }

    return Ok(());
}

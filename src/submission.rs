use async_trait::async_trait;
use tokio::sync::{watch, Mutex};
use anyhow::{Result, anyhow};
use tracing::info;
use std::sync::Arc;

use quiver::types::{Submission, SubmissionResponse};
use quiver::submission::{SubmissionProvider, SubmissionResponseHandler};

#[derive(Clone, Debug)]
pub struct NockPoolSubmissionProvider {
    pub submission_rx: Arc<Mutex<watch::Receiver<Submission>>>,
}

impl NockPoolSubmissionProvider {
    pub fn new(submission_rx: watch::Receiver<Submission>) -> Self {
        Self {
            submission_rx: Arc::new(Mutex::new(submission_rx)),
        }
    }
}

#[async_trait]
impl SubmissionProvider for NockPoolSubmissionProvider {
    async fn submit(&self) -> Result<Submission> {
        // Lock the mutex to get exclusive access to the single, shared receiver.
        let mut guard = self.submission_rx.lock().await;

        // Wait for the value to change from what the receiver has last seen.
        // This mutates the guarded receiver's internal "seen" state.
        // If the channel is closed, this will return an error.
        guard.changed().await?;

        // After a change has been received, borrow the new value and return it.
        // The mutex lock is released when `guard` goes out of scope here.
        let submission = guard.borrow().clone();
        Ok(submission)
    }
}

#[derive(Clone, Debug)]
pub struct NockPoolSubmissionResponseHandler {}

impl NockPoolSubmissionResponseHandler {
    pub fn new() -> Self {
        Self {}
    }

    async fn handle_verification_failure(&self, response: &SubmissionResponse) -> Result<()> {
        tracing::error!("Proof rejected by verifier - forcing reconnect to template provider");
        tracing::error!("  Digest: {:?}", response.digest);
        tracing::error!("  Reason: {}", response.message);
        
        // Return an error to force the quiver client to fail and reconnect
        Err(anyhow!("Verification failed, forcing reconnection: {}", response.message))
    }

    async fn handle_verification_success(&self, response: &SubmissionResponse) -> Result<()> {
        tracing::info!("Proof accepted by verifier");
        tracing::info!("  Digest: {:?}", response.digest);
        Ok(())
    }
}

#[async_trait]
impl SubmissionResponseHandler for NockPoolSubmissionResponseHandler {
    async fn handle(&self, response: SubmissionResponse) -> Result<()> {
        info!("Received verification response: {:?}", response);
        
        // Check if verification failed and take action
        if !response.success {
            tracing::warn!("Proof verification failed: {}", response.message);
            
            // Add your custom actions here when verification fails:
            // Examples:
            // - Log detailed failure information
            // - Send metrics/telemetry
            // - Adjust mining parameters
            // - Notify external systems
            // - Implement retry logic
            
            // Example actions:
            tracing::error!("Verification failure details - Digest: {:?}, Reason: {}", 
                response.digest, response.message);
            
            // You can add additional failure handling here
            self.handle_verification_failure(&response).await?;
        } else {
            tracing::info!("Proof successfully verified! Digest: {:?}", response.digest);
            
            // Add your custom actions here when verification succeeds:
            // Examples:
            // - Update success metrics
            // - Log success information
            // - Notify monitoring systems
            
            self.handle_verification_success(&response).await?;
        }
        
        Ok(())
    }
}

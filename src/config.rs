use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// The secret key for authentication with the nockpool server.
    #[arg(long)]
    pub key: Option<String>,

    /// Account token for generating mining tokens (alternative to --key).
    #[arg(long, env = "NOCKPOOL_ACCOUNT_TOKEN")]
    pub account_token: Option<String>,

    /// Set the maximum number of threads to use for mining. Uses all available cores if not set.
    #[arg(long)]
    pub max_threads: Option<u32>,

    /// The `ip:port` of the nockpool server.
    #[arg(long, default_value = "quiver.nockpool.com:27016")]
    pub server_address: String,

    /// The `ip:port` of the quiver client.
    #[arg(long, default_value = "0.0.0.0:27017")]
    pub client_address: String,

    /// If we only want to mine for network shares, set this to true.
    #[arg(long, default_value_t = false)]
    pub network_only: bool,

    /// If we want to use an insecure connection to the nockpool server, set this to true.
    #[arg(long, default_value_t = false)]
    pub insecure: bool,

    /// Run benchmarking tool to test the performance of the miner.
    #[arg(long, default_value_t = false)]
    pub benchmark: bool,

    /// Number of proofs to generate per thread during benchmark.
    #[arg(long, default_value_t = 1)]
    pub benchmark_proofs: u32,

    /// Clear stored mining key and exit.
    #[arg(long, default_value_t = false)]
    pub clear_key: bool,

    /// Base URL for the NockPool API (for local development).
    #[arg(long, env = "NOCKPOOL_API_URL", default_value = "https://nockpool.com")]
    pub api_url: String,

    /// Tracing collection endpoint
    #[arg(long, env = "ZIPKIN_ENDPOINT", default_value = "https://telemetry.nockpool.com:443")]
    pub collector_url: Option<String>,
}

impl Config {
    pub fn validate_auth(&self) -> Result<(), String> {
        if self.benchmark || self.clear_key {
            return Ok(());
        }

        let has_key = self.key.is_some();
        let has_account_token = self.account_token.is_some();

        match (has_key, has_account_token) {
            (true, false) => Ok(()),   // Direct key provided
            (false, true) => Ok(()),   // Account token provided
            (false, false) => Err("Either --key or --account-token must be provided".to_string()),
            (true, true) => Err("Cannot specify both --key and --account-token".to_string()),
        }
    }
}

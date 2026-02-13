# 🌊 NockPool by SWPSCo

<img width="624" height="206" alt="Nockpool logo" src="https://github.com/user-attachments/assets/cab9f6bd-0279-4d17-9c90-485954464394" />


#### **Important note**: We recommend installing via the [NockPool Launcher](https://github.com/SWPSCO/nockpool-miner-launcher) application, which will automatically keep your miner up to date!

This repo includes code and binaries necessary to participate in [Nockpool](https://nockpool.com), the premier [Nockchain](https://nockchain.org) mining pool, with your Linux or Apple Silicon machines.

### Install

You can download the prebuilt binaries in the release tab. The macOS bins are codesigned and the Linux bins are SLSA3 attested -- we recommend [verifying](https://github.com/slsa-framework/slsa-verifier).

### Run

#### Option 1: Using Account Token (Recommended)
```bash
nockpool-miner --account-token nockacct_youraccounttokenhere --max-threads 12
```

#### Option 2: Using Device Key
```bash
nockpool-miner --key nockpool_yourdevicekeyhere123 --max-threads 12
```

---

## FAQ

#### How do I get started?

1. Create an account at [nockpool.com](https://nockpool.com)
2. Generate an **account token** in your dashboard (recommended)
3. Use the account token with `--account-token` flag
4. The miner will automatically create and manage device keys for you

#### What's the difference between account tokens and device keys?

- **Account tokens** (`nockacct_*`): Long-lived tokens that can create and manage multiple device keys.
- **Device keys** (`nock_*`): Individual mining tokens created automatically by account tokens. One per mining device.

#### Where do I get tokens?

Create an account at [nockpool.com](https://nockpool.com) and generate account tokens in your dashboard.

#### How many threads should I use?

Logical cores times two minus 4 is a good rule of thumb. E.g., if you have a 16 core Ryzen capable of 32 threads, 28 would be a good target.

#### How much memory do I need?

As much as you can get! Recommended 8GB + 2.5 per thread.

#### How do I use custom jets?

Just swap out the `zkvm-jetpack` dependency in `Cargo.toml`.

--- 

### Building

Clone repo:

```
git clone https://github.com/SWPSCO/nockpool-miner
```

Build:

```bash
cargo build --release
```

Run: 

```bash
# With account token (recommended)
target/release/nockpool-miner --account-token nockacct_youraccounttokenhere

# Or with device key
target/release/nockpool-miner --key nockpool_yourdevicekeyhere123
```

## Command Line Options

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--account-token` | `NOCKPOOL_ACCOUNT_TOKEN` | - | Account token for generating mining tokens (recommended). |
| `--key` | `KEY` | - | Direct device key for authentication. |
| `--api-url` | `NOCKPOOL_API_URL` | `https://nockpool.com` | Base URL for NockPool token management API (for development). |
| `--max-threads` | `MAX_THREADS` | (all available threads - 2) | Set the maximum number of threads to use for mining. |
| `--server-address` | `SERVER_ADDRESS` | `quiver.nockpool.com:27016` | The `ip:port` of the nockpool server. |
| `--client-address` | `CLIENT_ADDRESS` | `0.0.0.0:27017` | The `ip:port` of the quiver client. |
| `--network-only` | `NETWORK_ONLY` | `false` | Mine only for network shares. |
| `--insecure` | `INSECURE` | `false` | Use insecure connection to the nockpool server. |
| `--benchmark` | `BENCHMARK` | `false` | Run benchmarking tool. Ignores all other arguments. |
| `--clear-key` | - | `false` | Clear stored mining key and exit. |
| `--jam` | `MINER_JAM_PATH` | `./miner.jam` | Path to jamfile for miner |
| `--lib-dir` | - | `./libzkvm_jetpack.so` | Path to dynamic libraries |

**Note:** Either `--account-token` or `--key` must be provided (but not both).

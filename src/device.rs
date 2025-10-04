use sysinfo::System;
use quiver::device_info::DeviceInfo;
use crate::miner::get_current_proof_rate;
use std::process::Command;

pub fn get_device_info() -> DeviceInfo {
    let mut sys = System::new();
    sys.refresh_cpu();
    sys.refresh_memory();
    let os = System::long_os_version().unwrap_or_else(|| "Unknown OS".to_string());
    let cpu_model = sys.cpus().first().map_or("Unknown CPU".to_string(), |cpu| cpu.brand().to_string());
    // Convert from bytes to gigabytes
    let ram_capacity_gb = sys.total_memory() / (1024 * 1024 * 1024);

    DeviceInfo {
        os: os.clone().trim().to_string(),
        cpu_model: cpu_model.clone().trim().to_string(),
        ram_capacity_gb,
    }
}

pub fn get_device_info_with_proof_rate() -> (DeviceInfo, f64) {
    let device_info = get_device_info();
    let proof_rate = get_current_proof_rate();
    (device_info, proof_rate)
}

pub fn get_gpu_info() -> Option<String> {
    let mut gpu_models: Vec<String> = Vec::new();

    // Only detect GPUs on Linux
    #[cfg(target_os = "linux")]
    {
        // Try to detect NVIDIA GPUs
        gpu_models.extend(detect_nvidia_gpus());

        // Try to detect AMD GPUs
        gpu_models.extend(detect_amd_gpus());

        // Try to detect Intel GPUs
        gpu_models.extend(detect_intel_gpus());
    }

    if gpu_models.is_empty() {
        None
    } else {
        // Clean up GPU names (don't remove duplicates - we want to count multiple identical GPUs)
        let mut cleaned_gpus: Vec<String> = gpu_models.into_iter()
            .map(|gpu| clean_gpu_name(&gpu))
            .filter(|gpu| !gpu.is_empty())
            .collect();

        // Filter out generic names if we have specific ones
        let has_specific_nvidia = cleaned_gpus.iter().any(|gpu| gpu.contains("GeForce") || gpu.contains("RTX") || gpu.contains("GTX"));
        let has_specific_amd = cleaned_gpus.iter().any(|gpu| gpu.contains("Radeon") || gpu.contains("RX "));
        let has_specific_intel = cleaned_gpus.iter().any(|gpu| gpu.contains("UHD") || gpu.contains("Iris") || gpu.contains("Arc"));

        cleaned_gpus.retain(|gpu| {
            if gpu == "NVIDIA GPU" && has_specific_nvidia {
                false
            } else if gpu == "AMD GPU" && has_specific_amd {
                false
            } else if gpu == "Intel GPU" && has_specific_intel {
                false
            } else if gpu.starts_with("Advanced Micro Devices") || gpu.starts_with("NVIDIA Corporation") || gpu.starts_with("Intel Corporation") {
                false // Filter out verbose manufacturer names
            } else {
                true
            }
        });

        if cleaned_gpus.is_empty() {
            None
        } else {
            // Count identical GPUs and format as "2x GPU Name" if there are duplicates
            let mut gpu_counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
            for gpu in cleaned_gpus {
                *gpu_counts.entry(gpu).or_insert(0) += 1;
            }

            let mut formatted_gpus: Vec<String> = gpu_counts.into_iter()
                .map(|(gpu, count)| {
                    if count > 1 {
                        format!("{}x {}", count, gpu)
                    } else {
                        gpu
                    }
                })
                .collect();

            // Sort for consistent ordering
            formatted_gpus.sort();

            Some(formatted_gpus.join(", "))
        }
    }
}

fn clean_gpu_name(gpu_name: &str) -> String {
    let mut cleaned = gpu_name.to_string();

    // Remove device IDs and revision info from lspci output
    // Example: "NVIDIA Corporation Device [10de:2786] (rev a1)" -> "NVIDIA Corporation Device"
    if let Some(pos) = cleaned.find(" [") {
        cleaned = cleaned[..pos].to_string();
    }

    // Remove revision info
    if let Some(pos) = cleaned.find(" (rev ") {
        cleaned = cleaned[..pos].to_string();
    }

    // Clean up common patterns
    cleaned = cleaned.replace("Advanced Micro Devices, Inc. [AMD/ATI] Device", "AMD GPU")
        .replace("NVIDIA Corporation Device", "NVIDIA GPU")
        .replace("Intel Corporation Device", "Intel GPU")
        .replace("  ", " ")
        .trim()
        .to_string();

    // If it's just a generic name, skip it unless we already have a proper name
    if cleaned == "AMD GPU" || cleaned == "NVIDIA GPU" || cleaned == "Intel GPU" {
        if gpu_name.contains("GeForce") || gpu_name.contains("Radeon") || gpu_name.contains("UHD") ||
           gpu_name.contains("Iris") || gpu_name.contains("RTX") || gpu_name.contains("GTX") {
            // Try to extract the actual model name
            if let Some(model_start) = gpu_name.find("GeForce") {
                if let Some(model_end) = gpu_name[model_start..].find(" [") {
                    return format!("NVIDIA {}", &gpu_name[model_start..model_start + model_end]);
                }
            }
            if let Some(model_start) = gpu_name.find("Radeon") {
                if let Some(model_end) = gpu_name[model_start..].find(" [") {
                    return format!("AMD {}", &gpu_name[model_start..model_start + model_end]);
                }
            }
        }
    }

    cleaned
}

#[cfg(target_os = "linux")]
fn detect_nvidia_gpus() -> Vec<String> {
    let mut gpu_models = Vec::new();

    // Check if nvidia-smi is available and working
    if let Ok(output) = Command::new("nvidia-smi")
        .args(&["--query-gpu=name", "--format=csv,noheader,nounits"])
        .output()
    {
        if output.status.success() {
            let output_str = String::from_utf8_lossy(&output.stdout);
            for line in output_str.lines() {
                let name = line.trim();
                if !name.is_empty() {
                    gpu_models.push(name.to_string());
                }
            }
        }
    }

    gpu_models
}

#[cfg(target_os = "linux")]
fn detect_amd_gpus() -> Vec<String> {
    let mut gpu_models = Vec::new();

    // Check lspci for AMD GPUs
    if let Ok(output) = Command::new("lspci")
        .args(&["-nn"])
        .output()
    {
        if output.status.success() {
            let output_str = String::from_utf8_lossy(&output.stdout);
            for line in output_str.lines() {
                let line_lower = line.to_lowercase();
                if (line_lower.contains("amd") || line_lower.contains("ati")) &&
                   (line_lower.contains("vga") || line_lower.contains("display") || line_lower.contains("3d")) {

                    // Extract GPU name from lspci output
                    let parts: Vec<&str> = line.split(": ").collect();
                    let model = if parts.len() > 1 {
                        parts[1].to_string()
                    } else {
                        "AMD GPU".to_string()
                    };

                    gpu_models.push(model);
                }
            }
        }
    }

    gpu_models
}

#[cfg(target_os = "linux")]
fn detect_intel_gpus() -> Vec<String> {
    let mut gpu_models = Vec::new();

    // Check lspci for Intel GPUs
    if let Ok(output) = Command::new("lspci")
        .args(&["-nn"])
        .output()
    {
        if output.status.success() {
            let output_str = String::from_utf8_lossy(&output.stdout);
            for line in output_str.lines() {
                let line_lower = line.to_lowercase();
                if line_lower.contains("intel") &&
                   (line_lower.contains("vga") || line_lower.contains("display") || line_lower.contains("graphics")) {

                    // Extract GPU name from lspci output
                    let parts: Vec<&str> = line.split(": ").collect();
                    let model = if parts.len() > 1 {
                        parts[1].to_string()
                    } else {
                        "Intel GPU".to_string()
                    };

                    gpu_models.push(model);
                }
            }
        }
    }

    gpu_models
}
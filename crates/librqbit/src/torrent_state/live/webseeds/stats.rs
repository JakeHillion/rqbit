use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct AggregateWebSeedStats {
    pub total_bytes_downloaded: u64,
    pub total_requests_succeeded: u64,
    pub total_requests_failed: u64,
    pub active_seeds: usize,
    pub dead_seeds: usize,
    pub backing_off_seeds: usize,
}

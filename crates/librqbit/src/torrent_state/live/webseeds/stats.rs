use serde::{Deserialize, Serialize};

/// Statistics for a single web seed
#[derive(Debug, Clone, Serialize)]
pub struct WebSeedStatsSnapshot {
    pub url: String,
    pub state: &'static str,
    pub bytes_downloaded: u64,
    pub requests_succeeded: u64,
    pub requests_failed: u64,
}

/// Combined web seed statistics: per-seed details and aggregate totals
#[derive(Debug, Clone, Serialize, Default)]
pub struct WebSeedsStatsSnapshot {
    pub seeds: Vec<WebSeedStatsSnapshot>,
    pub aggregate: AggregateWebSeedStats,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct AggregateWebSeedStats {
    pub total_bytes_downloaded: u64,
    pub total_requests_succeeded: u64,
    pub total_requests_failed: u64,
    pub active_seeds: usize,
    pub dead_seeds: usize,
    pub backing_off_seeds: usize,
}

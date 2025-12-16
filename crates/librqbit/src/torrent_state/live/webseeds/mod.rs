use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

pub mod downloader;
pub mod requester;

pub use downloader::WebSeedDownloader;
pub use requester::task_webseed_chunk_requester;

/// Represents a single web seed URL
pub struct WebSeed {
    /// Statistics for this web seed
    pub stats: WebSeedStats,
    /// Exponential backoff state
    pub backoff: BackoffState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSeedState {
    Active,
    BackingOff,
    Dead,
}

pub struct WebSeedStats {
    /// Total bytes downloaded from this web seed
    pub bytes_downloaded: std::sync::atomic::AtomicU64,
    /// Number of successful requests
    pub requests_succeeded: std::sync::atomic::AtomicU64,
    /// Number of failed requests
    pub requests_failed: std::sync::atomic::AtomicU64,
}

impl WebSeedStats {
    pub fn new() -> Self {
        Self {
            bytes_downloaded: std::sync::atomic::AtomicU64::new(0),
            requests_succeeded: std::sync::atomic::AtomicU64::new(0),
            requests_failed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

pub struct BackoffState {
    /// Current state of this web seed
    state: RwLock<WebSeedState>,
    /// When we last encountered a failure
    last_failure: RwLock<Option<Instant>>,
    /// Number of consecutive failures
    consecutive_failures: RwLock<u32>,
}

impl BackoffState {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(WebSeedState::Active),
            last_failure: RwLock::new(None),
            consecutive_failures: RwLock::new(0),
        }
    }

    pub fn get_state(&self) -> WebSeedState {
        *self.state.read()
    }

    pub fn mark_success(&self) {
        *self.consecutive_failures.write() = 0;
        *self.state.write() = WebSeedState::Active;
        *self.last_failure.write() = None;
    }

    pub fn mark_failure(&self, permanent: bool) {
        let mut failures = self.consecutive_failures.write();
        *failures += 1;

        let mut state = self.state.write();
        if permanent {
            *state = WebSeedState::Dead;
        } else {
            *state = WebSeedState::BackingOff;
        }

        *self.last_failure.write() = Some(Instant::now());
    }

    /// Check if we should retry based on exponential backoff
    /// Returns true if enough time has passed since the last failure
    pub fn should_retry(&self) -> bool {
        let state = self.state.read();
        if *state == WebSeedState::Dead {
            return false;
        }
        if *state == WebSeedState::Active {
            return true;
        }

        // Exponential backoff: 1s, 2s, 4s, 8s, ... up to 5 minutes
        let failures = *self.consecutive_failures.read();
        let backoff_secs = std::cmp::min(1u32 << failures, 300);
        let backoff_duration = Duration::from_secs(backoff_secs as u64);

        if let Some(last_failure) = *self.last_failure.read() {
            last_failure.elapsed() >= backoff_duration
        } else {
            true
        }
    }
}

impl WebSeed {
    pub fn new() -> Self {
        Self {
            stats: WebSeedStats::new(),
            backoff: BackoffState::new(),
        }
    }
}

/// Manages all web seeds for a torrent
pub struct WebSeedStates {
    pub seeds: DashMap<String, WebSeed>,
}

impl WebSeedStates {
    pub fn new() -> Self {
        Self {
            seeds: DashMap::new(),
        }
    }

    pub fn add_seed(&self, url: String) {
        self.seeds.insert(url, WebSeed::new());
    }

    pub fn get_active_seeds(&self) -> Vec<String> {
        self.seeds
            .iter()
            .filter(|entry| {
                let seed = entry.value();
                seed.backoff.get_state() == WebSeedState::Active || seed.backoff.should_retry()
            })
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn mark_success(&self, url: &str) {
        if let Some(seed) = self.seeds.get(url) {
            seed.backoff.mark_success();
        }
    }

    pub fn mark_failure(&self, url: &str, permanent: bool) {
        if let Some(seed) = self.seeds.get(url) {
            seed.backoff.mark_failure(permanent);
        }
    }

    pub fn record_bytes_downloaded(&self, url: &str, bytes: u64) {
        if let Some(seed) = self.seeds.get(url) {
            seed.stats
                .bytes_downloaded
                .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn record_request_success(&self, url: &str) {
        if let Some(seed) = self.seeds.get(url) {
            seed.stats
                .requests_succeeded
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn record_request_failure(&self, url: &str) {
        if let Some(seed) = self.seeds.get(url) {
            seed.stats
                .requests_failed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Stats {
    pub received: AtomicU64,
    pub forwarded: AtomicU64,
    pub deduplicated: AtomicU64,
    pub errors: AtomicU64,
}

impl Stats {
    pub fn inc_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_forwarded(&self) {
        self.forwarded.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_deduplicated(&self) {
        self.deduplicated.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_errors(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            forwarded: self.forwarded.load(Ordering::Relaxed),
            deduplicated: self.deduplicated.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct StatsSnapshot {
    pub received: u64,
    pub forwarded: u64,
    pub deduplicated: u64,
    pub errors: u64,
}

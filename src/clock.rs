use std::time::SystemTime;

#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(test)]
use std::time::{Duration, UNIX_EPOCH};

/// Trait for getting the current time, enabling dependency injection for testing
pub trait Clock: Send + Sync {
    fn now(&self) -> SystemTime;
}

/// Real system clock implementation
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Mock clock for testing with controllable time
#[cfg(test)]
#[derive(Debug)]
pub struct MockClock {
    /// Current time as nanoseconds since UNIX_EPOCH
    nanos: AtomicU64,
}

#[cfg(test)]
impl MockClock {
    /// Create a new mock clock starting at the given time
    pub fn new(start: SystemTime) -> Self {
        let nanos = start
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos() as u64;
        Self {
            nanos: AtomicU64::new(nanos),
        }
    }

    /// Create a new mock clock starting at a specific timestamp (seconds since epoch)
    pub fn at_secs(secs: u64) -> Self {
        Self::new(UNIX_EPOCH + Duration::from_secs(secs))
    }

    /// Advance the clock by the given duration
    pub fn advance(&self, duration: Duration) {
        self.nanos.fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }

    /// Set the clock to a specific time
    pub fn set(&self, time: SystemTime) {
        let nanos = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos() as u64;
        self.nanos.store(nanos, Ordering::SeqCst);
    }

    /// Get the current time as a SystemTime
    pub fn get(&self) -> SystemTime {
        let nanos = self.nanos.load(Ordering::SeqCst);
        UNIX_EPOCH + Duration::from_nanos(nanos)
    }
}

#[cfg(test)]
impl Default for MockClock {
    fn default() -> Self {
        // Default to a fixed time for reproducibility: 2024-01-01 00:00:00 UTC
        Self::at_secs(1704067200)
    }
}

#[cfg(test)]
impl Clock for MockClock {
    fn now(&self) -> SystemTime {
        self.get()
    }
}

/// Type alias for a shared clock reference
pub type SharedClock = std::sync::Arc<dyn Clock>;

/// Create a shared system clock
pub fn system_clock() -> SharedClock {
    std::sync::Arc::new(SystemClock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_clock() {
        let clock = SystemClock;
        let before = SystemTime::now();
        let from_clock = clock.now();
        let after = SystemTime::now();

        assert!(from_clock >= before);
        assert!(from_clock <= after);
    }

    #[test]
    fn test_mock_clock_default() {
        let clock = MockClock::default();
        let time = clock.now();
        // Should be 2024-01-01 00:00:00 UTC
        assert_eq!(
            time.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            1704067200
        );
    }

    #[test]
    fn test_mock_clock_advance() {
        let clock = MockClock::at_secs(1000);
        assert_eq!(clock.now().duration_since(UNIX_EPOCH).unwrap().as_secs(), 1000);

        clock.advance(Duration::from_secs(500));
        assert_eq!(clock.now().duration_since(UNIX_EPOCH).unwrap().as_secs(), 1500);
    }

    #[test]
    fn test_mock_clock_set() {
        let clock = MockClock::at_secs(1000);
        clock.set(UNIX_EPOCH + Duration::from_secs(2000));
        assert_eq!(clock.now().duration_since(UNIX_EPOCH).unwrap().as_secs(), 2000);
    }
}

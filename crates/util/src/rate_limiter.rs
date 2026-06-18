/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

/// Smooth, shared rate limiter measured in units/second (e.g. records/second).
///
/// Implemented as a shared "next available instant" cursor (GCRA / leaky-bucket
/// style): acquiring `units` permits costs `units / rate` seconds, and each
/// caller reserves the next free slot on a shared timeline. Reservations are
/// serialized by a brief mutex (no `.await` held across it); the actual wait
/// happens outside the lock. Because every concurrent caller reserves against
/// the *same* cursor, the aggregate rate across all callers converges to the
/// configured units/second regardless of how many tasks race.
///
/// There is no burst credit: an idle period does not let a later caller fire a
/// catch-up burst (the cursor never advances ahead of "now"), and arbitrarily
/// large requests are handled by simply reserving a proportionally longer slot.
#[derive(Debug)]
pub struct RateLimiter {
    units_per_sec: f64,
    /// Earliest instant at which the next reserved acquisition may proceed.
    next_available: Mutex<Instant>,
}

impl RateLimiter {
    /// Creates a limiter that permits `units_per_sec` units per second.
    #[must_use]
    pub fn new(units_per_sec: u64) -> Self {
        Self {
            units_per_sec: units_per_sec as f64,
            next_available: Mutex::new(Instant::now()),
        }
    }

    /// Reserves a slot for `units` and returns how long the caller must wait
    /// before proceeding (`Duration::ZERO` if it may proceed immediately).
    #[must_use]
    pub fn reserve(&self, units: usize) -> Duration {
        if units == 0 || self.units_per_sec <= 0.0 {
            return Duration::ZERO;
        }
        let cost = Duration::from_secs_f64(units as f64 / self.units_per_sec);
        let now = Instant::now();
        let mut next = self
            .next_available
            .lock()
            .expect("rate limiter lock poisoned");
        // Don't bank credit while idle: never start earlier than `now`.
        let start = (*next).max(now);
        *next = start + cost;
        start.saturating_duration_since(now)
    }

    /// Waits until `units` may be acquired under the configured rate.
    pub async fn acquire(&self, units: usize) {
        let wait = self.reserve(units);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RateLimiter;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn first_acquire_is_immediate() {
        let limiter = RateLimiter::new(10_000);
        // An idle limiter grants the first request without waiting.
        assert_eq!(limiter.reserve(5_000), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn paces_subsequent_acquires() {
        // 10k units/sec → 5k units costs 500ms.
        let limiter = RateLimiter::new(10_000);
        assert_eq!(limiter.reserve(5_000), Duration::ZERO);
        // The next 5k must wait ~500ms for the slot reserved by the first.
        assert_eq!(limiter.reserve(5_000), Duration::from_millis(500));
        // And the one after that waits a full second (two slots ahead).
        assert_eq!(limiter.reserve(5_000), Duration::from_millis(1_000));
    }

    #[tokio::test(start_paused = true)]
    async fn no_burst_credit_after_idle() {
        let limiter = RateLimiter::new(10_000);
        assert_eq!(limiter.reserve(10_000), Duration::ZERO);
        // Idle past the reserved slot; the cursor should not bank credit.
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(limiter.reserve(10_000), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn handles_request_larger_than_one_second() {
        // 10k/sec, ask for 50k → must wait ~5s, no capacity error.
        let limiter = RateLimiter::new(10_000);
        assert_eq!(limiter.reserve(10_000), Duration::ZERO);
        assert_eq!(limiter.reserve(50_000), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_units_never_waits() {
        let limiter = RateLimiter::new(10_000);
        assert_eq!(limiter.reserve(0), Duration::ZERO);
    }
}

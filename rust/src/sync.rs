//! Small shared helpers.

use std::sync::{Mutex, MutexGuard};

/// Poison-recovering `Mutex::lock`.
///
/// The engine's per-instance mutexes guard plain collections with no cross-field
/// invariant, so a panic while a lock is held cannot leave the data in a dangerous state.
/// Recovering means one panicked call fails, rather than every later call on the same
/// engine panicking on a poisoned lock (which would wedge `wait_idle`/`engineClose`).
pub(crate) trait MutexExt<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_or_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

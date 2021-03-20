use std::sync::atomic::{AtomicBool, Ordering};

pub struct AtomicLock {
    flag: AtomicBool,
}

impl Default for AtomicLock {
    fn default() -> Self {
        Self {
            flag: AtomicBool::new(false),
        }
    }
}

impl AtomicLock {
    pub fn new() -> Self {
        Self {
            flag: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) -> Option<AtomicLockGuard> {
        if self
            .flag
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return None;
        }
        Some(AtomicLockGuard::new(self))
    }
}

pub struct AtomicLockGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> AtomicLockGuard<'a> {
    fn new(lock: &'a AtomicLock) -> Self {
        Self { flag: &lock.flag }
    }
}

impl<'a> Drop for AtomicLockGuard<'a> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_lock() {
        let lock = AtomicLock::default();
        {
            let guard = lock.lock();
            assert!(guard.is_some());

            assert!(lock.lock().is_none());
            assert!(lock.lock().is_none());
        }
        assert!(lock.lock().is_some());
    }
}

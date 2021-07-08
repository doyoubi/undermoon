use super::utils::SLOT_NUM;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct SlotMutex {
    slots: Vec<AtomicBool>,
}

impl Default for SlotMutex {
    fn default() -> Self {
        Self {
            slots: std::iter::repeat_with(|| AtomicBool::new(false))
                .take(SLOT_NUM)
                .collect(),
        }
    }
}

impl SlotMutex {
    pub fn lock(&self, slot: usize) -> Option<SlotMutexGuard> {
        if let Some(s) = self.slots.get(slot) {
            if let Ok(false) = s.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst) {
                return Some(SlotMutexGuard { flag: s });
            }
        } else {
            error!("SlotMutex::lock invalid slot: {}", slot);
        }
        None
    }
}

pub struct SlotMutexGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> Drop for SlotMutexGuard<'a> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_mutex() {
        let mutex = SlotMutex::default();
        for slot in [0, 5000, SLOT_NUM - 1].iter() {
            let _guard = mutex.lock(*slot).unwrap();
            assert!(mutex.lock(*slot).is_none());
        }
        assert!(mutex.lock(SLOT_NUM).is_none());
    }
}

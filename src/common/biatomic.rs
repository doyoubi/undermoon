use std::sync::atomic::{AtomicU64, Ordering};

pub struct BiAtomicU32 {
    inner: AtomicU64,
}

impl BiAtomicU32 {
    pub fn new(num1: u32, num2: u32) -> Self {
        Self {
            inner: AtomicU64::new(Self::combine_two_num(num1, num2)),
        }
    }

    pub fn load(&self) -> (u32, u32) {
        let num = self.inner.load(Ordering::SeqCst);
        Self::split_to_two_num(num)
    }

    pub fn compare_and_apply<F1, F2>(&self, num1_func: F1, num2_func: F2) -> (u32, u32)
    where
        F1: Fn(u32) -> u32,
        F2: Fn(u32) -> u32,
    {
        loop {
            let old_num = self.inner.load(Ordering::SeqCst);
            let (old_num1, old_num2) = Self::split_to_two_num(old_num);
            let new_num1 = num1_func(old_num1);
            let new_num2 = num2_func(old_num2);
            let new_num = Self::combine_two_num(new_num1, new_num2);
            let prev_num = self
                .inner
                .compare_and_swap(old_num, new_num, Ordering::SeqCst);
            if prev_num == old_num {
                return (old_num1, old_num2);
            }
        }
    }

    fn combine_two_num(num1: u32, num2: u32) -> u64 {
        let n1 = (num1 as u64) << 32;
        let n2 = num2 as u64;
        n1 + n2
    }

    fn split_to_two_num(num: u64) -> (u32, u32) {
        let part2_mask = (!0u32) as u64;
        let part1_mask = part2_mask << 32;
        let part1 = (num & part1_mask) >> 32;
        let part2 = num & part2_mask;
        (part1 as u32, part2 as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_num_conversion() {
        let bi = BiAtomicU32::new(233, 666);
        assert_eq!(bi.load(), (233, 666));
        let old = bi.compare_and_apply(|n| n + 1, |n| n + 1);
        assert_eq!(old, (233, 666));
        assert_eq!(bi.load(), (234, 667));
        let old = bi.compare_and_apply(|n| n - 1, |n| n + 1);
        assert_eq!(old, (234, 667));
        assert_eq!(bi.load(), (233, 668));
    }

    #[test]
    fn test_bounder() {
        const M: u32 = u32::MAX;
        {
            let bi = BiAtomicU32::new(0, M);
            assert_eq!(bi.load(), (0, M));
            let old = bi.compare_and_apply(|n| n + 1, |n| n - 1);
            assert_eq!(old, (0, M));
            assert_eq!(bi.load(), (1, M - 1));
            let old = bi.compare_and_apply(|n| n.overflowing_sub(2).0, |n| n.overflowing_add(2).0);
            assert_eq!(old, (1, M - 1));
            assert_eq!(bi.load(), (M, 0));
        }
        {
            let bi = BiAtomicU32::new(M, 0);
            assert_eq!(bi.load(), (M, 0));
            let old = bi.compare_and_apply(|n| n - 1, |n| n + 1);
            assert_eq!(old, (M, 0));
            assert_eq!(bi.load(), (M - 1, 1));
            let old = bi.compare_and_apply(|n| n.overflowing_add(2).0, |n| n.overflowing_sub(2).0);
            assert_eq!(old, (M - 1, 1));
            assert_eq!(bi.load(), (0, M));
        }
    }
}

use core::mem;
use core::pin::Pin;
use futures::stream::{Fuse, FusedStream, Stream};
use futures::task::{Context, Poll};
use futures::Future;
use futures::StreamExt;
#[cfg(feature = "sink")]
use futures_sink::Sink;
use futures_timer::Delay;
use pin_project::pin_project;
use std::cmp::{max, min};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct BatchStats {
    last_wbuf_flush_size: AtomicUsize,
    last_flush_interval: AtomicU32,
}

impl Default for BatchStats {
    fn default() -> Self {
        Self {
            last_wbuf_flush_size: AtomicUsize::new(0),
            last_flush_interval: AtomicU32::new(0),
        }
    }
}

impl BatchStats {
    pub fn update(&self, last_wbuf_flush_size: usize, last_flush_interval: Duration) {
        self.last_wbuf_flush_size
            .store(last_wbuf_flush_size, Ordering::Relaxed);
        // Ignore the seconds part for simplicity.
        self.last_flush_interval
            .store(last_flush_interval.subsec_nanos(), Ordering::Relaxed);
    }

    pub fn get_flush_size(&self) -> usize {
        self.last_wbuf_flush_size.load(Ordering::Relaxed)
    }

    pub fn get_flush_interval(&self) -> u32 {
        self.last_flush_interval.load(Ordering::Relaxed)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BatchStrategy {
    Disabled,
    Fixed,
    Dynamic,
}

const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
const BEST_RECORD_TTL: Duration = Duration::from_secs(300);

struct BatchOptimizer {
    wbuf_size: usize,
    best_flush_size: usize,

    best_spr: u128,
    best_start_time: Instant,

    curr_flush_size: usize,
    curr_data_requested: usize,

    increase_flush_size: bool,
    last_record_time: Instant,
}

impl BatchOptimizer {
    fn new(flush_size: NonZeroUsize, wbuf_size: usize) -> Self {
        Self {
            wbuf_size,
            best_flush_size: flush_size.get(),
            best_spr: u128::MAX,
            best_start_time: Instant::now(),
            curr_flush_size: flush_size.get(),
            curr_data_requested: 0,
            increase_flush_size: true,
            last_record_time: Instant::now(),
        }
    }

    fn get_curr_flush_size(&self) -> usize {
        self.curr_flush_size
    }

    fn add_data(&mut self, len: usize) {
        self.curr_data_requested += len;
    }

    fn tick(&mut self, now: Instant) {
        let interval = now.duration_since(self.last_record_time);
        if interval < SAMPLE_INTERVAL {
            return;
        }
        self.last_record_time = now;

        if self.curr_data_requested == 0 {
            return;
        }

        if now.duration_since(self.best_start_time) > BEST_RECORD_TTL {
            // Make it be replaced next time
            self.best_spr = u128::MAX;
        }

        let spr = interval.as_nanos() / (self.curr_data_requested as u128);
        if spr < self.best_spr {
            self.best_spr = spr;
            self.best_flush_size = self.curr_flush_size;
            self.best_start_time = now;
        }

        const MAX_RANDOM_DT: usize = 256;
        let random_dt = (interval.subsec_nanos() as usize) % MAX_RANDOM_DT;
        self.curr_flush_size = if self.increase_flush_size {
            let d = max(self.best_flush_size / 10, random_dt);
            self.curr_flush_size + d
        } else {
            let d = max(self.best_flush_size / 11, random_dt);
            if self.curr_flush_size > d {
                self.curr_flush_size - d
            } else {
                self.curr_flush_size
            }
        };
        self.curr_flush_size = min(self.curr_flush_size, self.wbuf_size);
        self.increase_flush_size = !self.increase_flush_size;
        self.curr_data_requested = 0;
    }
}

pub struct BatchState {
    strategy: BatchStrategy,
    flush_size: NonZeroUsize,
    low_flush_interval: Duration,
    high_flush_interval: Duration,
    optimizer: Option<BatchOptimizer>,

    curr_wbuf_content_size: usize,
    last_flush_interval: Duration,
    last_flush_time: Instant,
    flush_timer: Delay,
    stats: Arc<BatchStats>,
}

impl BatchState {
    pub fn new(
        strategy: BatchStrategy,
        flush_size: NonZeroUsize,
        wbuf_size: usize,
        low_flush_interval: Duration,
        high_flush_interval: Duration,
        stats: Arc<BatchStats>,
    ) -> Self {
        let optimizer = if let BatchStrategy::Dynamic = strategy {
            Some(BatchOptimizer::new(flush_size, wbuf_size))
        } else {
            None
        };
        Self {
            strategy,
            flush_size,
            low_flush_interval,
            high_flush_interval,
            optimizer,
            curr_wbuf_content_size: 0,
            last_flush_interval: low_flush_interval,
            last_flush_time: Instant::now(),
            flush_timer: Delay::new(high_flush_interval),
            stats,
        }
    }

    pub fn add_content_size(&mut self, s: usize) {
        if let BatchStrategy::Disabled = self.strategy {
            return;
        }

        if let Some(optimizer) = self.optimizer.as_mut() {
            optimizer.add_data(s);
        }
        self.curr_wbuf_content_size += s;
    }

    pub fn need_flush(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        if let BatchStrategy::Disabled = self.strategy {
            return true;
        }

        if self.curr_wbuf_content_size == 0 {
            return false;
        }

        let flush_size = if let Some(optimizer) = self.optimizer.as_ref() {
            optimizer.get_curr_flush_size()
        } else {
            self.flush_size.get()
        };

        if self.curr_wbuf_content_size >= flush_size {
            return true;
        }
        if now.duration_since(self.last_flush_time) >= self.low_flush_interval {
            return true;
        }

        let mut flush = false;
        match Pin::new(&mut self.flush_timer).poll(cx) {
            Poll::Pending => (),
            Poll::Ready(()) => {
                flush = true;
            }
        }
        if flush {
            self.flush_timer.reset(self.high_flush_interval);
        }
        flush
    }

    pub fn reset(&mut self, now: Instant) {
        if let BatchStrategy::Disabled = self.strategy {
            return;
        }

        let flush_size = if let Some(optimizer) = self.optimizer.as_mut() {
            optimizer.tick(now);
            optimizer.get_curr_flush_size()
        } else {
            self.flush_size.get()
        };

        self.curr_wbuf_content_size = 0;
        if now > self.last_flush_time {
            self.last_flush_interval = now - self.last_flush_time;
        } else {
            self.last_flush_interval = Duration::new(0, 0);
        }
        self.last_flush_time = now;

        self.stats.update(flush_size, self.last_flush_interval);
    }
}

// The following codes are copied from github.com/mre/futures-batch
// with some optimization which might not be for general purpose:
// - Reset timer instead of setting `clock` to None for better performance.
// - Has two different timeout to avoid triggering the real timer too many times.
// - Flush if there's only one item even it's not timed out yet for non-pipeline requests.

pub trait TryChunksTimeoutStreamExt: Stream {
    fn try_chunks_timeout(
        self,
        capacity: NonZeroUsize,
        min_duration: Duration,
        max_duration: Duration,
    ) -> TryChunksTimeout<Self>
    where
        Self: Sized,
    {
        TryChunksTimeout::new(self, capacity, min_duration, max_duration)
    }
}
impl<T: ?Sized> TryChunksTimeoutStreamExt for T where T: Stream {}

#[pin_project]
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct TryChunksTimeout<St: Stream> {
    #[pin]
    stream: Fuse<St>,
    items: Vec<St::Item>,
    cap: NonZeroUsize,
    // https://github.com/rust-lang-nursery/futures-rs/issues/1475
    #[pin]
    clock: Delay,
    min_duration: coarsetime::Duration,
    max_duration: Duration,
    last_flush_time: coarsetime::Instant,
    flush_size: usize, // Make it to be able to learn from the real pipeline number.
}

impl<St: Stream> TryChunksTimeout<St>
where
    St: Stream,
{
    pub fn new(
        stream: St,
        capacity: NonZeroUsize,
        min_duration: Duration,
        max_duration: Duration,
    ) -> TryChunksTimeout<St> {
        TryChunksTimeout {
            stream: stream.fuse(),
            items: Vec::with_capacity(capacity.get()),
            cap: capacity,
            clock: Delay::new(max_duration),
            min_duration: coarsetime::Duration::from(min_duration),
            max_duration,
            last_flush_time: coarsetime::Instant::now(),
            flush_size: capacity.get(),
        }
    }

    fn take(mut self: Pin<&mut Self>) -> Vec<St::Item> {
        let this = self.as_mut().project();
        let cap = this.cap.get();
        mem::replace(this.items, Vec::with_capacity(cap))
    }

    fn flush(mut self: Pin<&mut Self>, now: coarsetime::Instant) -> Poll<Option<Vec<St::Item>>> {
        let this = self.as_mut().project();
        *this.last_flush_time = now;
        *this.flush_size = this.items.len();
        Poll::Ready(Some(self.take()))
    }
}

impl<St: Stream> Stream for TryChunksTimeout<St> {
    type Item = Vec<St::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let start_empty = self.as_mut().items.is_empty();
        loop {
            match self.as_mut().project().stream.poll_next(cx) {
                Poll::Ready(item) => match item {
                    // Push the item into the buffer and check whether it is full.
                    // If so, replace our buffer with a new and empty one and return
                    // the full one.
                    Some(item) => {
                        let this = self.as_mut().project();
                        this.items.push(item);
                        if this.items.len() >= this.cap.get() {
                            return self.flush(coarsetime::Instant::recent());
                        } else {
                            // Continue the loop
                            continue;
                        }
                    }

                    // Since the underlying stream ran out of values, return what we
                    // have buffered, if we have anything.
                    None => {
                        let this = self.as_mut().project();
                        let last = if this.items.is_empty() {
                            None
                        } else {
                            let full_buf = mem::replace(this.items, Vec::new());
                            Some(full_buf)
                        };

                        return Poll::Ready(last);
                    }
                },
                // Don't return here, as we need to need check the clock.
                Poll::Pending => {}
            }

            if self.items.is_empty() {
                return Poll::Pending;
            }

            // Learn from the last flush size.
            if self.items.len() >= self.flush_size {
                return self.flush(coarsetime::Instant::recent());
            }

            let now = coarsetime::Instant::now();
            if now > self.last_flush_time
                && now.duration_since(self.last_flush_time) >= self.min_duration
            {
                return self.flush(now);
            }

            if start_empty {
                let mut this = self.as_mut().project();
                this.clock.reset(*this.max_duration);
                // This return might cause the timer not to be able to wake up.
                // return Poll::Pending;
            }

            match self.as_mut().project().clock.poll(cx) {
                Poll::Ready(()) => {
                    return self.flush(coarsetime::Instant::recent());
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.items.is_empty() { 0 } else { 1 };
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(chunk_len);
        let upper = match upper {
            Some(x) => x.checked_add(chunk_len),
            None => None,
        };
        (lower, upper)
    }
}

impl<St: FusedStream> FusedStream for TryChunksTimeout<St> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() & self.items.is_empty()
    }
}

// Forwarding impl of Sink from the underlying stream
#[cfg(feature = "sink")]
impl<S, Item> Sink<Item> for TryChunksTimeout<S>
where
    S: Stream + Sink<Item>,
{
    type Error = S::Error;

    delegate_sink!(stream, Item);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt};
    use std::iter;
    use std::time::Duration;

    #[tokio::test]
    async fn messages_pass_through() {
        let results = stream::iter(iter::once(5))
            .try_chunks_timeout(
                NonZeroUsize::new(5).unwrap(),
                Duration::new(1, 0),
                Duration::new(1, 0),
            )
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![5]], results.await);
    }

    #[tokio::test]
    async fn message_chunks() {
        let iter = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
        let stream = stream::iter(iter);

        let chunk_stream = TryChunksTimeout::new(
            stream,
            NonZeroUsize::new(5).unwrap(),
            Duration::new(1, 0),
            Duration::new(1, 0),
        );
        assert_eq!(
            vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9]],
            chunk_stream.collect::<Vec<_>>().await
        );
    }

    #[tokio::test]
    async fn message_early_exit() {
        let iter = vec![1, 2, 3, 4].into_iter();
        let stream = stream::iter(iter);

        let chunk_stream = TryChunksTimeout::new(
            stream,
            NonZeroUsize::new(5).unwrap(),
            Duration::new(1, 0),
            Duration::new(1, 0),
        );
        assert_eq!(
            vec![vec![1, 2, 3, 4]],
            chunk_stream.collect::<Vec<_>>().await
        );
    }
}

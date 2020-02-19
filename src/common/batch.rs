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
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

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
        support_non_pipeline: bool,
    ) -> TryChunksTimeout<Self>
    where
        Self: Sized,
    {
        TryChunksTimeout::new(self, capacity, min_duration, max_duration, support_non_pipeline)
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
    min_duration: Duration,
    max_duration: Duration,
    last_flush_time: Instant,
    support_non_pipeline: bool,
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
        support_non_pipeline: bool,
    ) -> TryChunksTimeout<St> {
        TryChunksTimeout {
            stream: stream.fuse(),
            items: Vec::with_capacity(capacity.get()),
            cap: capacity,
            clock: Delay::new(max_duration),
            min_duration,
            max_duration,
            last_flush_time: Instant::now(),
            support_non_pipeline,
        }
    }

    fn take(mut self: Pin<&mut Self>) -> Vec<St::Item> {
        let this = self.as_mut().project();
        let cap = this.cap.get();
        mem::replace(this.items, Vec::with_capacity(cap))
    }

    fn flush(mut self: Pin<&mut Self>, now: Instant) -> Poll<Option<Vec<St::Item>>> {
        *self.as_mut().project().last_flush_time = now;
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
                            return self.flush(Instant::now());
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

            if self.support_non_pipeline && self.items.len() == 1 {
                // non-pipeline request
                return self.flush(Instant::now());
            }

            let now = Instant::now();
            if now > self.last_flush_time
                && now.duration_since(self.last_flush_time) >= self.min_duration
            {
                return self.flush(now);
            }

            if start_empty {
                let mut this = self.as_mut().project();
                this.clock.reset(*this.max_duration);
                return Poll::Pending;
            }

            match self.as_mut().project().clock.poll(cx) {
                Poll::Ready(()) => {
                    return self.flush(Instant::now());
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
                false,
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
            false,
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
            false,
        );
        assert_eq!(
            vec![vec![1, 2, 3, 4]],
            chunk_stream.collect::<Vec<_>>().await
        );
    }

    #[tokio::test]
    async fn message_timeout() {
        // TODO: Add tests
    }
}

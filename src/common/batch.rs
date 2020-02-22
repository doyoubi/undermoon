use coarsetime;
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

// The following codes are copied from github.com/mre/futures-batch
// with some optimization which might not be able to fit into general purpose:
// - Reset timer instead of setting `clock` to None for better performance.
// - Has two different timeout to avoid triggering the real timer too many times.
// - Flush also depends on the last flushed size to support non-pipeline requests.
// - Read data from a shared Vec instead of the returned value from `poll_next` to reduce heap allocation.

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
    items: Arc<Mutex<Vec<St::Item>>>,
    items_len: usize,
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
            items: Arc::new(Mutex::new(Vec::with_capacity(capacity.get()))),
            items_len: 0,
            cap: capacity,
            clock: Delay::new(max_duration),
            min_duration: coarsetime::Duration::from(min_duration),
            max_duration,
            last_flush_time: coarsetime::Instant::now(),
            flush_size: capacity.get(),
        }
    }

    pub fn get_output_buf(&self) -> Arc<Mutex<Vec<St::Item>>> {
        self.items.clone()
    }

    fn flush(
        last_flush_time: &mut coarsetime::Instant,
        flush_size: &mut usize,
        items_len: &mut usize,
        now: coarsetime::Instant,
    ) -> Poll<Option<()>> {
        *last_flush_time = now;
        *flush_size = *items_len;
        *items_len = 0;
        Poll::Ready(Some(()))
    }
}

impl<St: Stream> Stream for TryChunksTimeout<St> {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let items_lock = this.items;
        let mut stream = this.stream;
        let flush_size = this.flush_size;
        let last_flush_time = this.last_flush_time;
        let items_len = this.items_len;
        let min_duration = *this.min_duration;
        let max_duration = *this.max_duration;
        let mut clock = this.clock;

        let mut items = match items_lock.lock() {
            Ok(items) => items,
            _err => {
                error!("TryChunksTimeout: failed to get lock");
                return Poll::Ready(None);
            }
        };
        let start_empty = items.is_empty();

        loop {
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(item) => match item {
                    Some(item) => {
                        *items_len += 1;
                        items.push(item);
                        if items.len() >= this.cap.get() {
                            return Self::flush(
                                last_flush_time,
                                flush_size,
                                items_len,
                                coarsetime::Instant::recent(),
                            );
                        } else {
                            // Continue the loop
                            continue;
                        }
                    }

                    // Since the underlying stream ran out of values, return what we
                    // have buffered, if we have anything.
                    None => {
                        let last = if items.is_empty() { None } else { Some(()) };

                        return Poll::Ready(last);
                    }
                },
                // Don't return here, as we need to need check the clock.
                Poll::Pending => {}
            }

            if items.is_empty() {
                return Poll::Pending;
            }

            // Learn from the last flush size.
            if items.len() >= *flush_size {
                return Self::flush(
                    last_flush_time,
                    flush_size,
                    items_len,
                    coarsetime::Instant::recent(),
                );
            }

            let now = coarsetime::Instant::now();
            if now > *last_flush_time && now.duration_since(*last_flush_time) >= min_duration {
                return Self::flush(last_flush_time, flush_size, items_len, now);
            }

            if start_empty {
                clock.reset(max_duration);
                return Poll::Pending;
            }

            match clock.poll(cx) {
                Poll::Ready(()) => {
                    return Self::flush(
                        last_flush_time,
                        flush_size,
                        items_len,
                        coarsetime::Instant::recent(),
                    );
                }
                Poll::Pending => {}
            }

            return Poll::Pending;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunk_len = if self.items_len == 0 { 0 } else { 1 };
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
        self.stream.is_terminated() & (self.items_len == 0)
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
        let mut chunk_stream = stream::iter(iter::once(5)).try_chunks_timeout(
            NonZeroUsize::new(5).unwrap(),
            Duration::new(1, 0),
            Duration::new(1, 0),
        );
        let output = chunk_stream.get_output_buf();
        chunk_stream.next().await;
        assert_eq!(vec![5], output.lock().unwrap().clone());
    }

    #[tokio::test]
    async fn message_chunks() {
        let iter = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
        let stream = stream::iter(iter);

        let mut chunk_stream = TryChunksTimeout::new(
            stream,
            NonZeroUsize::new(5).unwrap(),
            Duration::new(1, 0),
            Duration::new(1, 0),
        );
        let output = chunk_stream.get_output_buf();
        chunk_stream.next().await;
        let tasks: Vec<_> = output.lock().unwrap().drain(..).collect();
        assert_eq!(vec![0, 1, 2, 3, 4], tasks);
        chunk_stream.next().await;
        let tasks: Vec<_> = output.lock().unwrap().drain(..).collect();
        assert_eq!(vec![5, 6, 7, 8, 9], tasks)
    }

    #[tokio::test]
    async fn message_early_exit() {
        let iter = vec![1, 2, 3, 4].into_iter();
        let stream = stream::iter(iter);

        let mut chunk_stream = TryChunksTimeout::new(
            stream,
            NonZeroUsize::new(5).unwrap(),
            Duration::new(1, 0),
            Duration::new(1, 0),
        );
        let output = chunk_stream.get_output_buf();
        chunk_stream.next().await;
        assert_eq!(vec![1, 2, 3, 4], output.lock().unwrap().clone(),);
    }
}

use core::mem;
use futures::stream::{Fuse, FusedStream};
use futures::task::{Context, Poll};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use std::num::NonZeroUsize;
use std::pin::Pin;

// This is copied and modified from the `Chunk` future combinator from futures-rs 0.3

pub trait TryChunksStreamExt: Stream {
    fn try_chunks(self, capacity: NonZeroUsize) -> TryChunks<Self>
    where
        Self: Sized,
    {
        TryChunks::new(self, capacity)
    }
}

impl<T: ?Sized> TryChunksStreamExt for T where T: Stream {}

#[pin_project]
#[derive(Debug)]
pub struct TryChunks<St: Stream> {
    #[pin]
    stream: Fuse<St>,
    items: Vec<St::Item>,
    cap: NonZeroUsize, // https://github.com/rust-lang/futures-rs/issues/1475
}

impl<St: Stream> TryChunks<St>
where
    St: Stream,
{
    pub fn new(stream: St, cap: NonZeroUsize) -> Self {
        Self {
            stream: stream.fuse(),
            items: Vec::with_capacity(cap.get()),
            cap,
        }
    }

    fn take(mut self: Pin<&mut Self>) -> Vec<St::Item> {
        let this = self.as_mut().project();
        let cap = this.cap.get();
        mem::replace(this.items, Vec::with_capacity(cap))
    }
}

impl<St: Stream> Stream for TryChunks<St> {
    type Item = Vec<St::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let item_opt = match self.as_mut().project().stream.poll_next(cx) {
                Poll::Ready(item_opt) => item_opt,
                Poll::Pending => {
                    return if self.items.is_empty() {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(self.take()))
                    };
                }
            };
            match item_opt {
                // Push the item into the buffer and check whether it is full.
                // If so, replace our buffer with a new and empty one and return
                // the full one.
                Some(item) => {
                    self.as_mut().project().items.push(item);
                    if self.items.len() >= self.cap.get() {
                        return Poll::Ready(Some(self.take()));
                    }
                }

                // Since the underlying stream ran out of values, return what we
                // have buffered, if we have anything.
                None => {
                    let last = if self.items.is_empty() {
                        None
                    } else {
                        let full_buf = mem::replace(self.as_mut().project().items, Vec::new());
                        Some(full_buf)
                    };

                    return Poll::Ready(last);
                }
            }
        }
    }
}

impl<St: FusedStream> FusedStream for TryChunks<St> {
    fn is_terminated(&self) -> bool {
        self.stream.is_terminated() && self.items.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt};

    #[tokio::test]
    async fn test_one_item() {
        let iter = vec![0].into_iter();
        let mut stream = stream::iter(iter).try_chunks(NonZeroUsize::new(5).unwrap());
        let item = stream.next().await.unwrap();
        assert_eq!(item, vec![0]);
    }

    #[tokio::test]
    async fn test_chunks() {
        let iter = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
        let mut stream = stream::iter(iter).try_chunks(NonZeroUsize::new(3).unwrap());
        let item = stream.next().await.unwrap();
        assert_eq!(item, vec![0, 1, 2]);
    }
}

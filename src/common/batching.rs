use std::io;
use std::mem;
use std::prelude::v1::*;
use std::time::{Duration, Instant};

use futures::stream::{Fuse, Stream};
use futures::{Async, Future, Poll};
use futures_timer::Delay;

// This file is copied from `tokio-batch` with an additional timeout
// to workaround performance problem caused by conditional variables
// in tokio 0.1.

// TODO: Remove this and use tokio-batch directly after upgrading to new future.

/// An adaptor that chunks up elements in a vector.
///
/// This adaptor will buffer up a list of items in the stream and pass on the
/// vector used for buffering when a specified capacity has been reached
/// or a predefined timeout was triggered.
///
/// This was taken and adjusted from
/// https://github.com/alexcrichton/futures-rs/blob/master/src/stream/chunks.rs
/// and moved into a separate crate for usability.
#[must_use = "streams do nothing unless polled"]
pub struct Chunks<S>
where
    S: Stream,
{
    clock: Option<Delay>,
    min_duration: Duration,
    max_duration: Duration,
    last_flush_time: Instant,
    items: Vec<S::Item>,
    err: Option<Error<S::Error>>,
    stream: Fuse<S>,
}

/// Error returned by `Chunks`.
#[derive(Debug)]
pub struct Error<T>(Kind<T>);

/// Chunks error variants
#[derive(Debug)]
enum Kind<T> {
    /// Inner value returned an error
    Inner(T),

    /// Timer returned an error.
    Timer(io::Error),
}

impl<S> Chunks<S>
where
    S: Stream,
{
    pub fn new(s: S, capacity: usize, min_duration: Duration, max_duration: Duration) -> Chunks<S> {
        assert!(capacity > 0);

        Chunks {
            clock: None,
            min_duration,
            max_duration,
            last_flush_time: Instant::now(),
            items: Vec::with_capacity(capacity),
            err: None,
            stream: s.fuse(),
        }
    }

    fn take(&mut self) -> Vec<S::Item> {
        let cap = self.items.capacity();
        mem::replace(&mut self.items, Vec::with_capacity(cap))
    }

    fn flush(&mut self, now: Instant) -> Poll<Option<Vec<S::Item>>, S::Error> {
        self.clock = None;
        self.last_flush_time = now;
        Ok(Some(self.take()).into())
    }
}

impl<S> Stream for Chunks<S>
where
    S: Stream,
{
    type Item = Vec<<S as Stream>::Item>;
    type Error = Error<S::Error>;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(e) = self.err.take() {
            return Err(e);
        }

        let cap = self.items.capacity();
        loop {
            match self.stream.poll() {
                Ok(Async::NotReady) => {}

                // Push the item into the buffer and check whether it is full.
                // If so, replace our buffer with a new and empty one and return
                // the full one.
                Ok(Async::Ready(Some(item))) => {
                    if self.items.is_empty() {
                        self.clock = Some(Delay::new(self.max_duration));
                    }
                    self.items.push(item);
                    if self.items.len() >= cap {
                        return self
                            .flush(Instant::now())
                            .map_err(|e| Error(Kind::Inner(e)));
                    } else {
                        continue;
                    }
                }

                // Since the underlying stream ran out of values, return what we
                // have buffered, if we have anything.
                Ok(Async::Ready(None)) => {
                    return if !self.items.is_empty() {
                        let full_buf = mem::replace(&mut self.items, Vec::new());
                        Ok(Some(full_buf).into())
                    } else {
                        Ok(Async::Ready(None))
                    };
                }

                // If we've got buffered items be sure to return them first,
                // we'll defer our error for later.
                Err(e) => {
                    if self.items.is_empty() {
                        return Err(Error(Kind::Inner(e)));
                    } else {
                        self.err = Some(Error(Kind::Inner(e)));
                        return self
                            .flush(Instant::now())
                            .map_err(|e| Error(Kind::Inner(e)));
                    }
                }
            }

            if self.items.is_empty() {
                return Ok(Async::NotReady);
            }

            let now = Instant::now();
            if now > self.last_flush_time
                && now.duration_since(self.last_flush_time) >= self.min_duration
            {
                return self.flush(now).map_err(|e| Error(Kind::Inner(e)));
            }

            match self.clock.poll() {
                Ok(Async::Ready(Some(()))) => {
                    return self
                        .flush(Instant::now())
                        .map_err(|e| Error(Kind::Inner(e)));
                }
                Ok(Async::Ready(None)) => {
                    assert!(self.items.is_empty(), "no clock but there are items");
                }
                Ok(Async::NotReady) => {}
                Err(e) => {
                    if self.items.is_empty() {
                        return Err(Error(Kind::Timer(e)));
                    } else {
                        self.err = Some(Error(Kind::Timer(e)));
                        return self
                            .flush(Instant::now())
                            .map_err(|e| Error(Kind::Inner(e)));
                    }
                }
            }

            return Ok(Async::NotReady);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::io;
    use std::iter;
    use std::time::{Duration, Instant};

    #[test]
    fn messages_pass_through() {
        let iter = iter::once(5);
        let stream = stream::iter_ok::<_, io::Error>(iter);

        let chunk_stream = Chunks::new(stream, 5, Duration::new(10, 0), Duration::new(10, 0));

        let v = chunk_stream.collect();
        tokio::run(v.then(|res| {
            match res {
                Err(_) => assert!(false),
                Ok(v) => assert_eq!(vec![vec![5]], v),
            };
            Ok(())
        }));
    }

    #[test]
    fn message_chunks() {
        let iter = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_iter();
        let stream = stream::iter_ok::<_, io::Error>(iter);

        let chunk_stream = Chunks::new(stream, 5, Duration::new(10, 0), Duration::new(10, 0));

        let v = chunk_stream.collect();
        tokio::run(v.then(|res| {
            match res {
                Err(_) => assert!(false),
                Ok(v) => assert_eq!(vec![vec![0, 1, 2, 3, 4], vec![5, 6, 7, 8, 9]], v),
            };
            Ok(())
        }));
    }

    #[test]
    fn message_early_exit() {
        let iter = vec![1, 2, 3, 4].into_iter();
        let stream = stream::iter_ok::<_, io::Error>(iter);

        let chunk_stream = Chunks::new(stream, 5, Duration::new(100, 0), Duration::new(10, 0));

        let v = chunk_stream.collect();
        tokio::run(v.then(|res| {
            match res {
                Err(_) => assert!(false),
                Ok(v) => assert_eq!(vec![vec![1, 2, 3, 4]], v),
            };
            Ok(())
        }));
    }

    #[test]
    fn message_timeout() {
        let iter = vec![1, 2, 3, 4].into_iter();
        let stream0 = stream::iter_ok::<_, io::Error>(iter);

        let iter = vec![5].into_iter();
        let stream1 = stream::iter_ok::<_, io::Error>(iter).and_then(|n| {
            Delay::new(Duration::from_millis(300))
                .and_then(move |_| Ok(n))
                .map_err(|e| io::Error::new(io::ErrorKind::TimedOut, e))
        });

        let iter = vec![6, 7, 8].into_iter();
        let stream2 = stream::iter_ok::<_, io::Error>(iter);

        let stream = stream0.chain(stream1).chain(stream2);
        let chunk_stream = Chunks::new(
            stream,
            5,
            Duration::from_millis(100),
            Duration::from_millis(100),
        );

        let now = Instant::now();
        let min_times = [Duration::from_millis(80), Duration::from_millis(150)];
        let max_times = [Duration::from_millis(280), Duration::from_millis(350)];
        let results = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]];
        let mut i = 0;

        let v = chunk_stream
            .map(move |s| {
                let now2 = Instant::now();
                println!("{:?}", now2 - now);
                assert!((now2 - now) < max_times[i]);
                assert!((now2 - now) > min_times[i]);
                i += 1;
                s
            })
            .collect();

        tokio::run(v.then(move |res| {
            match res {
                Err(_) => assert!(false),
                Ok(v) => assert_eq!(v, results),
            };
            Ok(())
        }));
    }
}

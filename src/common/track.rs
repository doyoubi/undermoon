use chrono::{DateTime, Local};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::task::{Context, Poll};
use futures::Future;
use pin_project::{pin_project, pinned_drop};
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct FutureDescription {
    future_id: u64,
    desc: String,
    start_time: DateTime<Local>,
}

impl FutureDescription {
    pub fn get_start_time(&self) -> DateTime<Local> {
        self.start_time
    }
}

impl fmt::Display for FutureDescription {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.future_id,
            self.start_time.format("%Y-%m-%d %H:%M:%S"),
            self.desc
        )
    }
}

pub struct TrackedFutureRegistry {
    curr_future_id: AtomicU64,
    future_map: DashMap<u64, Arc<FutureDescription>>,
}

impl Default for TrackedFutureRegistry {
    fn default() -> Self {
        Self {
            curr_future_id: AtomicU64::new(0),
            future_map: DashMap::new(),
        }
    }
}

impl TrackedFutureRegistry {
    pub fn wrap<F: Future>(
        registry: Arc<TrackedFutureRegistry>,
        fut: F,
        desc: String,
    ) -> TrackedFuture<F> {
        TrackedFuture::new(fut, registry, desc)
    }

    pub fn register(&self, desc: String) -> u64 {
        let future_id = self.curr_future_id.fetch_add(1, Ordering::Relaxed);
        let future_desc = Arc::new(FutureDescription {
            future_id,
            desc,
            start_time: Local::now(),
        });
        match self.future_map.entry(future_id) {
            Entry::Occupied(entry) => {
                error!(
                    "TrackedFutureRegistry found duplicated future id: {}, will replace it",
                    *entry.get()
                );
                entry.replace_entry(future_desc);
            }
            Entry::Vacant(entry) => {
                entry.insert(future_desc);
            }
        }
        future_id
    }

    pub fn deregister(&self, future_id: u64) {
        self.future_map.remove(&future_id);
    }

    pub fn get_all_futures(&self) -> Vec<Arc<FutureDescription>> {
        self.future_map
            .iter()
            .map(|item| item.value().clone())
            .collect()
    }
}

#[pin_project(PinnedDrop)]
pub struct TrackedFuture<F: Future> {
    #[pin]
    inner: F,
    registry: Arc<TrackedFutureRegistry>,
    future_id: u64,
}

impl<F: Future> TrackedFuture<F> {
    pub fn new(inner: F, registry: Arc<TrackedFutureRegistry>, desc: String) -> Self {
        let future_id = registry.register(desc);
        Self {
            inner,
            registry,
            future_id,
        }
    }
}

impl<F: Future> Future for TrackedFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(output) => {
                this.registry.deregister(*this.future_id);
                Poll::Ready(output)
            }
        }
    }
}

#[pinned_drop]
impl<F: Future> PinnedDrop for TrackedFuture<F> {
    fn drop(mut self: Pin<&mut Self>) {
        let this = self.project();
        let future_id = *this.future_id;
        this.registry.deregister(future_id);
    }
}

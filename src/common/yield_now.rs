use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// Copied from tokio::task::yield_now so that this future could be `Pin`.
pub struct YieldNow {
    yielded: bool,
}

impl Default for YieldNow {
    fn default() -> Self {
        Self { yielded: false }
    }
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            return Poll::Ready(());
        }

        self.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

use futures::channel::oneshot;
use futures::{Future};
use futures::task::{Context, Poll};
use std::pin::Pin;
use pin_project::{pin_project, pinned_drop};

pub fn new_future_group<FA: Future, FB: Future>(
    future1: FA,
    future2: FB,
) -> (FutureGroupHandle<FA>, FutureGroupHandle<FB>) {
    let (s1, r1) = oneshot::channel();
    let (s2, r2) = oneshot::channel();
    let handle1 = FutureGroupHandle {
        inner: future1,
        signal_sender: Some(s1),
        signal_receiver: r2,
    };
    let handle2 = FutureGroupHandle {
        inner: future2,
        signal_sender: Some(s2),
        signal_receiver: r1,
    };
    (handle1, handle2)
}

#[pin_project(PinnedDrop)]
pub struct FutureGroupHandle<F: Future> {
    #[pin]
    inner: F,
    #[pin]
    signal_receiver: oneshot::Receiver<()>,
    signal_sender: Option<oneshot::Sender<()>>,
}

impl<F: Future> Future for FutureGroupHandle<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Pending => (),
            Poll::Ready(_) => {
                if let Some(sender) = this.signal_sender.take() {
                    if let Err(()) = sender.send(()) {
                        debug!("failed to signal");
                    }
                }
                return Poll::Ready(())
            },
        }

        this.signal_receiver.poll(cx).map(|_| ())
    }
}

#[pinned_drop]
impl<F: Future> PinnedDrop for FutureGroupHandle<F> {
    fn drop(self: Pin<&mut Self>) {
        self.signal_sender
            .take()
            .and_then(|mut sender| sender.send(()).ok())
            .unwrap_or_else(|| debug!("FutureGroupHandle already closed"))
    }
}

pub fn new_auto_drop_future<F: Future>(future: F) -> (FutureAutoStop<F>, FutureAutoStopHandle) {
    let (s, r) = oneshot::channel();
    let handle = FutureAutoStopHandle {
        signal_sender: Some(s),
    };
    let fut = FutureAutoStop {
        inner: future,
        signal_receiver: r,
    };
    (fut, handle)
}

#[pin_project]
pub struct FutureAutoStop<F: Future> {
    #[pin]
    inner: F,
    #[pin]
    signal_receiver: oneshot::Receiver<()>,
}

pub struct FutureAutoStopHandle {
    signal_sender: Option<oneshot::Sender<()>>,
}

impl Drop for FutureAutoStopHandle {
    fn drop(&mut self) {
        self.signal_sender
            .take()
            .and_then(|mut sender| sender.send(()).ok())
            .unwrap_or_else(|| debug!("FutureAutoStopHandle already closed"))
    }
}

impl<F: Future> Future for FutureAutoStop<F> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Pending => (),
            Poll::Ready(_) => return Poll::Ready(()),
        }

        this.signal_receiver.poll(cx).map(|_| ())
    }
}

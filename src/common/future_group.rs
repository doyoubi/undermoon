use futures01::sync::oneshot;
use futures01::{Async, Future, Poll};

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

pub struct FutureGroupHandle<F: Future> {
    inner: F,
    signal_sender: Option<oneshot::Sender<()>>,
    signal_receiver: oneshot::Receiver<()>,
}

impl<F: Future> Future for FutureGroupHandle<F> {
    type Item = ();
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = &mut self.inner;
        let signal_sender = &mut self.signal_sender;
        let signal_receiver = &mut self.signal_receiver;

        match inner.poll() {
            Ok(Async::NotReady) => (),
            Ok(Async::Ready(_)) => {
                if let Some(sender) = signal_sender.take() {
                    if let Err(()) = sender.send(()) {
                        debug!("failed to signal");
                    }
                }
                return Ok(Async::Ready(()));
            }
            Err(e) => {
                if let Some(sender) = signal_sender.take() {
                    if let Err(()) = sender.send(()) {
                        debug!("failed to signal");
                    }
                }
                return Err(e);
            }
        };
        match signal_receiver.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(())) => Ok(Async::Ready(())),
            Err(_e) => Ok(Async::Ready(())),
        }
    }
}

impl<F: Future> Drop for FutureGroupHandle<F> {
    fn drop(&mut self) {
        self.signal_sender
            .take()
            .and_then(|sender| sender.send(()).ok())
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

pub struct FutureAutoStop<F: Future> {
    inner: F,
    signal_receiver: oneshot::Receiver<()>,
}

pub struct FutureAutoStopHandle {
    signal_sender: Option<oneshot::Sender<()>>,
}

impl Drop for FutureAutoStopHandle {
    fn drop(&mut self) {
        self.signal_sender
            .take()
            .and_then(|sender| sender.send(()).ok())
            .unwrap_or_else(|| debug!("FutureAutoStopHandle already closed"))
    }
}

impl<F: Future> Future for FutureAutoStop<F> {
    type Item = ();
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let inner = &mut self.inner;
        let signal_receiver = &mut self.signal_receiver;

        match inner.poll() {
            Ok(Async::NotReady) => (),
            Ok(Async::Ready(_)) => return Ok(Async::Ready(())),
            Err(e) => return Err(e),
        }
        match signal_receiver.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            _ => Ok(Async::Ready(())),
        }
    }
}

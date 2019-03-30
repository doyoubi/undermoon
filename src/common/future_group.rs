use futures::{Future, Poll, Async};
use futures::sync::oneshot;

pub fn new_future_group<FA: Future, FB: Future>(future1: FA, future2: FB) -> (FutureGroupHandle<FA>, FutureGroupHandle<FB>) {
    let (s1, r1) = oneshot::channel();
    let (s2, r2) = oneshot::channel();
    let handle1 = FutureGroupHandle{ inner: future1, signal_sender: Some(s1), signal_receiver: r2 };
    let handle2 = FutureGroupHandle{ inner: future2, signal_sender: Some(s2), signal_receiver: r1 };
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
                return Ok(Async::Ready(()))
            },
            Err(e) => {
                if let Some(sender) = signal_sender.take() {
                    if let Err(()) = sender.send(()) {
                        debug!("failed to signal");
                    }
                }
                return Err(e)
            },
        };
        match signal_receiver.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(())) => Ok(Async::Ready(())),
            Err(_e) => Ok(Async::Ready(())),
        }
    }
}

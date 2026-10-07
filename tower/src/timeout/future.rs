//! Future types

use super::error::Elapsed;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::Sleep;

pin_project! {
    /// [`Timeout`] response future
    ///
    /// The `S` type parameter is the deadline ("clock") future that is polled
    /// alongside the inner response future. It defaults to
    /// [`tokio::time::Sleep`], which is what [`Timeout`](crate::timeout::Timeout)
    /// and [`TimeoutLayer`](crate::timeout::TimeoutLayer) use, so the default
    /// Tokio timing path is unchanged.
    ///
    /// The generic parameter also lets tests inject a manually controlled
    /// deadline (see [`ResponseFuture::new`]): within a single `poll` the inner
    /// response is always polled first, so a response that becomes ready in the
    /// same turn as the deadline wins, and a deadline that fires while the
    /// response is still pending stops polling the inner future and returns the
    /// [`Elapsed`] error; the inner future is then dropped with the response
    /// future.
    #[derive(Debug)]
    pub struct ResponseFuture<T, S = Sleep> {
        #[pin]
        response: T,
        #[pin]
        sleep: S,
    }
}

impl<T, S> ResponseFuture<T, S> {
    /// Wrap an inner response future together with a deadline future.
    ///
    /// When `sleep` completes before `response`, polling the returned future
    /// produces an [`Elapsed`] error without polling `response` again, and
    /// `response` is dropped when the response future is dropped.
    pub(crate) fn new(response: T, sleep: S) -> Self {
        ResponseFuture { response, sleep }
    }
}

impl<F, S, T, E> Future for ResponseFuture<F, S>
where
    F: Future<Output = Result<T, E>>,
    S: Future<Output = ()>,
    E: Into<crate::BoxError>,
{
    type Output = Result<T, crate::BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        // First, try polling the future
        match this.response.poll(cx) {
            Poll::Ready(v) => return Poll::Ready(v.map_err(Into::into)),
            Poll::Pending => {}
        }

        // Now check the sleep
        match this.sleep.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(Elapsed(()).into())),
        }
    }
}

#[cfg(test)]
mod injected_clock_tests {
    use super::super::error::Elapsed;
    use super::*;
    use std::cell::Cell;
    use std::fmt;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const STATE_OK: u8 = 0;
    const STATE_ERR: u8 = 1;
    const STATE_PENDING: u8 = 2;

    #[derive(Debug)]
    struct TestErr(&'static str);
    impl fmt::Display for TestErr {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
    impl std::error::Error for TestErr {}

    /// A controllable "clock": flip it on and every deadline derived from it
    /// reports that the timeout has fired. No timers, no sleeps, no runtime.
    #[derive(Clone)]
    struct ManualClock {
        fired: Rc<Cell<bool>>,
    }

    impl ManualClock {
        fn new() -> Self {
            ManualClock {
                fired: Rc::new(Cell::new(false)),
            }
        }
        fn fire(&self) {
            self.fired.set(true);
        }
        fn deadline(&self) -> ManualDeadline {
            ManualDeadline {
                fired: self.fired.clone(),
            }
        }
    }

    struct ManualDeadline {
        fired: Rc<Cell<bool>>,
    }

    impl Future for ManualDeadline {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            if self.fired.get() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }

    /// Inner response future whose readiness is chosen up front (or mutated
    /// between polls), counting how many times it gets dropped.
    struct Tracked {
        state: Cell<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for Tracked {
        type Output = Result<(), TestErr>;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.state.get() {
                STATE_OK => Poll::Ready(Ok(())),
                STATE_ERR => Poll::Ready(Err(TestErr("boom"))),
                _ => Poll::Pending,
            }
        }
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll_once<F: Future>(fut: Pin<&mut F>) -> Poll<F::Output> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        fut.poll(&mut cx)
    }

    // Inner response and the injected deadline are ready in the *same* turn:
    // the response must win and the inner future is not dropped early.
    #[test]
    fn response_completed_same_turn_beats_fired_deadline() {
        let drops = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::new();
        clock.fire();

        let inner = Tracked {
            state: Cell::new(STATE_OK),
            drops: drops.clone(),
        };
        let mut fut = Box::pin(ResponseFuture::new(inner, clock.deadline()));

        match poll_once(fut.as_mut()) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected inner Ok result, got {:?}", other),
        }
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "the completed inner future must not be dropped by the timeout path"
        );

        drop(fut);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the completed inner future is dropped normally with the response future"
        );
    }

    // Deadline fires while the inner future is still pending: the Elapsed
    // timeout error is returned, and dropping the response future then drops
    // the inner future exactly once.
    #[test]
    fn fired_deadline_drops_inner_and_returns_elapsed() {
        let drops = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::new();
        clock.fire();

        let inner = Tracked {
            state: Cell::new(STATE_PENDING),
            drops: drops.clone(),
        };
        let mut fut = Box::pin(ResponseFuture::new(inner, clock.deadline()));

        match poll_once(fut.as_mut()) {
            Poll::Ready(Err(e)) => {
                assert!(
                    e.is::<Elapsed>(),
                    "expected Elapsed timeout error, got {:?}",
                    e
                );
                assert_eq!(e.to_string(), "request timed out");
            }
            other => panic!("expected timeout error, got {:?}", other),
        }
        // The timeout result has been produced, but the inner future is only
        // released once the response future itself is dropped (at the call
        // site, e.g. when the response/error is handled).
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(fut);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "the pending inner future must be dropped with the timed-out response future"
        );
    }

    // Deadline fires on a later poll turn: the first turn stays pending, then
    // firing the clock reports the timeout and the inner future is dropped
    // exactly once.
    #[test]
    fn deadline_firing_on_later_turn_drops_inner() {
        let drops = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::new();

        let inner = Tracked {
            state: Cell::new(STATE_PENDING),
            drops: drops.clone(),
        };
        let mut fut = Box::pin(ResponseFuture::new(inner, clock.deadline()));

        assert!(
            poll_once(fut.as_mut()).is_pending(),
            "neither response nor deadline ready -> Pending"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        clock.fire();

        match poll_once(fut.as_mut()) {
            Poll::Ready(Err(e)) => assert!(e.is::<Elapsed>()),
            other => panic!("expected timeout error, got {:?}", other),
        }
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(fut);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // Inner error is ready in the same turn the deadline fires: the error is
    // passed through untouched instead of being replaced by a timeout.
    #[test]
    fn ready_error_passes_through_even_when_deadline_fired() {
        let drops = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::new();
        clock.fire();

        let inner = Tracked {
            state: Cell::new(STATE_ERR),
            drops: drops.clone(),
        };
        let mut fut = Box::pin(ResponseFuture::new(inner, clock.deadline()));

        match poll_once(fut.as_mut()) {
            Poll::Ready(Err(e)) => {
                let e = e
                    .downcast::<TestErr>()
                    .expect("ready inner error must be passed through unchanged");
                assert_eq!(e.0, "boom");
            }
            other => panic!("expected the inner error, got {:?}", other),
        }
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        drop(fut);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    // Nothing is ready: Pending, and the inner future is kept alive.
    #[test]
    fn both_pending_keeps_inner_alive() {
        let drops = Arc::new(AtomicUsize::new(0));
        let clock = ManualClock::new();

        let inner = Tracked {
            state: Cell::new(STATE_PENDING),
            drops: drops.clone(),
        };
        let mut fut = Box::pin(ResponseFuture::new(inner, clock.deadline()));

        assert!(poll_once(fut.as_mut()).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        drop(fut);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

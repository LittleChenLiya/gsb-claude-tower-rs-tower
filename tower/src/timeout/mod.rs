//! Middleware that applies a timeout to requests.
//!
//! If the response does not complete within the specified timeout, the response
//! will be aborted.

pub mod error;
pub mod future;
mod layer;

pub use self::layer::TimeoutLayer;

use self::future::ResponseFuture;
use std::task::{Context, Poll};
use std::time::Duration;
use tower_service::Service;

/// Applies a timeout to requests.
#[derive(Debug, Clone)]
pub struct Timeout<T> {
    inner: T,
    timeout: Duration,
}

// ===== impl Timeout =====

impl<T> Timeout<T> {
    /// Creates a new [`Timeout`]
    pub const fn new(inner: T, timeout: Duration) -> Self {
        Timeout { inner, timeout }
    }

    /// Get a reference to the inner service
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Get a mutable reference to the inner service
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Consume `self`, returning the inner service
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<S, Request> Service<Request> for Timeout<S>
where
    S: Service<Request>,
    S::Error: Into<crate::BoxError>,
{
    type Response = S::Response;
    type Error = crate::BoxError;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.inner.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => Poll::Ready(r.map_err(Into::into)),
        }
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let response = self.inner.call(request);
        let sleep = tokio::time::sleep(self.timeout);

        ResponseFuture::new(response, sleep)
    }
}

#[cfg(test)]
mod tests {
    //! Short regression tests for the original, duration-based interface
    //! (`Timeout::new(inner, Duration)`), which keeps using the default Tokio
    //! timer. The deterministic, same-turn cases over an injected deadline
    //! live next to `ResponseFuture` in `future::injected_clock_tests`.

    use super::error::Elapsed;
    use super::*;
    use std::convert::Infallible;
    use std::future::{pending, ready, Future, Pending, Ready};
    use tower_service::Service;

    #[derive(Clone)]
    struct ReadyService;

    impl Service<()> for ReadyService {
        type Response = ();
        type Error = Infallible;
        type Future = Ready<Result<(), Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: ()) -> Self::Future {
            ready(Ok(()))
        }
    }

    #[derive(Clone)]
    struct PendingService;

    impl Service<()> for PendingService {
        type Response = ();
        type Error = Infallible;
        type Future = Pending<Result<(), Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: ()) -> Self::Future {
            pending()
        }
    }

    // Original interface: a response that is ready immediately is returned
    // even with a zero-length timeout, because the response is polled before
    // the timer. Time is paused, so no real sleep happens.
    #[tokio::test(start_paused = true)]
    async fn ready_response_wins_through_original_interface() {
        let mut service = Timeout::new(ReadyService, Duration::ZERO);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut fut = Box::pin(service.call(()));
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(())) => {}
            other => panic!("expected the ready response, got {:?}", other),
        }
    }

    // Original interface: an inner future that never completes fails with the
    // timeout error once the duration elapses. Time is paused and advanced
    // manually, so no real sleep happens.
    #[tokio::test(start_paused = true)]
    async fn elapsed_deadline_times_out_through_original_interface() {
        let mut service = Timeout::new(PendingService, Duration::from_millis(20));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut fut = Box::pin(service.call(()));
        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "still inside the timeout window"
        );

        tokio::time::advance(Duration::from_millis(30)).await;

        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(Err(e)) => {
                assert!(e.is::<Elapsed>(), "expected Elapsed, got {:?}", e);
                assert_eq!(e.to_string(), "request timed out");
            }
            other => panic!("expected a timeout error, got {:?}", other),
        }
    }
}

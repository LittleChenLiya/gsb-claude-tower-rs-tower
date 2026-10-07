//! Short regression example for the original [`Timeout`]/[`TimeoutLayer`]
//! interface. Tokio's runtime is started paused and time is advanced by hand,
//! so no real sleeping happens; this confirms the default tokio timing path
//! stays unchanged and agrees with the injected-deadline tests.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio_test::{assert_pending, assert_ready, task};
use tower::timeout::TimeoutLayer;
use tower::{Layer, Service};

/// An inner service whose response never completes on its own (no sleep).
#[derive(Clone)]
struct PendingService;

impl Service<()> for PendingService {
    type Response = &'static str;
    type Error = Infallible;
    type Future = PendingOnce;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        PendingOnce
    }
}

struct PendingOnce;

impl Future for PendingOnce {
    type Output = Result<&'static str, Infallible>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

/// An inner service that is immediately ready with a response.
#[derive(Clone)]
struct ReadyService;

impl Service<()> for ReadyService {
    type Response = &'static str;
    type Error = Infallible;
    type Future = std::future::Ready<Result<&'static str, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: ()) -> Self::Future {
        std::future::ready(Ok("fast"))
    }
}

/// Default timing path: advancing the virtual clock past the duration returns
/// the `Elapsed` timeout error, without any real sleep.
#[tokio::test(start_paused = true)]
async fn times_out_on_default_tokio_clock() {

    let mut svc = TimeoutLayer::new(Duration::from_secs(30)).layer(PendingService);

    let mut fut = task::spawn(svc.call(()));
    assert_pending!(
        fut.poll(),
        "before the deadline the request is still pending"
    );

    tokio::time::advance(Duration::from_secs(30)).await;
    let result = assert_ready!(fut.poll(), "deadline reached => ready");
    let error = result.unwrap_err();
    assert_eq!(error.to_string(), "request timed out");
    assert!(error
        .downcast_ref::<tower::timeout::error::Elapsed>()
        .is_some());
}

/// An inner response that finishes before the deadline is returned unchanged,
/// matching the manual-clock priority behaviour.
#[tokio::test(start_paused = true)]
async fn ready_inner_wins_on_default_tokio_clock() {

    let mut svc = TimeoutLayer::new(Duration::from_secs(30)).layer(ReadyService);

    let mut fut = task::spawn(svc.call(()));
    let result = assert_ready!(fut.poll(), "inner immediately ready");
    assert_eq!(result.unwrap(), "fast");
}

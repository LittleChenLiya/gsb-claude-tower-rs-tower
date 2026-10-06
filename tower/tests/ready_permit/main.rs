#![cfg(feature = "ready-permit")]
//! Directed poll-fixture tests for the `ready_permit` middleware.
//!
//! Each test drives a `ReadyPermit` service manually through the
//! `tower_test::mock` poll fixture, exercising one step of the permit
//! lifecycle documented in `tower::ready_permit`:
//!
//! 1. `poll_ready` acquires capacity; the inner service's readiness is
//!    preserved.
//! 2. `call` settles the held permit exactly once; the resource is released
//!    only when the call actually ends.
//! 3. Dropping a ready service returns the permit, so the next request can
//!    become ready.

use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio_test::{assert_pending, assert_ready_ok};
use tower::ready_permit::{ReadyPermit, ReadyPermitLayer};
use tower_test::{assert_request_eq, mock};

type Req = &'static str;
type Res = &'static str;

/// Build a directed poll fixture: a `ReadyPermit` over a mock service, bound
/// to `semaphore`, polled manually through `mock::Spawn`.
fn fixture(
    semaphore: &Arc<Semaphore>,
) -> (
    mock::Spawn<ReadyPermit<mock::Mock<Req, Res>>>,
    mock::Handle<Req, Res>,
) {
    let semaphore = semaphore.clone();
    mock::spawn_with(move |mock| ReadyPermit::with_semaphore(mock, semaphore.clone()))
}

/// Capacity 1: a permit acquired by `poll_ready` is returned when the ready
/// service is dropped, so the next request can become ready.
#[tokio::test(flavor = "current_thread")]
async fn drop_after_ready_returns_capacity() {
    let semaphore = Arc::new(Semaphore::new(1));
    let (mut s1, mut h1) = fixture(&semaphore);
    let (mut s2, mut h2) = fixture(&semaphore);
    h1.allow(1);
    h2.allow(1);

    // s1 acquires the only permit and becomes ready.
    assert_ready_ok!(s1.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // s2 cannot acquire capacity while s1 holds the permit.
    assert_pending!(s2.poll_ready());

    // Dropping the ready service returns the permit exactly once. The
    // semaphore hands it directly to the queued waiter (s2)...
    drop(s1);

    // ...so the next request can now become ready.
    assert!(s2.is_woken());
    assert_ready_ok!(s2.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // And dropping s2 settles that permit exactly once as well.
    drop(s2);
    assert_eq!(semaphore.available_permits(), 1);
}

/// Capacity 1: while a call is in flight, a second request cannot acquire
/// the permit; capacity is restored when the call completes.
#[tokio::test(flavor = "current_thread")]
async fn permit_held_for_whole_call() {
    let semaphore = Arc::new(Semaphore::new(1));
    let (mut s1, mut h1) = fixture(&semaphore);
    let (mut s2, mut h2) = fixture(&semaphore);
    h1.allow(1);
    h2.allow(1);

    assert_ready_ok!(s1.poll_ready());
    let r1 = s1.call("hello");

    // The permit moved into the in-flight call: a second request cannot
    // become ready.
    assert_pending!(s2.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    assert_request_eq!(h1, "hello").send_response("world");

    // The resource is released only when the call actually ends.
    assert_eq!(r1.await.unwrap(), "world");
    assert!(s2.is_woken());
    assert_ready_ok!(s2.poll_ready());
}

/// The capacity acquired by `poll_ready` is settled exactly once: repeated
/// `poll_ready` calls do not acquire more permits, and dropping the service
/// after `call` does not return the permit a second time.
#[tokio::test(flavor = "current_thread")]
async fn capacity_settled_exactly_once() {
    let semaphore = Arc::new(Semaphore::new(1));
    let (mut svc, mut handle) = fixture(&semaphore);
    handle.allow(2);

    assert_ready_ok!(svc.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // Polling ready again reuses the held permit instead of acquiring more.
    assert_ready_ok!(svc.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // `call` settles the held permit; dropping the service now must not
    // return the permit a second time.
    let r1 = svc.call("hello");
    drop(svc);
    assert_eq!(semaphore.available_permits(), 0);

    assert_request_eq!(handle, "hello").send_response("world");
    assert_eq!(r1.await.unwrap(), "world");

    // The call ended, so the capacity is back exactly once.
    assert_eq!(semaphore.available_permits(), 1);
}

/// The inner service's default readiness behavior is preserved: the permit
/// is held while the inner service is pending, and reused once it readies.
#[tokio::test(flavor = "current_thread")]
async fn inner_readiness_is_preserved() {
    let semaphore = Arc::new(Semaphore::new(1));
    let (mut svc, mut handle) = fixture(&semaphore);

    // The mock service is ready by default; make it not ready.
    handle.allow(0);

    // The permit is acquired, but the inner service is not ready yet.
    assert_pending!(svc.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // Once the inner service becomes ready, the held permit is reused.
    handle.allow(1);
    assert!(svc.is_woken());
    assert_ready_ok!(svc.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);
}

/// The layer builds a `ReadyPermit` service with the given capacity.
#[tokio::test(flavor = "current_thread")]
async fn layer_builds_ready_permit_service() {
    let (mut svc, mut handle) =
        mock::spawn_layer::<Req, Res, _>(ReadyPermitLayer::new(1));
    handle.allow(1);

    assert_ready_ok!(svc.poll_ready());
    let response = svc.call("ping");

    assert_request_eq!(handle, "ping").send_response("pong");
    assert_eq!(response.await.unwrap(), "pong");
}

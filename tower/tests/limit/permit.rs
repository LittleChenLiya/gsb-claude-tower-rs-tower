//! Targeted, deterministic tests for [`tower::limit::permit`] readiness
//! permit accounting.
//!
//! The first half drives a tiny, fully controllable `gated` service through
//! the [`Harness`] mock-task fixture, so every state of the permit lifecycle
//! is polled by hand without timers or background tasks. The last test uses
//! the `tower_test` mock service to exercise the layer wiring.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tokio::sync::Semaphore;
use tokio_test::{assert_pending, assert_ready_ok};
use tower::limit::permit::ReadyPermitLayer;
use tower::limit::ReadyPermit;
use tower::Service;
use tower_test::{assert_request_eq, mock};

/// A minimal mock-task harness for manual `poll_ready` / future polling.
///
/// It provides a no-op waker that records whether the polled value asked to
/// be woken, which is the only side effect the permit lifecycle tests need.
#[derive(Debug)]
struct Harness<T> {
    inner: T,
    woken: Arc<AtomicBool>,
    wake_count: Arc<AtomicUsize>,
}

impl<T> Harness<T> {
    fn new(inner: T) -> Self {
        Harness {
            inner,
            woken: Arc::new(AtomicBool::new(false)),
            wake_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn is_woken(&self) -> bool {
        self.woken.load(Ordering::SeqCst)
    }

    fn wake_count(&self) -> usize {
        self.wake_count.load(Ordering::SeqCst)
    }

    fn waker(&self) -> Waker {
        let state = Arc::new(HarnessWaker {
            woken: self.woken.clone(),
            wake_count: self.wake_count.clone(),
        });
        let raw = Arc::into_raw(state) as *const ();
        let raw = RawWaker::new(raw, &HARNESS_WAKER_VTABLE);
        unsafe { Waker::from_raw(raw) }
    }
}

#[derive(Debug)]
struct HarnessWaker {
    woken: Arc<AtomicBool>,
    wake_count: Arc<AtomicUsize>,
}

static HARNESS_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    harness_waker_clone,
    harness_waker_wake,
    harness_waker_wake_by_ref,
    harness_waker_drop,
);

unsafe fn harness_waker_clone(ptr: *const ()) -> RawWaker {
    let state = Arc::from_raw(ptr as *const HarnessWaker);
    let cloned = state.clone();
    std::mem::forget(state);
    RawWaker::new(Arc::into_raw(cloned) as *const (), &HARNESS_WAKER_VTABLE)
}

unsafe fn harness_waker_wake(ptr: *const ()) {
    let state = Arc::from_raw(ptr as *const HarnessWaker);
    state.woken.store(true, Ordering::SeqCst);
    state.wake_count.fetch_add(1, Ordering::SeqCst);
}

unsafe fn harness_waker_wake_by_ref(ptr: *const ()) {
    let state = Arc::from_raw(ptr as *const HarnessWaker);
    state.woken.store(true, Ordering::SeqCst);
    state.wake_count.fetch_add(1, Ordering::SeqCst);
    std::mem::forget(state);
}

unsafe fn harness_waker_drop(ptr: *const ()) {
    drop(Arc::from_raw(ptr as *const HarnessWaker));
}

impl<T> Harness<T> {
    fn poll_ready<Request>(&mut self) -> Poll<Result<(), T::Error>>
    where
        T: Service<Request>,
    {
        let waker = self.waker();
        let mut cx = Context::from_waker(&waker);
        self.inner.poll_ready(&mut cx)
    }

    fn call<Request>(&mut self, req: Request) -> T::Future
    where
        T: Service<Request>,
    {
        self.inner.call(req)
    }
}

impl<T> Harness<T>
where
    T: Future + Unpin,
{
    fn poll(&mut self) -> Poll<T::Output> {
        let waker = self.waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Pin::new(&mut self.inner);
        fut.as_mut().poll(&mut cx)
    }
}

/// Backside of the [`gated`] fixture: switches readiness externally and
/// counts the requests driven through the inner service.
#[derive(Clone, Debug)]
struct Gate {
    ready: Arc<Mutex<bool>>,
    waker: Arc<Mutex<Option<Waker>>>,
    calls: Arc<Mutex<usize>>,
}

impl Gate {
    fn new(initially_ready: bool) -> Self {
        Gate {
            ready: Arc::new(Mutex::new(initially_ready)),
            waker: Arc::new(Mutex::new(None)),
            calls: Arc::new(Mutex::new(0)),
        }
    }

    fn set_ready(&self, ready: bool) {
        *self.ready.lock().unwrap() = ready;
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn is_ready(&self) -> bool {
        *self.ready.lock().unwrap()
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

/// Response future of the [`gated`] service. It finishes on the first poll,
/// so a call ends exactly when the test polls or drops the future.
#[derive(Debug)]
struct GatedResponse;

impl Future for GatedResponse {
    type Output = Result<u8, u8>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(7))
    }
}

/// A service whose readiness is switched externally through the [`Gate`].
#[derive(Clone, Debug)]
struct GatedService {
    gate: Gate,
}

impl Service<u8> for GatedService {
    type Response = u8;
    type Error = u8;
    type Future = GatedResponse;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.gate.is_ready() {
            Poll::Ready(Ok(()))
        } else {
            *self.gate.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    fn call(&mut self, _req: u8) -> Self::Future {
        *self.gate.calls.lock().unwrap() += 1;
        GatedResponse
    }
}

/// Build a [`ReadyPermit`] around a [`GatedService`] and return its gate.
fn gated(max: usize, initially_ready: bool) -> (ReadyPermit<GatedService>, Gate) {
    let gate = Gate::new(initially_ready);
    let service = GatedService { gate: gate.clone() };
    (ReadyPermit::new(service, max), gate)
}

// Acceptance: the capacity taken by `poll_ready` is settled once. A handle
// dropped after readiness without a call returns the permit, so the next
// request becomes ready again.
#[tokio::test(flavor = "current_thread")]
async fn ready_then_drop_returns_capacity() {
    let semaphore = Arc::new(Semaphore::new(1));
    let gate_a = Gate::new(true);
    let gate_b = Gate::new(true);
    let a = ReadyPermit::with_semaphore(
        GatedService {
            gate: gate_a.clone(),
        },
        semaphore.clone(),
    );
    let b = ReadyPermit::with_semaphore(GatedService { gate: gate_b }, semaphore);
    let mut a = Harness::new(a);
    let mut b = Harness::new(b);

    // The first handle reserves the only permit.
    assert_ready_ok!(a.poll_ready());
    // Repeated readiness reuses the reservation; nothing is settled twice.
    assert_ready_ok!(a.poll_ready());

    // The next request cannot obtain capacity meanwhile.
    assert_pending!(b.poll_ready());

    // Abandon the reservation without issuing a call.
    drop(a);
    assert_eq!(gate_a.calls(), 0);
    assert!(b.is_woken());

    // The returned capacity lets the next request become ready.
    assert_ready_ok!(b.poll_ready());
}

// Acceptance: while a call is in flight a second request cannot take the
// permit; finishing the call restores readiness.
#[tokio::test(flavor = "current_thread")]
async fn call_holds_permit_until_response_future_ends() {
    let (service, gate) = gated(1, true);
    let mut service = Harness::new(service);

    assert_ready_ok!(service.poll_ready());
    let mut response = Harness::new(service.call(1u8));
    assert_eq!(gate.calls(), 1);

    // The outstanding call keeps the only permit occupied.
    assert_pending!(service.poll_ready());

    // Completing (and dropping) the call releases the capacity.
    assert_ready_ok!(response.poll());
    drop(response);
    assert!(service.is_woken());
    assert_ready_ok!(service.poll_ready());

    let response2 = service.call(2u8);
    assert_eq!(gate.calls(), 2);
    drop(response2);
}

// Acceptance: dropping the response future counts as the call ending.
#[tokio::test(flavor = "current_thread")]
async fn dropped_response_future_releases_capacity() {
    let (service, _gate) = gated(1, true);
    let mut service = Harness::new(service);

    assert_ready_ok!(service.poll_ready());
    let response = service.call(1u8);

    assert_pending!(service.poll_ready());

    drop(response);

    assert_ready_ok!(service.poll_ready());
}

// Lifecycle: capacity is reserved before the inner service reports readiness
// and is handed back when that readiness wait is abandoned by dropping the
// handle.
#[tokio::test(flavor = "current_thread")]
async fn reservation_waiting_on_inner_ready_is_returned_on_drop() {
    let semaphore = Arc::new(Semaphore::new(1));

    let gate = Gate::new(false);
    let waiter =
        ReadyPermit::with_semaphore(GatedService { gate: gate.clone() }, semaphore.clone());
    let mut waiter = Harness::new(waiter);
    assert_pending!(waiter.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // Dropping while the inner service has not reported readiness frees the
    // capacity taken from the pool.
    drop(waiter);
    assert_eq!(semaphore.available_permits(), 1);

    // The gate of the abandoned handle stays closed; it is not polled again.
    assert!(!gate.is_ready());

    // A fresh request can reserve the returned capacity.
    let gate2 = Gate::new(true);
    let mut next = Harness::new(ReadyPermit::with_semaphore(
        GatedService { gate: gate2 },
        semaphore,
    ));
    assert_ready_ok!(next.poll_ready());
}

// Lifecycle: once the capacity is available, the inner service becoming ready
// propagates readiness to the caller; capacity stays reserved across polls.
#[tokio::test(flavor = "current_thread")]
async fn capacity_is_settled_when_inner_readiness_arrives() {
    let semaphore = Arc::new(Semaphore::new(1));
    let gate = Gate::new(false);
    let mut service = Harness::new(ReadyPermit::with_semaphore(
        GatedService { gate: gate.clone() },
        semaphore.clone(),
    ));

    // Capacity is reserved immediately even though readiness is pending.
    assert_pending!(service.poll_ready());
    assert_eq!(semaphore.available_permits(), 0);

    // Opening the gate wakes the registered task, and readiness propagates.
    gate.set_ready(true);
    assert!(service.is_woken());
    assert_ready_ok!(service.poll_ready());

    // The reservation is still the single permit; it is not re-acquired.
    assert_eq!(semaphore.available_permits(), 0);
    // Only the gate opening wakes the task; the semaphore never wakes it
    // again because no additional permit is acquired.
    assert_eq!(service.wake_count(), 1);
}

// Layer wiring with the mock service: one request in flight at a time.
#[tokio::test(flavor = "current_thread")]
async fn layer_admits_one_in_flight_request_at_a_time() {
    let (mut service, mut handle) = mock::spawn_layer(ReadyPermitLayer::new(1));

    assert_ready_ok!(service.poll_ready());
    let r1 = service.call("hello 1");

    assert_pending!(service.poll_ready());
    assert!(!service.is_woken());

    assert_request_eq!(handle, "hello 1").send_response("world 1");

    assert_eq!(r1.await.unwrap(), "world 1");
    assert!(service.is_woken());

    assert_ready_ok!(service.poll_ready());
    let r2 = service.call("hello 2");
    assert_pending!(service.poll_ready());

    assert_request_eq!(handle, "hello 2").send_response("world 2");
    assert_eq!(r2.await.unwrap(), "world 2");
}

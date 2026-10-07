#![cfg(feature = "observe")]

//! Deterministic, in-memory acceptance tests for the `observe` middleware.
//!
//! Nothing here waits on a timer or a runtime: the inner futures are manually
//! driven (the first `poll` returns `Pending`, a later `poll` returns
//! `Ready`), and a canceled call is simply dropped while pending.

use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};
use tower::{
    observe::{CallObserver, CallOutcome, Observe, ObserveLayer},
    Service, ServiceBuilder,
};

type BoxError = Box<dyn Error + Send + Sync>;

/// In-memory fixture collecting every observed outcome.
#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<(u64, CallOutcome)>>,
    fail_next: AtomicU64,
}

impl Recorder {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The next `n` observations return an error.
    fn fail_next(&self, n: u64) {
        self.fail_next.store(n, Ordering::Relaxed);
    }

    fn events(&self) -> Vec<(u64, CallOutcome)> {
        self.events.lock().unwrap().clone()
    }

    fn count(&self, outcome: CallOutcome) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, o)| *o == outcome)
            .count()
    }
}

impl CallObserver for Recorder {
    fn observe(&self, id: u64, outcome: CallOutcome) -> Result<(), BoxError> {
        if self.fail_next.load(Ordering::Relaxed) > 0 {
            self.fail_next.fetch_sub(1, Ordering::Relaxed);
            return Err("observer boom".into());
        }
        self.events.lock().unwrap().push((id, outcome));
        Ok(())
    }
}

/// Behavior the mock inner service should exhibit on the next call.
#[derive(Clone, Copy, Debug)]
enum Script {
    /// Stays pending until dropped (canceled).
    PendingOk,
    /// Ready immediately with an `Ok` response.
    ReadyOk,
    /// Ready immediately with an inner error.
    ReadyErr,
}

/// Manual future: pending until `complete` is set; never waits on I/O.
struct ManualFut {
    complete: bool,
    output: Result<&'static str, &'static str>,
    polls: u64,
    dropped: Option<Arc<AtomicU64>>,
}

impl Future for ManualFut {
    type Output = Result<&'static str, &'static str>;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls += 1;
        if self.complete {
            Poll::Ready(self.output)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ManualFut {
    fn drop(&mut self) {
        if let Some(dropped) = &self.dropped {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Default)]
struct MockState {
    ready_fails: AtomicBool,
    ready_calls: AtomicU64,
    inner_calls: AtomicU64,
    /// Scripted outcomes, consumed from the end.
    script: Mutex<Vec<Script>>,
    /// Drop counters handed out to futures, in call order.
    drop_counters: Mutex<Vec<Arc<AtomicU64>>>,
}

/// Cloneable inner service. Clones share all backing state, like a service
/// whose clones front one shared connection/worker.
#[derive(Clone)]
struct MockService {
    st: Arc<MockState>,
}

impl MockService {
    fn new(ready_fails: bool, script: Vec<Script>) -> Self {
        MockService {
            st: Arc::new(MockState {
                ready_fails: AtomicBool::new(ready_fails),
                script: Mutex::new(script),
                ..Default::default()
            }),
        }
    }

    fn ready_calls(&self) -> u64 {
        self.st.ready_calls.load(Ordering::Relaxed)
    }

    fn inner_calls(&self) -> u64 {
        self.st.inner_calls.load(Ordering::Relaxed)
    }

    fn drops(&self) -> u64 {
        self.st
            .drop_counters
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum()
    }
}

impl Service<&'static str> for MockService {
    type Response = &'static str;
    type Error = &'static str;
    type Future = ManualFut;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.st.ready_calls.fetch_add(1, Ordering::Relaxed);
        if self.st.ready_fails.swap(false, Ordering::Relaxed) {
            Poll::Ready(Err("not ready"))
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, _req: &'static str) -> Self::Future {
        self.st.inner_calls.fetch_add(1, Ordering::Relaxed);
        let script = self
            .st
            .script
            .lock()
            .unwrap()
            .pop()
            .expect("mock has one scripted outcome per call");
        let (complete, output) = match script {
            Script::PendingOk => (false, Ok("ok")),
            Script::ReadyOk => (true, Ok("ok")),
            Script::ReadyErr => (true, Err("inner boom")),
        };
        let dropped = Arc::new(AtomicU64::new(0));
        self.st.drop_counters.lock().unwrap().push(dropped.clone());
        ManualFut {
            complete,
            output,
            polls: 0,
            dropped: Some(dropped),
        }
    }
}

type ObsMock = Observe<MockService, Arc<Recorder>>;

fn noop_waker() -> Waker {
    fn clone_raw(_: *const ()) -> RawWaker {
        noop_raw()
    }
    fn noop(_: *const ()) {}
    fn noop_raw() -> RawWaker {
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, noop, noop, noop);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe { Waker::from_raw(noop_raw()) }
}

fn poll_ready_ok<S, Req>(svc: &mut S)
where
    S: Service<Req>,
{
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(svc.poll_ready(&mut cx), Poll::Ready(Ok(()))));
}

fn poll_once<F: Future>(fut: &mut Pin<Box<F>>) -> Poll<F::Output> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    fut.as_mut().poll(&mut cx)
}

// ===== Acceptance scenarios =====

/// Two manual futures on one service: id 1 polled pending then dropped
/// (canceled), id 2 driven to success. Checks IDs and terminal counts.
#[test]
fn success_and_drop_unique_ids_and_terminal_counts() {
    let recorder = Recorder::new();
    let mock = MockService::new(false, vec![Script::ReadyOk, Script::PendingOk]);
    let mut svc: ObsMock = Observe::new(mock.clone(), recorder.clone());

    poll_ready_ok(&mut svc);

    // Call id=1: starts pending, polled once, then dropped -> canceled.
    let mut f1 = Box::pin(svc.call("req-1"));
    assert!(poll_once(&mut f1).is_pending(), "manual future starts pending");
    assert_eq!(svc.pending_calls(), vec![1]);
    drop(f1);
    assert!(svc.pending_calls().is_empty());

    // Call id=2: immediately ready success.
    poll_ready_ok(&mut svc);
    let mut f2 = Box::pin(svc.call("req-2"));
    assert!(matches!(poll_once(&mut f2), Poll::Ready(Ok("ok"))));
    assert!(svc.pending_calls().is_empty());
    drop(f2);

    assert_eq!(mock.inner_calls(), 2);
    assert_eq!(mock.drops(), 2, "both inner futures were dropped");

    let events = recorder.events();
    println!("success+drop events: {events:?}");
    assert_eq!(
        events,
        vec![(1, CallOutcome::Canceled), (2, CallOutcome::Success)]
    );
    assert_eq!(recorder.count(CallOutcome::Success), 1);
    assert_eq!(recorder.count(CallOutcome::InnerError), 0);
    assert_eq!(recorder.count(CallOutcome::Canceled), 1);
    let ids: Vec<u64> = events.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids.iter().collect::<std::collections::HashSet<_>>().len(), ids.len());
    assert_eq!(svc.observer_errors(), 0);
}

/// A `poll_ready` error allocates no ID, starts no inner call, records
/// nothing, and does not consume an ID (the later call still gets id 1).
#[test]
fn ready_failure_produces_no_record_and_consumes_no_id() {
    let recorder = Recorder::new();
    let mock = MockService::new(true, vec![Script::ReadyOk]);
    let mut svc = Observe::new(mock.clone(), recorder.clone());

    let waker = noop_waker();
    let mut cx = Context::from_waker(&mut &waker);
    assert!(matches!(
        svc.poll_ready(&mut cx),
        Poll::Ready(Err("not ready"))
    ));
    assert!(recorder.events().is_empty());
    assert!(svc.pending_calls().is_empty());
    assert_eq!(mock.inner_calls(), 0);
    assert_eq!(mock.ready_calls(), 1);

    // Readiness recovered on the next probe; the call takes id 1.
    poll_ready_ok(&mut svc);
    let mut f = Box::pin(svc.call("after-ready-fail"));
    assert!(matches!(poll_once(&mut f), Poll::Ready(Ok("ok"))));
    drop(f);

    assert_eq!(mock.inner_calls(), 1);
    let events = recorder.events();
    println!("ready-failure events: {events:?}");
    assert_eq!(events, vec![(1, CallOutcome::Success)]);
}

/// When the observer callback fails the inner result is returned unchanged;
/// the failure is only counted. Once recovered, the terminal outcome is
/// recorded exactly once (and dropping the completed future adds nothing).
#[test]
fn observer_error_does_not_mask_inner_result_and_records_once() {
    let recorder = Recorder::new();
    recorder.fail_next(1);
    let mock = MockService::new(false, vec![Script::ReadyOk, Script::ReadyErr]);
    let mut svc: ObsMock = Observe::new(mock, recorder.clone());

    poll_ready_ok(&mut svc);
    let mut f = Box::pin(svc.call("boom"));
    assert!(
        matches!(poll_once(&mut f), Poll::Ready(Err("inner boom"))),
        "inner error is surfaced verbatim despite observer failure"
    );
    drop(f);
    assert!(recorder.events().is_empty(), "failed notification adds no event");
    assert_eq!(svc.observer_errors(), 1);

    // Observer recovered: the inner success records once as success.
    poll_ready_ok(&mut svc);
    let mut g = Box::pin(svc.call("ok"));
    assert!(matches!(poll_once(&mut g), Poll::Ready(Ok("ok"))));
    drop(g); // dropping an already-completed future adds no second record
    println!("observer-error events: {:?}", recorder.events());
    assert_eq!(recorder.events(), vec![(2, CallOutcome::Success)]);
    assert_eq!(recorder.count(CallOutcome::Success), 1);
    assert_eq!(recorder.count(CallOutcome::Canceled), 0);
    assert_eq!(svc.observer_errors(), 1);
}

/// Clones share the ID allocator (unique IDs across clones) but keep separate
/// in-flight sets; canceling a call on one clone leaves the other untouched.
#[test]
fn clones_share_id_allocator_not_pending_state() {
    let recorder = Recorder::new();
    let mock = MockService::new(false, vec![Script::PendingOk, Script::PendingOk]);
    let svc: ObsMock = Observe::new(mock, recorder.clone());
    let mut a = svc.clone();
    let mut b = svc.clone();

    poll_ready_ok(&mut a);
    poll_ready_ok(&mut b);
    let fa = Box::pin(a.call("a"));
    let fb = Box::pin(b.call("b"));

    assert_eq!(a.pending_calls(), vec![1]);
    assert_eq!(b.pending_calls(), vec![2], "shared counter, distinct ids");

    drop(fb); // cancel id 2 on clone b
    assert_eq!(a.pending_calls(), vec![1], "clone a is unaffected");
    assert!(b.pending_calls().is_empty());
    drop(fa);

    let events = recorder.events();
    println!("clone events: {events:?}");
    assert_eq!(
        events,
        vec![(2, CallOutcome::Canceled), (1, CallOutcome::Canceled)]
    );
}

/// The layer plugs into the standard builder interface and records through the
/// same path.
#[test]
fn layer_works_through_service_builder() {
    let recorder = Recorder::new();
    let mock = MockService::new(false, vec![Script::ReadyOk]);
    let mut svc: ObsMock = ServiceBuilder::new()
        .layer(ObserveLayer::new(recorder.clone()))
        .service(mock);

    poll_ready_ok(&mut svc);
    let mut f = Box::pin(svc.call("req"));
    assert!(matches!(poll_once(&mut f), Poll::Ready(Ok("ok"))));
    drop(f);
    assert_eq!(recorder.events(), vec![(1, CallOutcome::Success)]);
}


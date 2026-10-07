#![cfg(feature = "observe")]

//! Acceptance tests for `tower::observe`.
//!
//! Everything here is driven manually with a no-op waker: there is no async
//! runtime and no real waiting. The "canceled" future is polled once (returns
//! `Pending`) and then dropped.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tower::observe::{CallEvent, CallObserver, CallOutcome, ObserveCall, ObserveCallLayer};
use tower::{BoxError, Layer, Service};

type R<T = &'static str, E = &'static str> = Result<T, E>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    ReadyOk,
    Pending,
    ReadyErr,
    ReadyFails,
}

#[derive(Clone)]
enum ManualFut {
    ReadyOk,
    ReadyErr,
    Pending,
}

impl Future for ManualFut {
    type Output = R;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &*self {
            ManualFut::ReadyOk => Poll::Ready(Ok("ok")),
            ManualFut::ReadyErr => Poll::Ready(Err("inner-boom")),
            ManualFut::Pending => Poll::Pending,
        }
    }
}

#[derive(Clone)]
struct ManualService {
    mode: Rc<Cell<Mode>>,
    calls: Rc<AtomicUsize>,
}

impl ManualService {
    fn new(mode: Mode) -> Self {
        Self {
            mode: Rc::new(Cell::new(mode)),
            calls: Rc::new(AtomicUsize::new(0)),
        }
    }
}

impl Service<()> for ManualService {
    type Response = &'static str;
    type Error = &'static str;
    type Future = ManualFut;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<R<(), &'static str>> {
        match self.mode.get() {
            Mode::ReadyFails => Poll::Ready(Err("not-ready")),
            _ => Poll::Ready(Ok(())),
        }
    }

    fn call(&mut self, _req: ()) -> ManualFut {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.mode.get() {
            Mode::ReadyOk => ManualFut::ReadyOk,
            Mode::ReadyErr => ManualFut::ReadyErr,
            Mode::Pending => ManualFut::Pending,
            Mode::ReadyFails => panic!("call must not follow a failed poll_ready"),
        }
    }
}

#[derive(Clone, Default)]
struct Recorder {
    events: Rc<RefCell<Vec<CallEvent>>>,
    observer_invocations: Rc<AtomicUsize>,
    fail: bool,
}

impl Recorder {
    fn ids(&self) -> Vec<u64> {
        self.events.borrow().iter().map(|e| e.id).collect()
    }

    fn outcomes(&self) -> Vec<CallOutcome> {
        self.events.borrow().iter().map(|e| e.outcome).collect()
    }

    fn count(&self, outcome: CallOutcome) -> usize {
        self.outcomes().iter().filter(|o| **o == outcome).count()
    }

    fn dump(&self) -> String {
        self.events
            .borrow()
            .iter()
            .map(|e| format!("  id={} outcome={}", e.id, e.outcome))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl CallObserver for Recorder {
    fn observe(&mut self, event: CallEvent) -> Result<(), BoxError> {
        self.observer_invocations.fetch_add(1, Ordering::Relaxed);
        self.events.borrow_mut().push(event);
        if self.fail {
            Err("observer-boom".into())
        } else {
            Ok(())
        }
    }
}

fn noop_waker() -> Waker {
    fn clone_raw(_: *const ()) -> RawWaker {
        raw()
    }
    fn noop(_: *const ()) {}
    fn raw() -> RawWaker {
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_raw, noop, noop, noop);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe { Waker::from_raw(raw()) }
}

fn ready_svc<S: Service<()>>(svc: &mut S) -> R<(), S::Error> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    match svc.poll_ready(&mut cx) {
        Poll::Ready(r) => r,
        Poll::Pending => panic!("manual service is always ready"),
    }
}

fn poll_once<F: Future>(fut: &mut Pin<Box<F>>) -> Poll<F::Output> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    fut.as_mut().poll(&mut cx)
}

#[test]
fn success_drop_and_ready_failure() {
    let rec = Recorder::default();
    let inner = ManualService::new(Mode::ReadyOk);
    let inner_calls = inner.calls.clone();
    let mode = inner.mode.clone();
    let mut svc = ObserveCallLayer::new(rec.clone()).layer(inner);

    // Call id=1: polled once, resolves successfully with no real waiting.
    ready_svc(&mut svc).unwrap();
    let mut f1 = Box::pin(svc.call(()));
    assert_eq!(poll_once(&mut f1), Poll::Ready(Ok("ok")));

    // Call id=2: polled once (Pending), then dropped -> canceled.
    mode.set(Mode::Pending);
    ready_svc(&mut svc).unwrap();
    let mut f2 = Box::pin(svc.call(()));
    assert_eq!(poll_once(&mut f2), Poll::Pending);

    // poll_ready fails: this must not allocate an id or record a call.
    mode.set(Mode::ReadyFails);
    assert_eq!(ready_svc(&mut svc), Err("not-ready"));

    // After the failed readiness check, ids continue where they left off.
    mode.set(Mode::ReadyErr);
    ready_svc(&mut svc).unwrap();
    let mut f3 = Box::pin(svc.call(()));
    assert_eq!(poll_once(&mut f3), Poll::Ready(Err("inner-boom")));

    // Pending future dropped here -> exactly one cancel record. Completed
    // futures dropping at scope end must not record a second time. (The
    // cancel for id=2 is recorded now, after id=3 already finished, so events
    // arrive in completion rather than id order.)
    drop(f2);

    let mut events = rec.events.borrow().clone();
    events.sort_by_key(|e| e.id);
    println!("observed terminal events (by id):");
    for e in &events {
        println!("  id={} outcome={}", e.id, e.outcome);
    }

    let ids: Vec<u64> = events.iter().map(|e| e.id).collect();
    assert_eq!(ids, vec![1, 2, 3], "ids must be unique and gapless");
    assert_eq!(events[0].outcome, CallOutcome::Success);
    assert_eq!(events[1].outcome, CallOutcome::Canceled);
    assert_eq!(events[2].outcome, CallOutcome::InnerError);
    assert_eq!(rec.count(CallOutcome::Success), 1);
    assert_eq!(rec.count(CallOutcome::Canceled), 1);
    assert_eq!(rec.count(CallOutcome::InnerError), 1);
    assert_eq!(rec.observer_invocations.load(Ordering::Relaxed), 3);
    assert_eq!(inner_calls.load(Ordering::Relaxed), 3);
}

#[test]
fn observer_error_keeps_inner_result() {
    let mut rec = Recorder::default();
    rec.fail = true;

    // Inner Ok survives the observer error.
    let mut ok_svc = ObserveCall::new(ManualService::new(Mode::ReadyOk), rec.clone());
    ready_svc(&mut ok_svc).unwrap();
    let mut ok_fut = Box::pin(ok_svc.call(()));
    assert_eq!(poll_once(&mut ok_fut), Poll::Ready(Ok("ok")));
    assert_eq!(rec.ids(), vec![1], "a brand new service starts ids at 1");
    assert_eq!(rec.outcomes(), vec![CallOutcome::Success]);

    // Inner Err likewise survives the observer error.
    let mut err_svc = ObserveCall::new(ManualService::new(Mode::ReadyErr), rec.clone());
    ready_svc(&mut err_svc).unwrap();
    let mut err_fut = Box::pin(err_svc.call(()));
    assert_eq!(poll_once(&mut err_fut), Poll::Ready(Err("inner-boom")));

    println!("observed terminal events:\n{}", rec.dump());

    // Each call was still recorded exactly once despite the callback errors.
    assert_eq!(rec.observer_invocations.load(Ordering::Relaxed), 2);
    assert_eq!(
        rec.outcomes(),
        vec![CallOutcome::Success, CallOutcome::InnerError]
    );
}

#[test]
fn clones_share_ids_not_pending_state() {
    let rec = Recorder::default();
    let mut a = ObserveCall::new(ManualService::new(Mode::ReadyOk), rec.clone());
    let mut b = a.clone();

    // Two interleaved, concurrently outstanding calls from the service and
    // its clone get distinct ids from the shared allocator.
    ready_svc(&mut a).unwrap();
    let mut fa = Box::pin(a.call(())); // id 1
    ready_svc(&mut b).unwrap();
    let mut fb = Box::pin(b.call(())); // id 2
    assert_eq!(poll_once(&mut fb), Poll::Ready(Ok("ok")));
    assert_eq!(poll_once(&mut fa), Poll::Ready(Ok("ok")));

    let mut ids = rec.ids();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2], "ids are shared across clones");
    assert!(rec.outcomes().iter().all(|o| *o == CallOutcome::Success));

    // Dropping one clone pending future cancels only that call. The new
    // ObserveCall owns a fresh allocator starting at id 1, shared by c and d.
    let rec2 = Recorder::default();
    let mut c = ObserveCall::new(ManualService::new(Mode::Pending), rec2.clone());
    let mut d = c.clone();
    ready_svc(&mut c).unwrap();
    ready_svc(&mut d).unwrap();
    let mut fc = Box::pin(c.call(())); // id 1 of this allocator
    let mut fd = Box::pin(d.call(())); // id 2 of this allocator
    assert_eq!(poll_once(&mut fc), Poll::Pending);
    assert_eq!(poll_once(&mut fd), Poll::Pending);
    drop(fc);
    assert_eq!(rec2.ids(), vec![1]);
    assert_eq!(rec2.outcomes(), vec![CallOutcome::Canceled]);
    drop(fd);

    let mut ids2 = rec2.ids();
    ids2.sort_unstable();
    assert_eq!(ids2, vec![1, 2]);
    assert_eq!(rec2.count(CallOutcome::Canceled), 2);
    assert_eq!(rec2.observer_invocations.load(Ordering::Relaxed), 2);
}

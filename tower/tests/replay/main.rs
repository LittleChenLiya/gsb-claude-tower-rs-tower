//! Acceptance tests for `tower::retry::replay`, using a fake `Service`
//! that counts calls, `poll_ready` permits and response drops.
//!
//! Run locally with:
//!
//! ```text
//! cargo test -p tower --features retry --test replay
//! ```
#![cfg(feature = "retry")]

use std::future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tower::retry::replay::{BodyPolicy, Replay, ReplayIdentity, ReplayPolicy};
use tower::Service;

/// A body that is either replayable or a non-cloneable stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Body {
    Empty,
    Streaming,
}

type Req = http::Request<Body>;

/// A response whose drops are counted, so tests can prove the
/// previous attempt's response is reclaimed exactly once.
#[derive(Debug)]
struct Resp {
    status: u16,
    drops: Arc<AtomicUsize>,
}

impl Drop for Resp {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// What the fake lower layer actually observed.
#[derive(Clone, Debug, Default)]
struct Counters {
    calls: Arc<AtomicUsize>,
    permits: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Counters {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn permits(&self) -> usize {
        self.permits.load(Ordering::SeqCst)
    }

    fn drops(&self) -> usize {
        self.drops.load(Ordering::SeqCst)
    }

    fn seen(&self) -> Vec<String> {
        self.seen.lock().unwrap().clone()
    }
}

/// The fake inner service: counts `poll_ready` permits and `call`s,
/// records the targets it saw, and plays back a scripted result per
/// call.
#[derive(Clone, Debug)]
struct Fake {
    script: Arc<Vec<Result<u16, &'static str>>>,
    counters: Counters,
}

impl Fake {
    fn new(script: Vec<Result<u16, &'static str>>) -> (Self, Counters) {
        let counters = Counters::default();
        let fake = Fake {
            script: Arc::new(script),
            counters: counters.clone(),
        };
        (fake, counters)
    }
}

impl Service<Req> for Fake {
    type Response = Resp;
    type Error = &'static str;
    type Future = future::Ready<Result<Resp, &'static str>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.counters.permits.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.counters
            .seen
            .lock()
            .unwrap()
            .push(req.uri().path().to_string());
        let n = self.counters.calls.fetch_add(1, Ordering::SeqCst);
        let step = self.script[n.min(self.script.len() - 1)];
        let drops = self.counters.drops.clone();
        future::ready(step.map(|status| Resp { status, drops }))
    }
}

/// How often the policy was consulted.
#[derive(Clone, Debug, Default)]
struct PolicyCalls {
    clone: Arc<AtomicUsize>,
    retry: Arc<AtomicUsize>,
}

/// A policy that retries failures up to `remaining` times.
///
/// When `tamper` is set, the clone produced for replaying points at
/// a different target — an illegal replay the middleware must
/// reject before calling the inner service.
#[derive(Clone, Debug)]
struct TestPolicy {
    remaining: usize,
    tamper: bool,
    calls: PolicyCalls,
}

fn policy(remaining: usize, tamper: bool) -> (TestPolicy, PolicyCalls) {
    let calls = PolicyCalls::default();
    let policy = TestPolicy {
        remaining,
        tamper,
        calls: calls.clone(),
    };
    (policy, calls)
}

impl ReplayPolicy<Req, Resp, &'static str> for TestPolicy {
    type Future = future::Ready<()>;

    fn seal(&mut self, req: &Req) -> ReplayIdentity {
        let body = match req.body() {
            Body::Empty => BodyPolicy::Cloneable,
            Body::Streaming => BodyPolicy::Streaming,
        };
        // Only the method, the target and the body policy are
        // sealed. Headers — in particular the `authorization`
        // secret header — are never part of the identity.
        ReplayIdentity::new(req.method().as_str(), req.uri().path(), body)
    }

    fn retry(
        &mut self,
        _req: &mut Req,
        result: &mut Result<Resp, &'static str>,
    ) -> Option<Self::Future> {
        self.calls.retry.fetch_add(1, Ordering::SeqCst);
        let failed = match result {
            Ok(resp) => resp.status >= 500,
            Err(_) => true,
        };
        if failed && self.remaining > 0 {
            self.remaining -= 1;
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        self.calls.clone.fetch_add(1, Ordering::SeqCst);
        match req.body() {
            Body::Streaming => None,
            Body::Empty => {
                let target = if self.tamper {
                    "/other"
                } else {
                    req.uri().path()
                };
                let mut builder = http::Request::builder()
                    .method(req.method().clone())
                    .uri(target);
                for (name, value) in req.headers() {
                    builder = builder.header(name, value);
                }
                Some(builder.body(Body::Empty).unwrap())
            }
        }
    }
}

/// A request carrying a secret `authorization` header, which must
/// never leak into the replay identity.
fn req(target: &str, body: Body) -> Req {
    http::Request::builder()
        .method("GET")
        .uri(target)
        .header("authorization", "Bearer s3cret")
        .body(body)
        .unwrap()
}

fn run(policy: TestPolicy, fake: Fake, req: Req) -> Result<Resp, &'static str> {
    let mut svc = Replay::new(policy, fake);
    tokio_test::block_on(async {
        futures::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
        svc.call(req).await
    })
}

#[test]
fn tampered_replay_is_rejected_before_call() {
    // First attempt fails; the clone for the second attempt points
    // at a different target and must be rejected before the call.
    let (fake, counters) = Fake::new(vec![Err("boom")]);
    let (pol, _) = policy(1, true);

    let result = run(pol, fake, req("/a", Body::Empty));

    // The first failure is returned as-is...
    assert_eq!(result.unwrap_err(), "boom");
    // ...the lower layer saw exactly one request, at the original
    // target...
    assert_eq!(counters.calls(), 1);
    assert_eq!(counters.seen(), vec!["/a".to_string()]);
    // ...and no permit was acquired for the rejected replay.
    assert_eq!(counters.permits(), 1);
}

#[test]
fn legal_clone_is_retried() {
    let (fake, counters) = Fake::new(vec![Err("boom"), Ok(200)]);
    let (pol, _) = policy(1, false);

    let resp = run(pol, fake, req("/a", Body::Empty)).unwrap();

    assert_eq!(resp.status, 200);
    // Only the legal clone reached the lower layer...
    assert_eq!(counters.calls(), 2);
    assert_eq!(counters.seen(), vec!["/a".to_string(), "/a".to_string()]);
    // ...with one permit per actual attempt.
    assert_eq!(counters.permits(), 2);
}

#[test]
fn streaming_body_terminates_explicitly() {
    let (fake, counters) = Fake::new(vec![Err("boom")]);
    let (pol, pc) = policy(1, false);

    let result = run(pol, fake, req("/a", Body::Streaming));

    assert_eq!(result.unwrap_err(), "boom");
    // A streaming body cannot be cloned: the failure terminates
    // without consulting `retry` or attempting a clone.
    assert_eq!(counters.calls(), 1);
    assert_eq!(pc.clone.load(Ordering::SeqCst), 0);
    assert_eq!(pc.retry.load(Ordering::SeqCst), 0);
}

#[test]
fn previous_response_is_reclaimed_exactly_once() {
    // The first attempt "fails" with a 500 response, the replay
    // succeeds.
    let (fake, counters) = Fake::new(vec![Ok(500), Ok(200)]);
    let (pol, _) = policy(1, false);

    let resp = run(pol, fake, req("/a", Body::Empty)).unwrap();

    assert_eq!(resp.status, 200);
    // The 500 response was owned by the future and reclaimed
    // exactly once when the new response arrived.
    assert_eq!(counters.drops(), 1);
    drop(resp);
    assert_eq!(counters.drops(), 2);
}

#[test]
fn rejected_replay_returns_last_response_once() {
    // The replay is rejected, so the failed response is handed back
    // to the caller instead of being dropped inside the future.
    let (fake, counters) = Fake::new(vec![Ok(500)]);
    let (pol, _) = policy(1, true);

    let resp = run(pol, fake, req("/a", Body::Empty)).unwrap();

    assert_eq!(resp.status, 500);
    assert_eq!(counters.calls(), 1);
    assert_eq!(counters.drops(), 0);
    drop(resp);
    assert_eq!(counters.drops(), 1);
}

#[test]
fn identity_never_contains_secret_headers() {
    let (mut pol, _) = policy(0, false);

    let identity = pol.seal(&req("/a", Body::Empty));

    assert_eq!(
        identity,
        ReplayIdentity::new("GET", "/a", BodyPolicy::Cloneable)
    );
    assert!(!identity.leaks("s3cret"));
    assert!(!identity.leaks("Bearer"));
    assert!(!identity.leaks("authorization"));
}

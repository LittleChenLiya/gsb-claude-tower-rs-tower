#![cfg(feature = "retry")]
#[path = "../support.rs"]
mod support;

use std::future;

use tokio_test::{assert_pending, assert_ready_err, assert_ready_ok, task};
use tower::retry::Policy;
use tower_test::{assert_request_eq, mock};

#[tokio::test(flavor = "current_thread")]
async fn retry_errors() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(RetryErrors);

    assert_ready_ok!(service.poll_ready());

    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry me");

    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_response("world");

    assert_eq!(fut.into_inner().await.unwrap(), "world");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_limit() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(Limit(2));

    assert_ready_ok!(service.poll_ready());

    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry 1");
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error("retry 2");
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error("retry 3");
    assert_eq!(assert_ready_err!(fut.poll()).to_string(), "retry 3");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_error_inspection() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(UnlessErr("reject"));

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry 1");
    assert_pending!(fut.poll());

    assert_request_eq!(handle, "hello").send_error("reject");
    assert_eq!(assert_ready_err!(fut.poll()).to_string(), "reject");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_cannot_clone_request() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(CannotClone);

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_error("retry 1");
    assert_eq!(assert_ready_err!(fut.poll()).to_string(), "retry 1");
}

#[tokio::test(flavor = "current_thread")]
async fn success_with_cannot_clone() {
    let _t = support::trace_init();

    // Even though the request couldn't be cloned, if the first request succeeds,
    // it should succeed overall.
    let (mut service, mut handle) = new_service(CannotClone);

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_response("world");
    assert_ready_ok!(fut.poll(), "world");
}

#[tokio::test(flavor = "current_thread")]
async fn retry_mutating_policy() {
    let _t = support::trace_init();

    let (mut service, mut handle) = new_service(MutatingPolicy { remaining: 2 });

    assert_ready_ok!(service.poll_ready());
    let mut fut = task::spawn(service.call("hello"));

    assert_request_eq!(handle, "hello").send_response("world");
    assert_pending!(fut.poll());
    // the policy alters the request. in real life, this might be setting a header
    assert_request_eq!(handle, "retrying").send_response("world");

    assert_pending!(fut.poll());

    assert_request_eq!(handle, "retrying").send_response("world");

    assert_ready_err!(fut.poll(), "out of retries");
}

type Req = &'static str;
type Res = &'static str;
type InnerError = &'static str;
type Error = Box<dyn std::error::Error + Send + Sync>;
type Mock = mock::Mock<Req, Res>;
type Handle = mock::Handle<Req, Res>;

#[derive(Clone)]
struct RetryErrors;

impl Policy<Req, Res, Error> for RetryErrors {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        if result.is_err() {
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

#[derive(Clone)]
struct Limit(usize);

impl Policy<Req, Res, Error> for Limit {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        if result.is_err() && self.0 > 0 {
            self.0 -= 1;
            Some(future::ready(()))
        } else {
            None
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

#[derive(Clone)]
struct UnlessErr(InnerError);

impl Policy<Req, Res, Error> for UnlessErr {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        result.as_ref().err().and_then(|err| {
            if err.to_string() != self.0 {
                Some(future::ready(()))
            } else {
                None
            }
        })
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

#[derive(Clone)]
struct CannotClone;

impl Policy<Req, Res, Error> for CannotClone {
    type Future = future::Ready<()>;
    fn retry(&mut self, _: &mut Req, _: &mut Result<Res, Error>) -> Option<Self::Future> {
        unreachable!("retry cannot be called since request isn't cloned");
    }

    fn clone_request(&mut self, _req: &Req) -> Option<Req> {
        None
    }
}

/// Test policy that changes the request to `retrying` during retries and the result to `"out of retries"`
/// when retries are exhausted.
#[derive(Clone)]
struct MutatingPolicy {
    remaining: usize,
}

impl Policy<Req, Res, Error> for MutatingPolicy
where
    Error: From<&'static str>,
{
    type Future = future::Ready<()>;

    fn retry(&mut self, req: &mut Req, result: &mut Result<Res, Error>) -> Option<Self::Future> {
        if self.remaining == 0 {
            *result = Err("out of retries".into());
            None
        } else {
            *req = "retrying";
            self.remaining -= 1;
            Some(future::ready(()))
        }
    }

    fn clone_request(&mut self, req: &Req) -> Option<Req> {
        Some(*req)
    }
}

fn new_service<P: Policy<Req, Res, Error> + Clone>(
    policy: P,
) -> (mock::Spawn<tower::retry::Retry<P, Mock>>, Handle) {
    let retry = tower::retry::RetryLayer::new(policy);
    mock::spawn_layer(retry)
}

// Acceptance tests for the sealed replay identity carried by `Policy::replay_id`.
//
// A fake `Service` (optionally wrapped in the real `ConcurrencyLimit` layer)
// is used instead of a mock that would allow a rejected replay to slip past:
// these tests assert that the lower layer is or is not called, that the old
// response is dropped exactly once, and that permits are really released.
mod replay_identity {
    use super::support;
    use std::future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[cfg(feature = "limit")]
    use tower::retry::RetryLayer;
    use tower::retry::{Policy, ReplayId, Retry};
    #[cfg(feature = "limit")]
    use tower::{limit::ConcurrencyLimitLayer, ServiceBuilder};
    use tower::{Service, ServiceExt};

    #[derive(Clone, Debug, PartialEq)]
    struct RReq {
        method: &'static str,
        target: &'static str,
        body: &'static str,
        // Deliberately NOT part of the sealed identity.
        secret: String,
    }

    #[derive(Debug)]
    struct RResp {
        id: usize,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for RResp {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct RErr {
        name: &'static str,
        drops: Arc<AtomicUsize>,
    }

    impl RErr {
        fn new(name: &'static str, drops: Arc<AtomicUsize>) -> Self {
            RErr { name, drops }
        }
    }

    impl Drop for RErr {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl std::fmt::Display for RErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.name)
        }
    }

    impl std::error::Error for RErr {}

    #[derive(Clone, Debug)]
    struct Seen {
        target: &'static str,
        secret: String,
    }

    /// Fake lower service: the first call fails, every later call succeeds.
    /// It records every request that actually reached it.
    #[derive(Clone)]
    struct Fake {
        calls: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<Seen>>>,
        err_drops: Arc<AtomicUsize>,
        ok_drops: Arc<AtomicUsize>,
    }

    impl Fake {
        fn new() -> Fake {
            Fake {
                calls: Arc::new(AtomicUsize::new(0)),
                seen: Arc::new(Mutex::new(Vec::new())),
                err_drops: Arc::new(AtomicUsize::new(0)),
                ok_drops: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn seen(&self) -> Vec<Seen> {
            self.seen.lock().unwrap().clone()
        }
    }

    impl Service<RReq> for Fake {
        type Response = RResp;
        type Error = RErr;
        type Future = future::Ready<Result<RResp, RErr>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: RReq) -> Self::Future {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.seen.lock().unwrap().push(Seen {
                target: req.target,
                secret: req.secret.clone(),
            });
            if n == 1 {
                future::ready(Err(RErr::new("first", self.err_drops.clone())))
            } else {
                future::ready(Ok(RResp {
                    id: n,
                    drops: self.ok_drops.clone(),
                }))
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Mode {
        /// Every replay clone changes the target: the identity gate must
        /// refuse the replay before the second call.
        ChangeTarget,
        /// Faithful clone, but the per-attempt secret header is rotated:
        /// the replay must be admitted because headers are not sealed.
        LegalRotateSecret,
        /// One-shot streaming body that cannot be cloned: retrying must
        /// terminate explicitly (`clone_request` returns `None`).
        Stream,
    }

    #[derive(Clone)]
    struct GatePolicy {
        mode: Mode,
        retry_votes: Arc<AtomicUsize>,
    }

    impl GatePolicy {
        fn new(mode: Mode) -> Self {
            GatePolicy {
                mode,
                retry_votes: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Policy<RReq, RResp, RErr> for GatePolicy {
        type Future = future::Ready<()>;

        fn retry(
            &mut self,
            _: &mut RReq,
            result: &mut Result<RResp, RErr>,
        ) -> Option<Self::Future> {
            self.retry_votes.fetch_add(1, Ordering::SeqCst);
            if result.is_err() {
                Some(future::ready(()))
            } else {
                None
            }
        }

        fn clone_request(&mut self, req: &RReq) -> Option<RReq> {
            match self.mode {
                Mode::ChangeTarget => Some(RReq {
                    method: req.method,
                    target: "/evil",
                    body: req.body,
                    secret: req.secret.clone(),
                }),
                Mode::LegalRotateSecret => Some(RReq {
                    method: req.method,
                    target: req.target,
                    body: req.body,
                    // A refreshed credential must not invalidate the replay.
                    secret: "rotated".to_owned(),
                }),
                Mode::Stream => None,
            }
        }

        fn replay_id(&mut self, req: &RReq) -> ReplayId {
            // Method, target and body strategy only: the secret header is
            // deliberately absent from the sealed identity.
            ReplayId::new(req.method, req.target, req.body)
        }
    }

    fn base_req() -> RReq {
        RReq {
            method: "POST",
            target: "/a",
            body: "buffered",
            secret: "secret-1".to_owned(),
        }
    }

    async fn drive<S>(svc: &mut S, req: RReq) -> Result<RResp, RErr>
    where
        S: Service<RReq, Response = RResp, Error = RErr> + Unpin,
    {
        svc.ready().await.unwrap();
        svc.call(req).await
    }

    /// First call fails. The policy votes to retry and produces a clone, but
    /// the clone changed the target, so it is rejected *before* the second
    /// call: the lower layer runs exactly once and the original error is
    /// returned.
    #[tokio::test(flavor = "current_thread")]
    async fn rejects_changed_target_before_second_call() {
        let _t = support::trace_init();

        let policy = GatePolicy::new(Mode::ChangeTarget);
        let votes = policy.retry_votes.clone();
        let fake = Fake::new();
        let calls = fake.calls.clone();
        let seen = fake.seen.clone();
        let err_drops = fake.err_drops.clone();
        let mut svc = Retry::new(policy, fake);

        let result = tokio::time::timeout(Duration::from_secs(5), drive(&mut svc, base_req()))
            .await
            .expect("retry must not hang");

        // The policy did vote for a retry; the identity gate overrode it.
        assert_eq!(votes.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "lower layer called once");
        assert_eq!(seen.lock().unwrap().len(), 1);

        let err = result.expect_err("rejected replay returns the first error");
        assert_eq!(err.name, "first");
        drop(err);
        // The old response is handed back and cleaned up exactly once.
        assert_eq!(err_drops.load(Ordering::SeqCst), 1);
    }

    /// A faithful clone is admitted and retried; the rotated secret header
    /// reaches the lower layer because it is not part of the identity.
    #[tokio::test(flavor = "current_thread")]
    async fn legal_clone_is_retried() {
        let _t = support::trace_init();

        let policy = GatePolicy::new(Mode::LegalRotateSecret);
        let fake = Fake::new();
        let calls = fake.calls.clone();
        let err_drops = fake.err_drops.clone();
        let mut svc = Retry::new(policy, fake);

        let resp = tokio::time::timeout(Duration::from_secs(5), drive(&mut svc, base_req()))
            .await
            .expect("retry must not hang")
            .expect("second call succeeds");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(resp.id, 2);

        let seen = svc.get_ref().seen();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].target, "/a");
        assert_eq!(seen[0].secret, "secret-1");
        assert_eq!(seen[1].target, "/a");
        assert_eq!(seen[1].secret, "rotated");
        // The failed first response was dropped exactly once when the replay
        // was committed; it was never returned to the caller.
        assert_eq!(err_drops.load(Ordering::SeqCst), 1);
    }

    /// A non-cloneable streaming body terminates the retry: the retry
    /// function is never even consulted and the lower layer is called once.
    #[tokio::test(flavor = "current_thread")]
    async fn streaming_body_terminates_without_replay() {
        let _t = support::trace_init();

        let policy = GatePolicy::new(Mode::Stream);
        let votes = policy.retry_votes.clone();
        let fake = Fake::new();
        let calls = fake.calls.clone();
        let err_drops = fake.err_drops.clone();
        let mut svc = Retry::new(policy, fake);

        let req = RReq {
            body: "stream",
            ..base_req()
        };
        let result = tokio::time::timeout(Duration::from_secs(5), drive(&mut svc, req))
            .await
            .expect("must not hang")
            .expect_err("first failure is returned");

        assert_eq!(result.name, "first");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(votes.load(Ordering::SeqCst), 0, "retry never consulted");
        drop(result);
        assert_eq!(err_drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn identity_covers_method_target_body_but_not_secret() {
        let mut p = GatePolicy::new(Mode::LegalRotateSecret);

        let only_secret_differs = (
            RReq {
                secret: "a".to_owned(),
                ..base_req()
            },
            RReq {
                secret: "b".to_owned(),
                ..base_req()
            },
        );
        assert_eq!(
            p.replay_id(&only_secret_differs.0),
            p.replay_id(&only_secret_differs.1),
            "secret header must not be part of the replay identity",
        );

        assert_ne!(
            p.replay_id(&base_req()),
            p.replay_id(&RReq {
                target: "/b",
                ..base_req()
            }),
        );
        assert_ne!(
            p.replay_id(&base_req()),
            p.replay_id(&RReq {
                method: "GET",
                ..base_req()
            }),
        );
        assert_ne!(
            p.replay_id(&base_req()),
            p.replay_id(&RReq {
                body: "stream",
                ..base_req()
            }),
        );
    }

    /// With a concurrency limit of one, a refused replay must release the
    /// permit held by the first attempt; otherwise the next request could
    /// never start.
    #[cfg(feature = "limit")]
    #[tokio::test(flavor = "current_thread")]
    async fn rejected_replay_releases_permit() {
        let _t = support::trace_init();

        let fake = Fake::new();
        let calls = fake.calls.clone();
        let mut svc = ServiceBuilder::new()
            .layer(RetryLayer::new(GatePolicy::new(Mode::ChangeTarget)))
            .layer(ConcurrencyLimitLayer::new(1))
            .service(fake);

        let first = tokio::time::timeout(Duration::from_secs(5), drive(&mut svc, base_req()))
            .await
            .expect("must not hang")
            .expect_err("replay rejected");
        assert_eq!(first.name, "first");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(first);

        // A second, unrelated request must acquire the single permit. This
        // hangs (and trips the timeout) if the rejected replay leaked it.
        let second = tokio::time::timeout(Duration::from_secs(5), drive(&mut svc, base_req()))
            .await
            .expect("permit must be reclaimed")
            .expect("next request succeeds");
        assert_eq!(second.id, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// The legal replay must itself reacquire the single permit, which is
    /// only possible if the first attempt released it.
    #[cfg(feature = "limit")]
    #[tokio::test(flavor = "current_thread")]
    async fn legal_replay_reacquires_single_permit() {
        let _t = support::trace_init();

        let fake = Fake::new();
        let calls = fake.calls.clone();
        let mut svc = ServiceBuilder::new()
            .layer(RetryLayer::new(GatePolicy::new(Mode::LegalRotateSecret)))
            .layer(ConcurrencyLimitLayer::new(1))
            .service(fake);

        let resp = tokio::time::timeout(Duration::from_secs(5), drive(&mut svc, base_req()))
            .await
            .expect("must not hang")
            .expect("retry succeeds under limit 1");
        assert_eq!(resp.id, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

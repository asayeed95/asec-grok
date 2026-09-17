//! When the server that hosts a session ends (AGE-2131 #2).
//!
//! At afbc0fb `--timeout` and `--linger` were stored on the session and read by nothing,
//! and `axum::serve` had no shutdown wiring: every server outlived its child forever,
//! and the process that spawned ptyctl was the only lifetime control. Now:
//!
//! - the child exits → unless `linger`, the server stays up for the exit grace
//!   (`--timeout`, default [`DEFAULT_EXIT_GRACE`]) so clients can read the final screen,
//!   status and exit code, then shuts down;
//! - `--linger` → the server stays up after the child exits until `/control/stop`
//!   (or a signal to the process); a stop then ends it after the same grace;
//! - shutting down flips the session's `shutting_down` watch first, so `/ws` handlers
//!   send their `closed` frame and drop their sockets, and the graceful drain completes.
//!   A drain that does not complete within [`DRAIN_BACKSTOP`] is abandoned: the server
//!   returns anyway, and the process exit cuts what is left.

use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::session::Lifecycle;

/// Seconds the server stays up after the child exits when `--timeout` is not given.
pub const DEFAULT_EXIT_GRACE_SECS: u64 = 5;
/// The same, as a duration.
pub const DEFAULT_EXIT_GRACE: Duration = Duration::from_secs(DEFAULT_EXIT_GRACE_SECS);
/// How long a graceful drain may take after the shutdown signal before it is abandoned.
pub const DRAIN_BACKSTOP: Duration = Duration::from_secs(3);

/// The server's shutdown policy, from the CLI flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownPolicy {
    pub linger: bool,
    pub exit_grace: Duration,
}

impl ShutdownPolicy {
    /// From the session's recorded `--timeout` / `--linger`.
    pub fn from_lifecycle(lc: &Lifecycle) -> Self {
        Self {
            linger: lc.linger,
            exit_grace: lc
                .timeout
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_EXIT_GRACE),
        }
    }
}

async fn wait_true(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            // The sender is gone: nothing will ever flip it. Treat as "now" so a
            // dropped session cannot pin the server open forever.
            return;
        }
    }
}

/// Resolves when the server should stop accepting and drain, per the policy; flips the
/// session's `shutting_down` watch just before resolving.
pub async fn shutdown_signal(lc: Lifecycle, policy: ShutdownPolicy) {
    // Either the child exits, or a stop is requested (a stop also ends the child, but a
    // stop on an already-exited lingering session must still end the server).
    tokio::select! {
        _ = wait_true(lc.exited.clone()) => {}
        _ = wait_true(lc.stop_requested.clone()) => {}
    }
    let stop_requested = *lc.stop_requested.borrow();
    if policy.linger && !stop_requested {
        wait_true(lc.stop_requested.clone()).await;
    }
    tokio::time::sleep(policy.exit_grace).await;
    let _ = lc.shutting_down_tx.send(true);
}

/// Serve `router` on `listener` until the policy says stop, drain bounded by
/// [`DRAIN_BACKSTOP`], then return.
pub async fn serve_until_done(
    listener: TcpListener,
    router: Router,
    lc: Lifecycle,
    policy: ShutdownPolicy,
) -> Result<()> {
    let (fired_tx, fired_rx) = watch::channel::<bool>(false);
    let serve = axum::serve(listener, router).with_graceful_shutdown(async move {
        shutdown_signal(lc, policy).await;
        let _ = fired_tx.send(true);
    });
    let backstop = async move {
        wait_true(fired_rx).await;
        tokio::time::sleep(DRAIN_BACKSTOP).await;
    };
    tokio::select! {
        result = serve => result.context("HTTP server error"),
        _ = backstop => {
            log::warn!("graceful drain did not complete within {DRAIN_BACKSTOP:?}; ending the server anyway");
            Ok(())
        }
    }
}

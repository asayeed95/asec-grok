//! Core PTY session — ties PTY + Terminal + I/O channels together.

use std::collections::VecDeque;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, broadcast, mpsc, watch};

use crate::keys;
use crate::pty::{PtyChild, PtyConfig, PtyHandle, PtyMaster};
use crate::styled::StyledLine;
use crate::term::{
    CursorPosition, ScreenOpts, ScreenOutput, ScrollbackLine, SessionListener, Terminal,
    TerminalModes,
};
// Re-exported so existing `session::Wait*` paths keep working after the wait-module extraction.
use crate::wait::{RAW_TAIL_CAP, push_raw_tail};
pub use crate::wait::{WaitCondition, WaitDiagnostics, WaitHandle, WaitOutcome};

/// Configuration for starting a session.
///
/// `timeout` and `linger` describe the SERVER's lifetime around the child's exit; the
/// session records them, and `crate::lifecycle` reads them (AGE-2131 #2 — at afbc0fb
/// both were stored and never read, so every server outlived its child forever).
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub pty: PtyConfig,
    /// Seconds the server stays up after the child exits so clients can read the final
    /// screen, status and exit code (None = the default grace, `lifecycle::DEFAULT_EXIT_GRACE`).
    pub timeout: Option<u64>,
    /// Keep the server running after the child exits, until `/control/stop` or a signal.
    pub linger: bool,
}

/// How long `stop()` gives the child after SIGTERM before SIGKILL.
pub const DEFAULT_STOP_GRACE: Duration = Duration::from_secs(2);

/// What `stop()` did and what it found (AGE-2131 #1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StopOutcome {
    /// The strongest signal that was sent: `"none"` (already exited), `"SIGTERM"`, or `"SIGKILL"`.
    pub signalled: &'static str,
    /// True once the waiter reaped the child (the exit code is then on `/query/status`).
    pub exited: bool,
}

/// The handles `stop()` needs, cloned out of the session so the stop sequence — up to
/// two grace periods long — never holds the server's session lock (the same reason
/// `wait_handle()` exists).
#[derive(Clone)]
pub struct StopHandle {
    pid: Option<u32>,
    child: Arc<std::sync::Mutex<PtyChild>>,
    exited_rx: watch::Receiver<bool>,
    stop_requested_tx: Arc<watch::Sender<bool>>,
}

/// The lifecycle facts a server needs to decide when to end (read by `crate::lifecycle`).
#[derive(Clone)]
pub struct Lifecycle {
    /// Flips to `true` once the child has been reaped and its exit code recorded.
    pub exited: watch::Receiver<bool>,
    /// Flips to `true` once `stop()` has been requested.
    pub stop_requested: watch::Receiver<bool>,
    /// Set by the server when it begins shutting down; WebSocket handlers watch it.
    pub shutting_down_tx: Arc<watch::Sender<bool>>,
    pub shutting_down: watch::Receiver<bool>,
    pub linger: bool,
    pub timeout: Option<u64>,
}

/// Status of a PTY session.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SessionStatus {
    pub alive: bool,
    pub pid: Option<u32>,
    pub exit_code: Option<u32>,
    pub size: (u16, u16),
    pub modes: TerminalModes,
    pub scrollback_lines: usize,
}

/// A running PTY session.
pub struct PtySession {
    terminal: Arc<Mutex<Terminal>>,
    /// Master half of the PTY, kept here so resize reaches the real PTY.
    master: PtyMaster,
    pty_write_tx: mpsc::UnboundedSender<Vec<u8>>,
    /// False only after the waiter has reaped the child and stored its exit code — the
    /// reader thread no longer touches it (AGE-2131 #3).
    alive: Arc<AtomicBool>,
    exit_code: Arc<std::sync::Mutex<Option<u32>>>,
    pid: Option<u32>,
    /// The child, shared with the waiter task under one lock so `stop()` can signal it
    /// and never signals a pid the waiter has already reaped (pid reuse).
    child: Arc<std::sync::Mutex<PtyChild>>,
    exited_rx: watch::Receiver<bool>,
    stop_requested_tx: Arc<watch::Sender<bool>>,
    stop_requested_rx: watch::Receiver<bool>,
    shutting_down_tx: Arc<watch::Sender<bool>>,
    shutting_down_rx: watch::Receiver<bool>,
    linger: bool,
    timeout: Option<u64>,
    /// Grid generation counter, bumped by the feeder after each `term.feed()`.
    generation_rx: watch::Receiver<u64>,
    /// Weak so the feeder's exit still drops the sender, signalling "ended" to waiters;
    /// lets `resize` bump the generation too (a resize changes the grid without output).
    generation_tx: Weak<watch::Sender<u64>>,
    /// Last [`RAW_TAIL_CAP`] bytes of raw PTY output for wait-timeout diagnostics.
    raw_tail: Arc<std::sync::Mutex<VecDeque<u8>>>,
    /// Broadcast channel for real-time PTY output streaming (WebSocket).
    output_tx: broadcast::Sender<Vec<u8>>,
}

impl PtySession {
    /// Start a new PTY session.
    pub async fn start(config: SessionConfig) -> Result<Self> {
        let cols = config.pty.cols;
        let rows = config.pty.rows;

        // Spawn the PTY process; keep the master half for resize. The child is shared
        // between the waiter task (reaps it) and `stop()` (signals it) under one lock.
        let pty = PtyHandle::spawn(&config.pty).context("failed to spawn PTY")?;
        let (master, child, mut reader, mut writer) = pty.into_parts();
        let pid = child.pid();
        let child = Arc::new(std::sync::Mutex::new(child));

        // Channel for terminal-generated PtyWrite responses.
        let (pty_response_tx, mut pty_response_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // Channel for user-initiated writes (send_keys, send_bytes).
        let (pty_write_tx, mut pty_write_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // Channel for PTY reader -> terminal feeder.
        let (pty_read_tx, mut pty_read_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // Broadcast channel for WebSocket streaming (capacity: 256 chunks).
        let (output_tx, _) = broadcast::channel::<Vec<u8>>(256);

        // Grid-generation watch for event-driven waits (watch over Notify: check-then-wait is lost-wakeup-free).
        // The feeder holds the only strong Arc so its exit still drops the sender ("ended" signal).
        let (generation_tx, generation_rx) = watch::channel::<u64>(0);
        let generation_tx = Arc::new(generation_tx);
        let generation_tx_weak = Arc::downgrade(&generation_tx);
        let raw_tail: Arc<std::sync::Mutex<VecDeque<u8>>> =
            Arc::new(std::sync::Mutex::new(VecDeque::with_capacity(RAW_TAIL_CAP)));

        // Lifecycle signals: child exited (reaped), stop requested, server shutting down.
        let (exited_tx, exited_rx) = watch::channel::<bool>(false);
        let (stop_requested_tx, stop_requested_rx) = watch::channel::<bool>(false);
        let (shutting_down_tx, shutting_down_rx) = watch::channel::<bool>(false);

        // Create the terminal.
        let listener = SessionListener::new(pty_response_tx);
        let terminal = Arc::new(Mutex::new(Terminal::new(cols, rows, listener)));
        let alive = Arc::new(AtomicBool::new(true));
        let exit_code: Arc<std::sync::Mutex<Option<u32>>> = Arc::new(std::sync::Mutex::new(None));

        // --- PTY Reader Thread (blocking) ---
        // Ends at EOF/error and says nothing about `alive`: at afbc0fb it stored
        // `alive=false` here, the waiter broke on that before calling `wait()`, and the
        // exit code was lost whenever EOF beat the waiter's 100 ms tick (AGE-2131 #3).
        std::thread::Builder::new()
            .name("pty-reader".into())
            .spawn(move || {
                let mut buf = [0u8; 65536];
                loop {
                    match reader.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            if pty_read_tx.send(buf[..n].to_vec()).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::WouldBlock {
                                log::debug!("PTY read error: {e}");
                                break;
                            }
                        }
                    }
                }
            })
            .context("failed to spawn PTY reader thread")?;

        // --- PTY Writer Task (async) ---
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(bytes) = pty_write_rx.recv() => {
                        if let Err(e) = std::io::Write::write_all(&mut writer, &bytes) {
                            log::debug!("PTY write error: {e}");
                            break;
                        }
                        let _ = std::io::Write::flush(&mut writer);
                    }
                    Some(bytes) = pty_response_rx.recv() => {
                        if let Err(e) = std::io::Write::write_all(&mut writer, &bytes) {
                            log::debug!("PTY response write error: {e}");
                            break;
                        }
                        let _ = std::io::Write::flush(&mut writer);
                    }
                    else => break,
                }
            }
        });

        // --- Terminal Feeder Task (async) ---
        let terminal_feeder = terminal.clone();
        let output_tx_feeder = output_tx.clone();
        let raw_tail_feeder = raw_tail.clone();
        tokio::spawn(async move {
            while let Some(bytes) = pty_read_rx.recv().await {
                // Broadcast raw PTY output to all WebSocket subscribers.
                // Ignore errors (no active subscribers is fine).
                let _ = output_tx_feeder.send(bytes.clone());

                push_raw_tail(&raw_tail_feeder, &bytes);

                {
                    let mut term = terminal_feeder.lock().await;
                    term.feed(&bytes);
                }
                // Bump only after term.feed(): a waiter woken by the broadcast above would read a stale grid.
                generation_tx.send_modify(|g| *g += 1);
            }
        });

        // --- Child Process Waiter: the ONE place `alive` flips ---
        // Reaps with a non-blocking try_wait under the child lock; stores the exit code
        // BEFORE clearing `alive`, so `/query/status` can never show alive=false with
        // exit_code=null (AGE-2131 #3), then signals `exited` for the lifecycle.
        let alive_waiter = alive.clone();
        let exit_code_waiter = exit_code.clone();
        let child_waiter = child.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let reaped = child_waiter.lock().unwrap().try_wait_code();
                if let Some(code) = reaped {
                    *exit_code_waiter.lock().unwrap() = Some(code);
                    alive_waiter.store(false, Ordering::SeqCst);
                    let _ = exited_tx.send(true);
                    break;
                }
            }
        });

        Ok(Self {
            terminal,
            master,
            pty_write_tx,
            alive,
            exit_code,
            pid,
            child,
            exited_rx,
            stop_requested_tx: Arc::new(stop_requested_tx),
            stop_requested_rx,
            shutting_down_tx: Arc::new(shutting_down_tx),
            shutting_down_rx,
            linger: config.linger,
            timeout: config.timeout,
            generation_rx,
            generation_tx: generation_tx_weak,
            raw_tail,
            output_tx,
        })
    }

    /// Send keystrokes using vim notation (e.g. `"<C-c>"`, `"hello<CR>"`).
    pub async fn send_keys(&self, notation: &str) -> Result<()> {
        let bytes = keys::parse_keys(notation)?;
        self.send_bytes(&bytes).await
    }

    /// Send raw bytes to the PTY.
    pub async fn send_bytes(&self, bytes: &[u8]) -> Result<()> {
        self.pty_write_tx
            .send(bytes.to_vec())
            .map_err(|_| anyhow::anyhow!("PTY write channel closed"))
    }

    /// Read screen content as plain text.
    pub async fn screen(&self, opts: &ScreenOpts) -> ScreenOutput {
        let term = self.terminal.lock().await;
        term.screen_content(opts)
    }

    /// Read screen content with style information.
    pub async fn screen_styled(&self, opts: &ScreenOpts) -> Vec<StyledLine> {
        let term = self.terminal.lock().await;
        term.screen_styled(opts)
    }

    /// Read screen content as HTML.
    pub async fn screen_html(&self, opts: &ScreenOpts) -> String {
        let term = self.terminal.lock().await;
        term.screen_html(opts)
    }

    /// Get cursor position (1-indexed).
    pub async fn cursor(&self) -> CursorPosition {
        let term = self.terminal.lock().await;
        term.cursor_position()
    }

    /// Get session status (basic info, no terminal lock needed).
    pub fn status_basic(&self) -> (bool, Option<u32>, Option<u32>) {
        (
            self.alive.load(Ordering::SeqCst),
            self.pid,
            *self.exit_code.lock().unwrap(),
        )
    }

    /// Get full session status including terminal modes.
    pub async fn status(&self) -> SessionStatus {
        let (alive, pid, exit_code) = self.status_basic();
        let term = self.terminal.lock().await;
        // Size comes from the grid under the same lock resize holds, so it can never be torn.
        let size = term.size();
        SessionStatus {
            alive,
            pid,
            exit_code,
            size: (size.cols as u16, size.rows as u16),
            modes: term.terminal_modes(),
            scrollback_lines: term.scrollback_count(),
        }
    }

    /// Resize the real PTY and the terminal grid.
    pub async fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        let mut term = self.terminal.lock().await;
        // PTY first (fail-fast); the held terminal lock keeps the SIGWINCH redraw out of a stale grid.
        self.master.resize(cols, rows)?;
        term.resize(cols, rows);
        // Bump after the grid resize (matching the feeder's post-feed ordering): reflow/clipping
        // changes screen text, so in-flight waits must re-check — and StableMs must restart.
        if let Some(generation_tx) = self.generation_tx.upgrade() {
            generation_tx.send_modify(|g| *g += 1);
        }
        Ok(())
    }

    /// Check if the child process is still alive.
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// Subscribe to real-time PTY output for WebSocket streaming.
    pub fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.output_tx.subscribe()
    }

    /// Read scrollback history lines.
    pub async fn scrollback(&self, count: usize) -> Vec<ScrollbackLine> {
        let term = self.terminal.lock().await;
        term.scrollback_lines(count)
    }

    /// Clone the handles needed to wait without holding any outer session lock.
    pub fn wait_handle(&self) -> WaitHandle {
        WaitHandle {
            terminal: self.terminal.clone(),
            generation_rx: self.generation_rx.clone(),
            raw_tail: self.raw_tail.clone(),
        }
    }

    /// Wait until `condition` is met or `timeout` elapses.
    pub async fn wait_for(
        &self,
        condition: WaitCondition,
        timeout: Duration,
    ) -> Result<WaitOutcome> {
        self.wait_handle().wait_for(condition, timeout).await
    }

    /// The lifecycle facts for the server that hosts this session (`crate::lifecycle`).
    pub fn lifecycle(&self) -> Lifecycle {
        Lifecycle {
            exited: self.exited_rx.clone(),
            stop_requested: self.stop_requested_rx.clone(),
            shutting_down_tx: self.shutting_down_tx.clone(),
            shutting_down: self.shutting_down_rx.clone(),
            linger: self.linger,
            timeout: self.timeout,
        }
    }

    /// A receiver that flips to `true` when the server begins shutting down.
    pub fn shutting_down(&self) -> watch::Receiver<bool> {
        self.shutting_down_rx.clone()
    }

    /// Clone the handles `stop()` needs, so the caller can drop its session lock first.
    pub fn stop_handle(&self) -> StopHandle {
        StopHandle {
            pid: self.pid,
            child: self.child.clone(),
            exited_rx: self.exited_rx.clone(),
            stop_requested_tx: self.stop_requested_tx.clone(),
        }
    }

    /// Stop the session: SIGTERM the child, SIGKILL it after [`DEFAULT_STOP_GRACE`]
    /// if it is still running, and mark the session stop-requested so the hosting
    /// server ends too. At afbc0fb this sent on a channel whose receiver had been
    /// dropped at construction and stopped nothing (AGE-2131 #1).
    pub async fn stop(&mut self) -> Result<StopOutcome> {
        Ok(self.stop_handle().stop(DEFAULT_STOP_GRACE).await)
    }
}

impl StopHandle {
    /// Signal the child only while it is still unreaped, under the child lock: the
    /// waiter reaps under the same lock, so a pid can never be signalled after the
    /// process it named is gone (and the pid possibly reused).
    fn signal_if_running(&self, signal: libc::c_int) -> bool {
        let Some(pid) = self.pid else {
            return false;
        };
        let mut child = self.child.lock().unwrap();
        if !child.is_alive() {
            return false;
        }
        // SAFETY: plain `kill(2)` on a pid this session spawned and has not reaped.
        unsafe { libc::kill(pid as libc::pid_t, signal) == 0 }
    }

    async fn wait_exited(&self, within: Duration) -> bool {
        let mut rx = self.exited_rx.clone();
        let deadline = tokio::time::Instant::now() + within;
        loop {
            if *rx.borrow_and_update() {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if tokio::time::timeout(remaining, rx.changed()).await.is_err() {
                return *rx.borrow();
            }
        }
    }

    /// SIGTERM, then SIGKILL after `grace` if the child has not been reaped by then.
    pub async fn stop(self, grace: Duration) -> StopOutcome {
        let _ = self.stop_requested_tx.send(true);
        let mut signalled = "none";
        if self.signal_if_running(libc::SIGTERM) {
            signalled = "SIGTERM";
            if !self.wait_exited(grace).await && self.signal_if_running(libc::SIGKILL) {
                signalled = "SIGKILL";
            }
        }
        // The waiter's next tick reaps a killed child; give it that tick, bounded.
        let exited = self.wait_exited(grace).await;
        StopOutcome { signalled, exited }
    }
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use super::*;

    /// Start a session running `command` at 80x24.
    pub(crate) async fn start_session(command: Vec<String>) -> PtySession {
        PtySession::start(SessionConfig {
            pty: PtyConfig {
                command,
                cols: 80,
                rows: 24,
                cwd: None,
                env: HashMap::new(),
            },
            timeout: None,
            linger: false,
        })
        .await
        .expect("failed to start session")
    }

    /// Send `keys` then wait (bounded) for the child to exit, so the shell and
    /// reader thread don't outlive the test.
    pub(crate) async fn shutdown(session: &PtySession, keys: &str) {
        session.send_keys(keys).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while session.is_alive() {
            assert!(
                Instant::now() < deadline,
                "child did not exit after {keys:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Poll the screen until `pred` matches, panicking with the last screen on timeout.
    async fn wait_for_screen(session: &PtySession, pred: impl Fn(&str) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let screen = session.screen(&ScreenOpts::default()).await;
            let text = screen.lines.join("\n");
            if pred(&text) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for screen; last screen:\n{text}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// AGE-2131 #3: the exit code must survive the reader/waiter race. `exit 3` ends before
    /// the waiter's first 100 ms tick; at afbc0fb the reader thread's EOF flipped `alive`
    /// first and the waiter `break`-ed on it before ever calling `wait()`, so `exit_code`
    /// stayed `None` on every run. Ten runs, not one: the race is timing, and one green
    /// run proves only that the scheduler was kind once.
    #[tokio::test(flavor = "multi_thread")]
    async fn exit_code_survives_the_reader_waiter_race_ten_times() {
        for run in 0..10 {
            let session = start_session(vec!["/bin/sh".into(), "-c".into(), "exit 3".into()]).await;
            let deadline = Instant::now() + Duration::from_secs(10);
            while session.is_alive() {
                assert!(
                    Instant::now() < deadline,
                    "run {run}: the child never reported exited"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let (alive, _pid, code) = session.status_basic();
            assert!(!alive);
            assert_eq!(
                code,
                Some(3),
                "run {run}: exit code lost (alive=false, exit_code={code:?})"
            );
        }
    }

    /// AGE-2131 #1: `stop()` ends a child that honours SIGTERM, within the grace, and
    /// the exit code is then on the status. At afbc0fb it sent on a dropped receiver.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_ends_a_child_that_honours_sigterm() {
        let mut session = start_session(vec!["/bin/sleep".into(), "30".into()]).await;
        assert!(session.is_alive());
        let started = Instant::now();
        let outcome = session.stop().await.unwrap();
        assert_eq!(outcome.signalled, "SIGTERM", "{outcome:?}");
        assert!(outcome.exited, "{outcome:?}");
        assert!(
            started.elapsed() < DEFAULT_STOP_GRACE,
            "a SIGTERM-honouring child took the whole grace: {:?}",
            started.elapsed()
        );
        let (alive, _pid, code) = session.status_basic();
        assert!(!alive);
        assert!(code.is_some(), "exit code missing after stop");
    }

    /// AGE-2131 #1: a child that ignores SIGTERM is SIGKILLed after the grace, and the
    /// stop reports which signal did it. `exec` so the ignoring process IS the child.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_escalates_to_sigkill_when_sigterm_is_ignored() {
        let session = start_session(vec![
            "/bin/sh".into(),
            "-c".into(),
            "trap '' TERM; exec /bin/sleep 30".into(),
        ])
        .await;
        // Let the shell reach `exec` so the trap disposition is the one that is inherited.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let grace = Duration::from_millis(500);
        let started = Instant::now();
        let outcome = session.stop_handle().stop(grace).await;
        assert_eq!(outcome.signalled, "SIGKILL", "{outcome:?}");
        assert!(outcome.exited, "{outcome:?}");
        assert!(
            started.elapsed() >= grace,
            "SIGKILL was sent before the grace elapsed: {:?}",
            started.elapsed()
        );
        assert!(!session.is_alive());
    }

    /// `stop()` on an already-exited child signals nothing and reports it — the pid is
    /// never touched once the waiter has reaped it.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_after_exit_signals_nothing() {
        let session = start_session(vec!["/bin/sh".into(), "-c".into(), "exit 0".into()]).await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while session.is_alive() {
            assert!(Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let outcome = session.stop_handle().stop(Duration::from_millis(200)).await;
        assert_eq!(
            outcome,
            StopOutcome {
                signalled: "none",
                exited: true
            }
        );
    }

    /// The child must observe a resize on its own TTY (TIOCSWINSZ), not just the emulator grid.
    #[tokio::test(flavor = "multi_thread")]
    async fn resize_reaches_child_process() {
        let session = start_session(vec!["/bin/sh".into()]).await;

        // `stty size` prints "rows cols" as reported by the child's TTY.
        session.send_keys("stty size<CR>").await.unwrap();
        wait_for_screen(&session, |s| s.contains("24 80")).await;

        session.resize(120, 40).await.unwrap();
        let screen = session.screen(&ScreenOpts::default()).await;
        assert_eq!((screen.size.cols, screen.size.rows), (120, 40));

        session.send_keys("stty size<CR>").await.unwrap();
        wait_for_screen(&session, |s| s.contains("40 120")).await;

        shutdown(&session, "exit<CR>").await;
    }

    /// resize() must bump the wait generation: a resize reflows/clips the grid without
    /// any PTY output, so in-flight waits would otherwise sleep through the change.
    #[tokio::test(flavor = "multi_thread")]
    async fn resize_bumps_wait_generation() {
        // A child that never writes: the resize is the only possible generation bump.
        let session = start_session(vec!["/bin/sleep".into(), "30".into()]).await;

        let mut generation_rx = session.wait_handle().generation_rx;
        let before = *generation_rx.borrow_and_update();

        session.resize(120, 40).await.unwrap();

        // The bump happens inside resize(), so it is visible as soon as resize returns.
        assert!(
            *generation_rx.borrow() > before,
            "resize did not bump the wait generation (still {before})"
        );

        shutdown(&session, "<C-c>").await;
    }

    /// wait_for(Text) returns as soon as delayed output lands, not at the timeout.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_text_matches_delayed_output() {
        let session = start_session(vec![
            "/bin/sh".into(),
            "-c".into(),
            "sleep 0.3; echo READY; sleep 30".into(),
        ])
        .await;

        let outcome = session
            .wait_for(WaitCondition::Text("READY".into()), Duration::from_secs(10))
            .await
            .unwrap();
        assert!(outcome.matched, "expected match: {outcome:?}");
        assert!(outcome.diagnostics.is_none());
        // Event-driven completion: far below the 10s timeout despite the delayed echo.
        assert!(
            outcome.elapsed_ms < 5000,
            "elapsed_ms = {}",
            outcome.elapsed_ms
        );

        // Interrupt the trailing `sleep 30` so the child exits.
        shutdown(&session, "<C-c>").await;
    }

    /// A wait for absent text times out at the deadline and carries the diagnostic snapshot.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_timeout_carries_diagnostics() {
        let session = start_session(vec!["/bin/sh".into()]).await;
        session
            .send_keys("echo hello-from-the-shell<CR>")
            .await
            .unwrap();
        wait_for_screen(&session, |s| s.contains("hello-from-the-shell")).await;

        let outcome = session
            .wait_for(
                WaitCondition::Text("NEVER_APPEARS_123".into()),
                Duration::from_millis(1000),
            )
            .await
            .unwrap();
        assert!(!outcome.matched);
        // Timed out at the deadline (with scheduler tolerance), neither early nor far late.
        assert!(
            (950..4000).contains(&outcome.elapsed_ms),
            "elapsed_ms = {}",
            outcome.elapsed_ms
        );
        let diag = outcome.diagnostics.expect("timeout must carry diagnostics");
        assert!(diag.screen.contains("hello-from-the-shell"));
        assert!(diag.raw_tail.contains("hello-from-the-shell"));
        assert!(diag.generation > 0);
        assert!(diag.cursor.row >= 1);
        assert!(!diag.ended, "deadline timeout must not be flagged as ended");

        // Once the child exits the grid is final: the wait fails fast, flagged `ended`.
        shutdown(&session, "exit<CR>").await;
        let outcome = session
            .wait_for(
                WaitCondition::Text("NEVER_APPEARS_123".into()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(!outcome.matched);
        assert!(
            outcome.elapsed_ms < 5000,
            "fail-fast took {}ms",
            outcome.elapsed_ms
        );
        assert!(outcome.diagnostics.expect("diagnostics on ended").ended);
    }

    /// wait_for(Gone) matches once the text is cleared from the screen.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_gone_matches_after_clear() {
        let session = start_session(vec!["/bin/sh".into()]).await;
        session.send_keys("echo MARKER_GONE_42<CR>").await.unwrap();
        let outcome = session
            .wait_for(
                WaitCondition::Text("MARKER_GONE_42".into()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(outcome.matched);

        // Clear the screen and home the cursor; the marker must vanish from the grid.
        session
            .send_keys(r"printf '\033[2J\033[H'<CR>")
            .await
            .unwrap();
        let outcome = session
            .wait_for(
                WaitCondition::Gone("MARKER_GONE_42".into()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(outcome.matched, "marker still on screen: {outcome:?}");

        shutdown(&session, "exit<CR>").await;
    }

    /// wait_for(StableMs) matches once output quiesces, never before the window elapses.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_stable_matches_after_quiesce() {
        let session = start_session(vec!["/bin/sh".into()]).await;
        session.send_keys("echo quiesce-now<CR>").await.unwrap();

        let outcome = session
            .wait_for(WaitCondition::StableMs(400), Duration::from_secs(10))
            .await
            .unwrap();
        assert!(outcome.matched, "screen never stabilized: {outcome:?}");
        // A full uninterrupted window is a lower bound on the elapsed time.
        assert!(
            outcome.elapsed_ms >= 400,
            "elapsed_ms = {}",
            outcome.elapsed_ms
        );

        shutdown(&session, "exit<CR>").await;
    }

    /// wait_for(StableMs) treats a resize as grid activity: the stability window restarts.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_stable_restarts_window_on_resize() {
        // A child that never writes: the resize is the only grid activity.
        let session = start_session(vec!["/bin/sleep".into(), "30".into()]).await;

        let wait = tokio::spawn(
            session
                .wait_handle()
                .wait_for(WaitCondition::StableMs(800), Duration::from_secs(10)),
        );
        // Land the resize inside the first stability window.
        tokio::time::sleep(Duration::from_millis(200)).await;
        session.resize(100, 30).await.unwrap();
        let resized_at = Instant::now();

        let outcome = wait.await.unwrap().unwrap();
        assert!(outcome.matched, "screen never stabilized: {outcome:?}");
        // A full window must elapse after the resize; without the restart the wait
        // matches off the original window, well under 800ms after the resize.
        assert!(
            resized_at.elapsed() >= Duration::from_millis(800),
            "stability window did not restart on resize (completed {:?} after it)",
            resized_at.elapsed()
        );

        shutdown(&session, "<C-c>").await;
    }

    /// wait_for(Regex) matches the screen text; invalid patterns error instead of waiting.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_regex_matches() {
        let session = start_session(vec!["/bin/sh".into()]).await;
        session.send_keys("echo exit code 42<CR>").await.unwrap();
        let outcome = session
            .wait_for(
                WaitCondition::Regex(r"exit code \d+".into()),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
        assert!(outcome.matched);

        assert!(
            session
                .wait_for(
                    WaitCondition::Regex("(unclosed".into()),
                    Duration::from_secs(1)
                )
                .await
                .is_err()
        );

        shutdown(&session, "exit<CR>").await;
    }
}

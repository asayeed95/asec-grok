//! HTTP + WebSocket server exposing PTY session control.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;

use crate::session::{PtySession, WaitCondition, WaitOutcome};
use crate::term::ScreenOpts;

/// Shared application state.
type AppState = Arc<Mutex<PtySession>>;

/// Query parameters for screen endpoint.
#[derive(Debug, serde::Deserialize, Default)]
pub struct ScreenParams {
    /// Row range, 1-indexed (e.g. "1:5", "5:", ":10", "5").
    pub rows: Option<String>,
    /// Column range, 1-indexed.
    pub cols: Option<String>,
    /// Character to show at cursor position.
    pub cursor: Option<char>,
    /// Output format: "text" (default), "styled", "html".
    pub format: Option<String>,
    /// Include trailing empty lines.
    pub full: Option<bool>,
}

/// Request body for send endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct SendRequest {
    pub keys: String,
}

/// Request body for resize endpoint.
#[derive(Debug, serde::Deserialize)]
pub struct ResizeRequest {
    pub cols: u16,
    pub rows: u16,
}

/// Query parameters for scrollback endpoint.
#[derive(Debug, serde::Deserialize, Default)]
pub struct ScrollbackParams {
    /// Number of scrollback lines to return (default: 100).
    pub lines: Option<usize>,
}

/// Default wait timeout in milliseconds.
const WAIT_DEFAULT_TIMEOUT_MS: u64 = 10_000;
/// Maximum wait timeout in milliseconds.
const WAIT_MAX_TIMEOUT_MS: u64 = 120_000;

/// Query parameters for wait endpoint — exactly one condition must be set.
#[derive(Debug, serde::Deserialize, Default)]
pub struct WaitParams {
    /// Wait until this text appears on screen.
    pub text: Option<String>,
    /// Wait until this regex matches the screen text.
    pub regex: Option<String>,
    /// Wait until this text is absent from the screen.
    pub gone: Option<String>,
    /// Wait until the grid has been unchanged for this many milliseconds.
    pub stable_ms: Option<u64>,
    /// Timeout in milliseconds (default 10000, capped at 120000).
    pub timeout_ms: Option<u64>,
}

/// Incoming WebSocket message from client.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
enum WsClientMessage {
    /// Send raw input text to the PTY.
    #[serde(rename = "input")]
    Input { data: String },
    /// Send vim-notation keys to the PTY.
    #[serde(rename = "keys")]
    Keys { keys: String },
    /// Resize the terminal.
    #[serde(rename = "resize")]
    Resize { cols: u16, rows: u16 },
}

/// Build the axum router.
pub fn build_router(session: PtySession) -> Router {
    let state: AppState = Arc::new(Mutex::new(session));

    Router::new()
        .route("/query/screen", get(handle_screen))
        .route("/query/cursor", get(handle_cursor))
        .route("/query/status", get(handle_status))
        .route("/query/scrollback", get(handle_scrollback))
        // Top-level on purpose: wait is a synchronization primitive, neither a /query read nor a /control mutation.
        .route("/wait", get(handle_wait))
        .route("/control/send", post(handle_send))
        .route("/control/resize", post(handle_resize))
        .route("/control/stop", post(handle_stop))
        .route("/ws", get(handle_ws_upgrade))
        .layer(CorsLayer::very_permissive())
        .with_state(state)
}

/// Parse a range string like "1:5", "5:", ":10", "5" into a Range<usize>.
fn parse_range(s: &str) -> Option<std::ops::Range<usize>> {
    if let Some((a, b)) = s.split_once(':') {
        let start = if a.is_empty() {
            1
        } else {
            a.parse::<usize>().ok()?
        };
        let end = if b.is_empty() {
            usize::MAX
        } else {
            b.parse::<usize>().ok()?
        };
        Some(start..end)
    } else {
        let n = s.parse::<usize>().ok()?;
        Some(n..n + 1)
    }
}

fn build_screen_opts(params: &ScreenParams) -> ScreenOpts {
    ScreenOpts {
        rows: params.rows.as_deref().and_then(parse_range),
        cols: params.cols.as_deref().and_then(parse_range),
        cursor_char: params.cursor,
        include_empty: params.full.unwrap_or(false),
    }
}

async fn handle_screen(
    State(state): State<AppState>,
    Query(params): Query<ScreenParams>,
) -> Response {
    let session = state.lock().await;
    let opts = build_screen_opts(&params);

    let format = params.format.as_deref().unwrap_or("text");
    match format {
        "styled" => {
            let styled = session.screen_styled(&opts).await;
            Json(styled).into_response()
        }
        "html" => {
            let html = session.screen_html(&opts).await;
            ([(axum::http::header::CONTENT_TYPE, "text/html")], html).into_response()
        }
        _ => {
            let output = session.screen(&opts).await;
            Json(output).into_response()
        }
    }
}

async fn handle_cursor(State(state): State<AppState>) -> Json<serde_json::Value> {
    let session = state.lock().await;
    let cursor = session.cursor().await;
    Json(serde_json::json!({
        "row": cursor.row,
        "col": cursor.col,
    }))
}

async fn handle_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let session = state.lock().await;
    let status = session.status().await;
    Json(serde_json::json!({
        "alive": status.alive,
        "pid": status.pid,
        "exit_code": status.exit_code,
        "size": [status.size.0, status.size.1],
        "modes": status.modes,
        "scrollback_lines": status.scrollback_lines,
    }))
}

async fn handle_send(
    State(state): State<AppState>,
    Json(req): Json<SendRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let session = state.lock().await;
    match session.send_keys(&req.keys).await {
        Ok(()) => Ok(Json(serde_json::json!({"ok": true}))),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": e.to_string()})),
        )),
    }
}

async fn handle_resize(
    State(state): State<AppState>,
    Json(req): Json<ResizeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let session = state.lock().await;
    match session.resize(req.cols, req.rows).await {
        Ok(()) => Ok(Json(serde_json::json!({"ok": true}))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )),
    }
}

/// Long-poll until a screen condition is met or the timeout elapses.
///
/// Timeout is a normal outcome (200 with `matched: false` + diagnostics),
/// not an error — `--gone`/`--stable_ms` waits time out routinely.
async fn handle_wait(
    State(state): State<AppState>,
    Query(params): Query<WaitParams>,
) -> Result<Json<WaitOutcome>, (StatusCode, Json<serde_json::Value>)> {
    let mut conditions = Vec::new();
    if let Some(text) = params.text {
        conditions.push(WaitCondition::Text(text));
    }
    if let Some(pattern) = params.regex {
        conditions.push(WaitCondition::Regex(pattern));
    }
    if let Some(text) = params.gone {
        conditions.push(WaitCondition::Gone(text));
    }
    if let Some(ms) = params.stable_ms {
        conditions.push(WaitCondition::StableMs(ms));
    }
    let (Some(condition), true) = (conditions.pop(), conditions.is_empty()) else {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "exactly one of text, regex, gone, stable_ms is required"
            })),
        ));
    };
    let timeout_ms = params
        .timeout_ms
        .unwrap_or(WAIT_DEFAULT_TIMEOUT_MS)
        .min(WAIT_MAX_TIMEOUT_MS);

    // Clone the wait handles and drop the session guard, or send/screen would block for the whole wait.
    let handle = state.lock().await.wait_handle();
    match handle
        .wait_for(condition, std::time::Duration::from_millis(timeout_ms))
        .await
    {
        Ok(outcome) => Ok(Json(outcome)),
        // Alternate format keeps the regex parse detail from the error chain.
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("{e:#}")})),
        )),
    }
}

/// `POST /control/stop` — SIGTERM the child, SIGKILL after the grace, and report what
/// happened. The stop sequence runs WITHOUT the session lock (a `StopHandle`, like
/// `/wait`'s handle), so `/query/*` keep answering while it runs. At afbc0fb this
/// stopped nothing (AGE-2131 #1). `ok` is kept for older clients; the rest is new.
async fn handle_stop(State(state): State<AppState>) -> Json<serde_json::Value> {
    let handle = state.lock().await.stop_handle();
    let outcome = handle.stop(crate::session::DEFAULT_STOP_GRACE).await;
    let (alive, _pid, exit_code) = state.lock().await.status_basic();
    Json(serde_json::json!({
        "ok": true,
        "signalled": outcome.signalled,
        "exited": outcome.exited,
        "alive": alive,
        "exit_code": exit_code,
    }))
}

// -- Scrollback --

async fn handle_scrollback(
    State(state): State<AppState>,
    Query(params): Query<ScrollbackParams>,
) -> Json<serde_json::Value> {
    let session = state.lock().await;
    let count = params.lines.unwrap_or(100);
    let lines = session.scrollback(count).await;
    Json(serde_json::json!({
        "count": lines.len(),
        "lines": lines,
    }))
}

// -- WebSocket streaming --

/// Upgrade an HTTP request to a WebSocket connection.
async fn handle_ws_upgrade(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state))
}

/// Handle a single WebSocket connection.
///
/// Protocol:
///
/// **Server -> Client:**
/// - Binary frames: raw PTY output bytes (high-throughput streaming)
/// - Text frames: JSON `{"type":"closed","exit_code":N}` on process exit
///
/// **Client -> Server:**
/// - Binary frames: raw bytes written to PTY stdin
/// - Text frames: JSON with `type` field:
///   - `{"type":"input","data":"text"}` — send text to PTY
///   - `{"type":"keys","keys":"<C-c>"}` — vim-notation keystrokes
///   - `{"type":"resize","cols":120,"rows":40}` — resize terminal
async fn handle_ws_connection(socket: WebSocket, state: AppState) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    // Subscribe to the PTY output broadcast channel, and to the server's shutdown.
    let session = state.lock().await;
    let mut output_rx = session.subscribe();
    let mut shutting_down = session.shutting_down();
    drop(session);

    // Task: forward PTY output -> WebSocket (binary frames). Ends — with the `closed`
    // frame — when the output channel closes OR the server is shutting down: a socket
    // left open here would hold the graceful drain until the backstop (AGE-2131 #2).
    // The child's exit alone sends nothing on this socket; clients read that from
    // `/query/status` (the surface HeyCLI's Lane E is built against).
    let state_output = state.clone();
    let mut send_task = tokio::spawn(async move {
        loop {
            let event = tokio::select! {
                r = output_rx.recv() => r,
                _ = async {
                    loop {
                        if *shutting_down.borrow_and_update() { break; }
                        if shutting_down.changed().await.is_err() { break; }
                    }
                } => Err(tokio::sync::broadcast::error::RecvError::Closed),
            };
            match event {
                Ok(bytes) => {
                    if ws_tx.send(Message::Binary(bytes.into())).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Client is too slow; send a warning and continue.
                    let msg = serde_json::json!({
                        "type": "warning",
                        "message": format!("dropped {n} output chunks (slow consumer)"),
                    });
                    let _ = ws_tx.send(Message::Text(msg.to_string().into())).await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    // Output channel closed (the session is gone) or the server is shutting down.
                    let session = state_output.lock().await;
                    let status = session.status().await;
                    let msg = serde_json::json!({
                        "type": "closed",
                        "exit_code": status.exit_code,
                    });
                    let _ = ws_tx.send(Message::Text(msg.to_string().into())).await;
                    break;
                }
            }
        }
    });

    // Task: receive WebSocket messages -> PTY input / control.
    let state_input = state.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(bytes) => {
                    // Raw binary input -> PTY.
                    let session = state_input.lock().await;
                    let _ = session.send_bytes(&bytes).await;
                }
                Message::Text(text) => {
                    // JSON-structured command.
                    if let Ok(cmd) = serde_json::from_str::<WsClientMessage>(&text) {
                        let session = state_input.lock().await;
                        match cmd {
                            WsClientMessage::Input { data } => {
                                let _ = session.send_bytes(data.as_bytes()).await;
                            }
                            WsClientMessage::Keys { keys: notation } => {
                                let _ = session.send_keys(&notation).await;
                            }
                            WsClientMessage::Resize { cols, rows } => {
                                let _ = session.resize(cols, rows).await;
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    // Wait for either task to finish, then abort the other.
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::{Duration, Instant};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::session::tests::start_session;

    /// Send one raw HTTP/1.1 request and read the full response (Connection: close).
    async fn http(port: u16, request: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect failed");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write failed");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read failed");
        String::from_utf8_lossy(&response).into_owned()
    }

    fn get(path: &str) -> String {
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
    }

    fn post_json(path: &str, body: &str) -> String {
        format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    /// A long-poll /wait must not hold the session mutex: the send and screen
    /// requests served mid-wait are exactly what make the wait complete.
    #[tokio::test(flavor = "multi_thread")]
    async fn wait_does_not_block_other_endpoints() {
        let session = start_session(vec!["/bin/sh".into()]).await;
        let router = super::build_router(session);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let start = Instant::now();
        // Long-poll for text that only appears if /control/send gets through mid-wait.
        let wait_task = tokio::spawn(async move {
            http(port, &get("/wait?text=WAIT_DONE_77&timeout_ms=30000")).await
        });

        // /query/screen must answer while the wait is in flight (deadline-polled).
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let resp = http(port, &get("/query/screen")).await;
            if resp.starts_with("HTTP/1.1 200") {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "screen not served during wait: {resp}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let resp = http(
            port,
            &post_json("/control/send", r#"{"keys":"echo WAIT_DONE_77<CR>"}"#),
        )
        .await;
        assert!(
            resp.starts_with("HTTP/1.1 200"),
            "send not served during wait: {resp}"
        );

        let wait_resp = wait_task.await.unwrap();
        assert!(wait_resp.starts_with("HTTP/1.1 200"), "{wait_resp}");
        assert!(wait_resp.contains(r#""matched":true"#), "{wait_resp}");
        // A deadlocked handler could only return at its 30s timeout.
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "wait took {:?}",
            start.elapsed()
        );

        // Zero or multiple conditions are usage errors.
        let resp = http(port, &get("/wait")).await;
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");
        let resp = http(port, &get("/wait?text=a&gone=b")).await;
        assert!(resp.starts_with("HTTP/1.1 400"), "{resp}");

        // Bounded shutdown so the shell doesn't outlive the test.
        let resp = http(port, &post_json("/control/send", r#"{"keys":"exit<CR>"}"#)).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let resp = http(port, &get("/query/status")).await;
            if resp.contains(r#""alive":false"#) {
                break;
            }
            assert!(Instant::now() < deadline, "shell did not exit: {resp}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    // ── AGE-2131 #2: the server's lifetime around the child's exit (crate::lifecycle) ──

    async fn serve(
        session: crate::session::PtySession,
        policy: crate::lifecycle::ShutdownPolicy,
    ) -> (u16, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let lc = session.lifecycle();
        let router = super::build_router(session);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(crate::lifecycle::serve_until_done(
            listener, router, lc, policy,
        ));
        (port, task)
    }

    async fn port_open(port: u16) -> bool {
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
    }

    /// Without --linger the server outlives the child by exactly the exit grace: the
    /// final status (with the exit code) is readable inside the grace, and the port is
    /// closed after it. At afbc0fb the server never ended.
    #[tokio::test(flavor = "multi_thread")]
    async fn server_ends_after_the_exit_grace_without_linger() {
        let session = start_session(vec!["/bin/sh".into(), "-c".into(), "exit 0".into()]).await;
        let policy = crate::lifecycle::ShutdownPolicy {
            linger: false,
            exit_grace: Duration::from_millis(1500),
        };
        let started = Instant::now();
        let (port, task) = serve(session, policy).await;

        // Inside the grace: the child has exited and its exit code is on the status.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let resp = http(port, &get("/query/status")).await;
            if resp.contains(r#""alive":false"#) {
                assert!(resp.contains(r#""exit_code":0"#), "{resp}");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "child never reported exited: {resp}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // The server returns on its own, and the port is closed afterwards.
        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("server did not end after the grace")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert!(
            started.elapsed() >= policy.exit_grace,
            "ended before the grace: {:?}",
            started.elapsed()
        );
        assert!(
            started.elapsed() < policy.exit_grace + crate::lifecycle::DRAIN_BACKSTOP,
            "the drain hit the backstop with no connections open: {:?}",
            started.elapsed()
        );
        assert!(
            !port_open(port).await,
            "port still open after the server returned"
        );
    }

    /// With --linger the server stays up after the child exits; /control/stop then ends
    /// it (after the grace) and reports that nothing needed signalling.
    #[tokio::test(flavor = "multi_thread")]
    async fn server_lingers_until_stop_with_linger() {
        let session = start_session(vec!["/bin/sh".into(), "-c".into(), "exit 0".into()]).await;
        let policy = crate::lifecycle::ShutdownPolicy {
            linger: true,
            exit_grace: Duration::from_millis(300),
        };
        let (port, task) = serve(session, policy).await;

        // Well past any grace, the server still answers and the child is reported exited.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let resp = http(port, &get("/query/status")).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(
            resp.contains(r#""alive":false"#) && resp.contains(r#""exit_code":0"#),
            "{resp}"
        );

        let resp = http(port, &post_json("/control/stop", "{}")).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(resp.contains(r#""signalled":"none""#), "{resp}");
        assert!(resp.contains(r#""exited":true"#), "{resp}");

        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("server did not end after stop")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert!(!port_open(port).await);
    }

    /// /control/stop on a live child: the child is ended, the response says SIGTERM and
    /// carries the exit code, and the server ends after the grace.
    #[tokio::test(flavor = "multi_thread")]
    async fn stop_endpoint_ends_the_child_and_then_the_server() {
        let session = start_session(vec!["/bin/sleep".into(), "30".into()]).await;
        let policy = crate::lifecycle::ShutdownPolicy {
            linger: false,
            exit_grace: Duration::from_millis(300),
        };
        let (port, task) = serve(session, policy).await;
        let resp = http(port, &get("/query/status")).await;
        assert!(resp.contains(r#""alive":true"#), "{resp}");

        let resp = http(port, &post_json("/control/stop", "{}")).await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(resp.contains(r#""signalled":"SIGTERM""#), "{resp}");
        assert!(resp.contains(r#""exited":true"#), "{resp}");
        assert!(resp.contains(r#""alive":false"#), "{resp}");
        assert!(!resp.contains(r#""exit_code":null"#), "{resp}");

        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("server did not end after stop")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
    }

    /// An open /ws must not pin the server past the grace: on shutdown the handler sends
    /// its `closed` frame (with the exit code) and drops the socket, and the drain
    /// completes well inside the backstop. Nothing is sent on the socket for the child's
    /// exit alone — that surface is unchanged.
    #[tokio::test(flavor = "multi_thread")]
    async fn ws_receives_closed_on_shutdown_and_the_drain_completes() {
        use futures_util::StreamExt;
        let session = start_session(vec![
            "/bin/sh".into(),
            "-c".into(),
            "sleep 0.5; exit 7".into(),
        ])
        .await;
        let policy = crate::lifecycle::ShutdownPolicy {
            linger: false,
            exit_grace: Duration::from_millis(1500),
        };
        let (port, task) = serve(session, policy).await;
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws"))
            .await
            .expect("ws connect");
        let connected_at = Instant::now();

        // Everything up to the closed frame; the child's exit itself sends no frame.
        let mut closed: Option<String> = None;
        let deadline = Instant::now() + Duration::from_secs(10);
        while let Ok(Some(msg)) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            ws.next(),
        )
        .await
        {
            match msg {
                Ok(tokio_tungstenite::tungstenite::Message::Text(t))
                    if t.contains(r#""type":"closed""#) =>
                {
                    closed = Some(t.to_string());
                    break;
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        let closed = closed.expect("no closed frame before the socket ended");
        assert!(closed.contains(r#""exit_code":7"#), "{closed}");
        // The frame came AFTER the grace (the child exited at ~0.5 s), not on the exit itself.
        assert!(
            connected_at.elapsed() >= Duration::from_millis(1500),
            "closed frame arrived before the grace elapsed: {:?}",
            connected_at.elapsed()
        );
        let _ = ws.close(None).await;

        let result = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("server did not end")
            .unwrap();
        assert!(result.is_ok(), "{result:?}");
        // Drain completed on its own: total time is grace + a little, not grace + backstop.
        assert!(
            connected_at.elapsed() < Duration::from_millis(1500) + crate::lifecycle::DRAIN_BACKSTOP,
            "the drain waited for the backstop: {:?}",
            connected_at.elapsed()
        );
    }
}

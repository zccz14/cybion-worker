use super::*;
use axum::{
    Json, Router,
    extract::Path as AxumPath,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};

fn event(id: &str, command: &str) -> String {
    format!(
        "event: tool_call\ndata: {}\n\n",
        json!({
            "id": id, "thread_id": id, "name": "bash", "arguments": {"command": command},
        })
    )
}

async fn controller(
    events: String,
) -> (
    WorkerConfig,
    mpsc::UnboundedReceiver<(String, Value)>,
    mpsc::UnboundedReceiver<(String, Value)>,
    JoinHandle<()>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let (receipt_sender, receipt_receiver) = mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/worker/v1/users/user/workers/worker/events",
            get(move || {
                let events = events.clone();
                async move { ([("content-type", "text/event-stream")], events) }
            }),
        )
        .route(
            "/worker/v1/users/user/workers/worker/calls/{id}/result",
            post(
                move |AxumPath(id): AxumPath<String>, Json(result): Json<Value>| {
                    let sender = sender.clone();
                    async move {
                        let status = if id == "reject" {
                            StatusCode::SERVICE_UNAVAILABLE
                        } else {
                            StatusCode::OK
                        };
                        sender.send((id, result)).unwrap();
                        status
                    }
                },
            ),
        )
        .route(
            "/worker/v1/users/user/workers/worker/calls/{id}/received",
            post(
                move |AxumPath(id): AxumPath<String>,
                      headers: HeaderMap,
                      Json(receipt): Json<Value>| {
                    let receipt_sender = receipt_sender.clone();
                    async move {
                        assert!(
                            headers
                                .get(delivery::BOOT_HEADER)
                                .is_some_and(|value| !value.is_empty()),
                            "receipt must carry the boot ID"
                        );
                        receipt_sender.send((id, receipt)).unwrap();
                        StatusCode::OK
                    }
                },
            ),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = WorkerConfig {
        controller_url: format!("http://{}", listener.local_addr().unwrap()),
        user_id: "user".to_owned(),
        machine_id: "worker".to_owned(),
        access_token: "test".to_owned(),
    };
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (config, receiver, receipt_receiver, server)
}

async fn expect_receipt(receipts: &mut mpsc::UnboundedReceiver<(String, Value)>) -> String {
    let (id, receipt) = tokio::time::timeout(Duration::from_secs(5), receipts.recv())
        .await
        .expect("receipt was not posted")
        .unwrap();
    assert_eq!(receipt, json!({}));
    id
}

#[tokio::test]
async fn dispatches_next_call_and_finishes_stream_while_first_call_is_running() {
    let root = tempfile::tempdir().unwrap();
    let gate = root.path().join("release");
    let slow = if cfg!(windows) {
        format!(
            "powershell -NoProfile -NonInteractive -Command \"while (!(Test-Path -LiteralPath '{}')) {{ Start-Sleep -Milliseconds 20 }}; Write-Output slow\"",
            gate.display()
        )
    } else {
        format!(
            "while [ ! -f '{}' ]; do sleep 0.02; done; echo slow",
            gate.display()
        )
    };
    let (config, mut results, mut receipts, server) =
        controller(event("slow", &slow) + &event("fast", "echo fast")).await;
    let client = Client::new();
    let session = tokio::spawn(async move { event_session(&client, &config).await });
    let first = tokio::time::timeout(Duration::from_secs(5), results.recv()).await;
    let stream_finished_before_slow = session.is_finished();
    // Always release the fixture, including when testing the old blocking implementation.
    std::fs::write(&gate, "release").unwrap();
    let first = first
        .expect("fast command was blocked behind slow command")
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .unwrap()
        .unwrap();
    session.await.unwrap().unwrap();
    server.abort();
    assert_eq!(first.0, "fast");
    assert_eq!(second.0, "slow");
    assert!(!first.1["failed"].as_bool().unwrap());
    assert!(!second.1["failed"].as_bool().unwrap());
    assert!(
        first.1["result"]["stdout"]
            .as_str()
            .unwrap()
            .contains("fast")
    );
    assert!(
        stream_finished_before_slow,
        "SSE session waited for dispatched work"
    );
    let mut receipt_ids = [
        expect_receipt(&mut receipts).await,
        expect_receipt(&mut receipts).await,
    ];
    receipt_ids.sort();
    assert_eq!(receipt_ids, ["fast", "slow"]);
}

#[tokio::test]
async fn failed_result_upload_does_not_stop_dispatching_other_calls() {
    let (config, mut results, mut receipts, server) =
        controller(event("reject", "echo reject") + &event("next", "echo next")).await;
    event_session(&Client::new(), &config).await.unwrap();
    let mut ids = Vec::new();
    for _ in 0..2 {
        let (id, _) = tokio::time::timeout(Duration::from_secs(5), results.recv())
            .await
            .unwrap()
            .unwrap();
        ids.push(id);
    }
    let mut receipt_ids = [
        expect_receipt(&mut receipts).await,
        expect_receipt(&mut receipts).await,
    ];
    receipt_ids.sort();
    server.abort();
    ids.sort();
    assert_eq!(ids, ["next", "reject"]);
    assert_eq!(receipt_ids, ["next", "reject"]);
}

#[tokio::test]
async fn duplicate_delivery_is_receipted_again_without_re_executing() {
    let (config, mut results, mut receipts, server) =
        controller(event("dup", "echo dup") + &event("dup", "echo dup")).await;
    event_session(&Client::new(), &config).await.unwrap();
    assert_eq!(expect_receipt(&mut receipts).await, "dup");
    assert_eq!(expect_receipt(&mut receipts).await, "dup");
    let (id, result) = tokio::time::timeout(Duration::from_secs(5), results.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(id, "dup");
    assert!(result["result"]["stdout"].as_str().unwrap().contains("dup"));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), results.recv())
            .await
            .is_err(),
        "duplicate delivery must not execute twice"
    );
    server.abort();
}

#[tokio::test]
async fn reconnect_and_lost_upload_confirmation_do_not_execute_twice() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let uploads = Arc::new(AtomicUsize::new(0));
    let receipts = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let app = Router::new()
        .route(
            "/worker/v1/users/user/workers/worker/events",
            get(|| async {
                (
                    [("content-type", "text/event-stream")],
                    event("once", "echo execution"),
                )
            }),
        )
        .route(
            "/worker/v1/users/user/workers/worker/calls/{id}/result",
            post({
                let uploads = uploads.clone();
                move |headers: axum::http::HeaderMap, Json(body): Json<Value>| {
                    let uploads = uploads.clone();
                    let tx = tx.clone();
                    async move {
                        let n = uploads.fetch_add(1, Ordering::SeqCst);
                        tx.send((
                            headers[delivery::BOOT_HEADER].to_str().unwrap().to_owned(),
                            body,
                        ))
                        .unwrap();
                        if n == 0 {
                            (StatusCode::SERVICE_UNAVAILABLE, [("retry-after", "0")])
                        } else {
                            (StatusCode::OK, [("retry-after", "0")])
                        }
                    }
                }
            }),
        )
        .route(
            "/worker/v1/users/user/workers/worker/calls/{id}/received",
            post({
                let receipts = receipts.clone();
                move || {
                    let receipts = receipts.clone();
                    async move {
                        receipts.fetch_add(1, Ordering::SeqCst);
                        StatusCode::OK
                    }
                }
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = WorkerConfig {
        controller_url: format!("http://{}", listener.local_addr().unwrap()),
        user_id: "user".into(),
        machine_id: "worker".into(),
        access_token: "test".into(),
    };
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let delivery = Arc::new(delivery::DeliveryState::new());
    let client = Client::new();
    event_session_ready(&client, &config, None, delivery.clone())
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.0, delivery.boot_id);
    delivery.wait_idle().await;
    event_session_ready(&client, &config, None, delivery.clone())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err()
    );
    delivery.wait_idle().await;
    // One receipt per received delivery, one execution for two deliveries.
    assert_eq!(receipts.load(Ordering::SeqCst), 2);
    assert_eq!(uploads.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn permanent_upload_rejection_is_not_retried() {
    let (config, mut received, _receipts, server) = controller(String::new()).await;
    let error = delivery::post_until_confirmed(
        &Client::new(),
        &config,
        &format!("{}/missing", config.controller_url),
        "boot",
        &json!({}),
    )
    .await
    .unwrap_err();
    assert!(!delivery::retryable(&error));
    assert!(received.try_recv().is_err());
    server.abort();
}

#[tokio::test]
async fn upgrade_event_is_bound_to_the_current_process_and_waits_for_work() {
    let state = Arc::new(delivery::DeliveryState::new());
    let call = ToolCall {
        id: "slow".into(),
        thread_id: "thread".into(),
        name: "bash".into(),
        arguments: json!({"command":"echo slow"}),
    };
    assert!(state.admit(&call).unwrap());
    let waiter = tokio::spawn({
        let state = state.clone();
        async move { state.wait_idle().await }
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!waiter.is_finished());
    state.finished();
    waiter.await.unwrap();
    let event = format!(
        "event: upgrade\ndata: {}\n\n",
        json!({"id":"upgrade","version":"v0.2.1","boot_id":state.boot_id})
    );
    let parsed = self_update::parse_event(&event).unwrap().unwrap();
    assert_eq!(parsed.boot_id, state.boot_id);
    assert_eq!(parsed.version, "v0.2.1");
}

use super::*;
use axum::{
    Json, Router,
    extract::Path as AxumPath,
    http::StatusCode,
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
    JoinHandle<()>,
) {
    let (sender, receiver) = mpsc::unbounded_channel();
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
    (config, receiver, server)
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
    let (config, mut results, server) =
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
}

#[tokio::test]
async fn failed_result_upload_does_not_stop_dispatching_other_calls() {
    let (config, mut results, server) =
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
    server.abort();
    ids.sort();
    assert_eq!(ids, ["next", "reject"]);
}

use super::*;
use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::net::TcpListener;

struct FakeBrowser {
    address: SocketAddr,
    page_closed: AtomicBool,
    page_recreated: AtomicBool,
}

async fn fake_devtools_browser() -> (String, Arc<FakeBrowser>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let browser = Arc::new(FakeBrowser {
        address,
        page_closed: AtomicBool::new(false),
        page_recreated: AtomicBool::new(false),
    });
    let app = Router::new()
        .route("/json/version", get(version))
        .route("/json/list", get(list))
        .route("/devtools/page/page-1", get(unresponsive_page))
        .route("/devtools/browser", get(browser_socket))
        .with_state(browser.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), browser)
}

async fn version(State(browser): State<Arc<FakeBrowser>>) -> Json<Value> {
    Json(json!({"webSocketDebuggerUrl": format!("ws://{}/devtools/browser", browser.address)}))
}

async fn list(State(browser): State<Arc<FakeBrowser>>) -> Json<Value> {
    let page = |id: &str| json!({"id": id, "type": "page", "webSocketDebuggerUrl": format!("ws://{}/devtools/page/{id}", browser.address)});
    if !browser.page_closed.load(Ordering::SeqCst) {
        Json(json!([page("page-1")]))
    } else if !browser.page_recreated.load(Ordering::SeqCst) {
        Json(json!([]))
    } else {
        Json(json!([page("page-2")]))
    }
}

async fn unresponsive_page(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(|mut socket: WebSocket| async move {
        let _ = socket.recv().await;
        // Never answer the command, simulating a wedged page renderer.
        tokio::time::sleep(Duration::from_secs(3600)).await;
    })
}

async fn browser_socket(State(browser): State<Arc<FakeBrowser>>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |mut socket: WebSocket| async move {
        while let Some(Ok(message)) = socket.recv().await {
            if let WsMessage::Text(text) = message {
                let request: Value = serde_json::from_str(text.as_str()).unwrap();
                match request["method"].as_str() {
                    Some("Target.closeTarget") => browser.page_closed.store(true, Ordering::SeqCst),
                    Some("Target.createTarget") => {
                        browser.page_recreated.store(true, Ordering::SeqCst)
                    }
                    _ => {}
                }
                let response = json!({"id": request["id"], "result": {}}).to_string();
                let _ = socket.send(WsMessage::Text(response.into())).await;
            }
        }
    })
}

#[tokio::test]
async fn browser_command_restarts_unresponsive_page_after_timeout() {
    let (base, browser) = fake_devtools_browser().await;
    let error = cdp_command(
        &base,
        "Runtime.evaluate",
        json!({"expression": "1+1"}),
        Duration::from_millis(300),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unresponsive"), "{error}");
    assert!(
        browser.page_closed.load(Ordering::SeqCst),
        "page should be closed"
    );
    assert!(
        browser.page_recreated.load(Ordering::SeqCst),
        "page should be recreated"
    );
}

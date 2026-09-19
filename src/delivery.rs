use super::*;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

pub const BOOT_HEADER: &str = "x-cybion-worker-boot-id";

pub struct DeliveryState {
    pub boot_id: String,
    seen: Mutex<HashMap<String, String>>,
    active: AtomicUsize,
}

impl DeliveryState {
    pub fn new() -> Self {
        Self {
            boot_id: uuid::Uuid::new_v4().to_string(),
            seen: Mutex::new(HashMap::new()),
            active: AtomicUsize::new(0),
        }
    }

    pub fn admit(&self, call: &ToolCall) -> Result<bool> {
        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(
            &json!({"thread_id":call.thread_id,"name":call.name,"arguments":call.arguments}),
        )?));
        let mut seen = self.seen.lock().expect("delivery lock poisoned");
        if let Some(existing) = seen.get(&call.id) {
            ensure!(
                existing == &fingerprint,
                "repeated call ID has different arguments"
            );
            return Ok(false);
        }
        seen.insert(call.id.clone(), fingerprint);
        self.active.fetch_add(1, Ordering::SeqCst);
        Ok(true)
    }

    pub fn finished(&self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }

    pub async fn wait_idle(&self) {
        while self.active.load(Ordering::SeqCst) != 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub fn frame_end(bytes: &[u8]) -> Option<(usize, usize)> {
    (0..bytes.len()).find_map(|i| {
        if bytes[i..].starts_with(b"\n\n") {
            Some((i, 2))
        } else if bytes[i..].starts_with(b"\r\n\r\n") {
            Some((i, 4))
        } else {
            None
        }
    })
}

pub fn retry_delay(attempt: u32) -> Duration {
    let base = (1_u64 << attempt.saturating_sub(1).min(5)).min(30) * 1000;
    let jitter = u64::from(uuid::Uuid::new_v4().as_bytes()[0]) * base / 1024;
    Duration::from_millis((base + jitter).min(30_000))
}

pub fn retryable(error: &anyhow::Error) -> bool {
    match error
        .downcast_ref::<reqwest::Error>()
        .and_then(reqwest::Error::status)
    {
        Some(status) => {
            status.is_server_error() || status.as_u16() == 408 || status.as_u16() == 429
        }
        None => true,
    }
}

pub async fn post_until_confirmed(
    client: &Client,
    config: &WorkerConfig,
    url: &str,
    boot_id: &str,
    payload: &Value,
) -> Result<()> {
    let mut failures = 0;
    loop {
        let response = client
            .post(url)
            .bearer_auth(&config.access_token)
            .header(BOOT_HEADER, boot_id)
            .timeout(Duration::from_secs(30))
            .json(payload)
            .send()
            .await;
        let retry_after = response
            .as_ref()
            .ok()
            .and_then(|r| r.headers().get("retry-after"))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_secs);
        let result = response
            .and_then(reqwest::Response::error_for_status)
            .map(|_| ())
            .map_err(anyhow::Error::from);
        match result {
            Ok(()) => return Ok(()),
            Err(error) if retryable(&error) => {
                failures += 1;
                let delay = retry_after.unwrap_or_else(|| retry_delay(failures));
                tracing::warn!(%error, attempt=failures, delay_ms=delay.as_millis(), "Retrying Worker message; not re-executing tool");
                tokio::time::sleep(delay).await;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deduplication_survives_completion_but_not_process_restart() {
        let state = DeliveryState::new();
        let call = ToolCall {
            id: "call".into(),
            thread_id: "thread".into(),
            name: "bash".into(),
            arguments: json!({"command":"echo hello"}),
        };
        assert!(state.admit(&call).unwrap());
        assert!(!state.admit(&call).unwrap());
        state.finished();
        assert!(!state.admit(&call).unwrap());
        let other = DeliveryState::new();
        assert_ne!(other.boot_id, state.boot_id);
        assert!(other.admit(&call).unwrap());
        assert!(
            state
                .admit(&ToolCall {
                    arguments: json!({"command":"different"}),
                    ..call
                })
                .is_err()
        );
    }
    #[test]
    fn frame_boundaries_and_backoff_are_bounded() {
        assert_eq!(frame_end(b"data: a\r\n\r\nrest"), Some((7, 4)));
        assert_eq!(frame_end(b"data: a\n\nrest"), Some((7, 2)));
        for n in 0..100 {
            assert!(retry_delay(n) <= Duration::from_secs(30));
        }
    }
}

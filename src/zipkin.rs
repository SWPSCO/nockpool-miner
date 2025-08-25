// minimal Zipkin-over-HTTPS exporter

use std::{
    collections::HashMap,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use futures::Future;
use rand::{rngs::OsRng, RngCore};
use tokio::sync::mpsc;
use tracing::{Metadata, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer};

pub async fn time_proof<F, T>(name: &str, tags: &HashMap<String, String>, fut: F) -> T
where
    F: Future<Output = T>,
{
    let span = tracing::info_span!("proof", proof = %name);
    for (k, v) in tags {
        span.record(k.as_str(), &tracing::field::display(v));
    }
    let _g = span.enter();
    fut.await
}

fn normalize_zipkin_endpoint(base: &str) -> String {
    if base.ends_with("/api/v2/spans") {
        base.to_string()
    } else {
        format!("{}/api/v2/spans", base.trim_end_matches('/'))
    }
}

struct StartTime(Instant);
struct TraceId(String);
struct SpanId(String);
struct LocalTags(HashMap<String, String>);

pub struct SpanData {
    trace_id: String,
    span_id: String,
    parent_id: Option<String>,
    name: String,
    start_time_us: u64,
    duration_us: u64,
    local_tags: HashMap<String, String>,
}

impl<S> Layer<S> for SpanExportLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span missing");
        let mut exts = span.extensions_mut();

        let meta = span.metadata();
        let is_proof = meta.name() == "poke";
        exts.insert(ShouldExport(is_proof));

        if !is_proof {
            return;
        }

        exts.insert(StartTime(Instant::now()));

        let trace_id = if let Some(parent) = span.parent() {
            if let Some(t) = parent.extensions().get::<TraceId>() {
                t.0.clone()
            } else {
                new_trace_id()
            }
        } else {
            new_trace_id()
        };

        exts.insert(TraceId(trace_id));
        exts.insert(SpanId(new_span_id()));

        // collect initial attributes as tags
        let mut tags = HashMap::<String, String>::new();
        struct TagVisitor<'a>(&'a mut HashMap<String, String>);
        impl<'a> tracing::field::Visit for TagVisitor<'a> {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) { self.0.insert(f.name().to_string(), format!("{v:?}")); }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) { self.0.insert(f.name().to_string(), v.to_string()); }
            fn record_bool(&mut self, f: &tracing::field::Field, v: bool) { self.0.insert(f.name().to_string(), v.to_string()); }
            fn record_i64(&mut self, f: &tracing::field::Field, v: i64) { self.0.insert(f.name().to_string(), v.to_string()); }
            fn record_u64(&mut self, f: &tracing::field::Field, v: u64) { self.0.insert(f.name().to_string(), v.to_string()); }
        }
        let mut vis = TagVisitor(&mut tags);
        attrs.record(&mut vis);
        exts.insert(LocalTags(tags));
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        if let Some(span) = ctx.span(id) {
            let mut exts = span.extensions_mut();
            let should_export = exts
                .get_mut::<ShouldExport>()
                .map(|s| s.0)
                .unwrap_or(false);
            if !should_export {
                return;
            }

            if let Some(LocalTags(ref mut tags)) = exts.get_mut::<LocalTags>() {
                struct TagVisitor<'a>(&'a mut std::collections::HashMap<String, String>);
                impl<'a> tracing::field::Visit for TagVisitor<'a> {
                    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                        self.0.insert(f.name().to_string(), format!("{v:?}"));
                    }
                    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                        self.0.insert(f.name().to_string(), v.to_string());
                    }
                    fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
                        self.0.insert(f.name().to_string(), v.to_string());
                    }
                    fn record_i64(&mut self, f: &tracing::field::Field, v: i64) {
                        self.0.insert(f.name().to_string(), v.to_string());
                    }
                    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
                        self.0.insert(f.name().to_string(), v.to_string());
                    }
                }
                let mut v = TagVisitor(tags);
                values.record(&mut v);
            }
        }
    }


    fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("span missing");
        let exts = span.extensions();

        if !exts.get::<ShouldExport>().map(|s| s.0).unwrap_or(true) {
            return; // ignore non-proof spans
        }

        let start = exts.get::<StartTime>().map(|s| s.0).unwrap_or_else(Instant::now);
        let duration_us = start.elapsed().as_micros() as u64;
        let trace_id = exts.get::<TraceId>().map(|t| t.0.clone()).unwrap_or_else(new_trace_id);
        let span_id  = exts.get::<SpanId>().map(|s| s.0.clone()).unwrap_or_else(new_span_id);
        let parent_id = span.parent().and_then(|p| p.extensions().get::<SpanId>().map(|sid| sid.0.clone()));
        let local_tags = exts.get::<LocalTags>().map(|t| t.0.clone()).unwrap_or_default();

        let data = SpanData {
            trace_id,
            span_id,
            parent_id,
            name: span.metadata().name().to_string(),
            start_time_us: now_epoch_us(),
            duration_us,
            local_tags,
        };

        let _ = self.tx.send(data);
    }

    fn on_event(&self, _event: &tracing::Event<'_>, _ctx: Context<'_, S>) {}
    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::always()
    }
}

// ------------ exporter ------------

async fn span_batch_exporter(mut rx: mpsc::UnboundedReceiver<SpanData>, endpoint: String) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .expect("zipkin client");

    let batch_size = 32usize;
    let flush_every = std::time::Duration::from_secs(60);
    let mut buf: Vec<SpanData> = Vec::with_capacity(batch_size);
    let mut tick = tokio::time::interval(flush_every);

    tracing::debug!(target: "zipkin", %endpoint, batch_size, flush_secs = flush_every.as_secs(), "exporter task started");

    loop {
        tokio::select! {
            Some(s) = rx.recv() => {
                buf.push(s);
                if buf.len() >= batch_size {
                    tracing::debug!(target: "zipkin", count = buf.len(), reason = "batch_size", "flushing spans");
                    flush(&client, &endpoint, &mut buf).await;
                }
            }
            _ = tick.tick() => {
                if !buf.is_empty() {
                    tracing::debug!(target: "zipkin", count = buf.len(), reason = "interval", "flushing spans");
                    flush(&client, &endpoint, &mut buf).await;
                }
            }
            else => break,
        }
    }
}

async fn flush(client: &reqwest::Client, endpoint: &str, buf: &mut Vec<SpanData>) {
    if buf.is_empty() { return; }
    let count = buf.len();
    let batch: Vec<SpanData> = std::mem::take(buf);
    let payload = convert_to_zipkin(&batch);

    // control logging body
    let dump_body = std::env::var_os("NP_ZIPKIN_DEBUG_BODY").is_some();

    let res = client
        .post(endpoint)
        .header("content-type", "application/json")
        .header("user-agent", "nockpool-miner/zipkin")
        .body(payload)
        .send()
        .await;

    match res {
        Ok(resp) => {
            let status = resp.status();
            if dump_body {
                match resp.text().await {
                    Ok(body) => {
                        if status.is_success() {
                            tracing::debug!(target: "zipkin", %status, count, body = body.as_str(), "zipkin POST ok");
                        } else {
                            tracing::warn!(target: "zipkin", %status, count, body = body.as_str(), "zipkin POST non-success");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(target: "zipkin", %status, count, error = %e, "zipkin POST (failed reading body)");
                    }
                }
            } else {
                if status.is_success() {
                    tracing::debug!(target: "zipkin", %status, count, "zipkin POST ok");
                } else {
                    tracing::warn!(target: "zipkin", %status, count, "zipkin POST non-success");
                }
                // drain the body to let the connection be reused
                let _ = resp.bytes().await;
            }
        }
        Err(e) => {
            tracing::error!(target: "zipkin", error = %e, "zipkin POST failed");
        }
    }
}


pub struct SpanExportLayer {
    tx: tokio::sync::mpsc::UnboundedSender<SpanData>,
}

pub fn make_layer(
    collector_url: &str,
    service_name: &str,
    device_key: &str,
    extra_global_tags: &std::collections::HashMap<String, String>,
) -> SpanExportLayer {
    let endpoint = normalize_zipkin_endpoint(collector_url);
    tracing::info!(target: "zipkin", %endpoint, "zipkin exporter enabled");
    let endpoint = normalize_zipkin_endpoint(collector_url);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<SpanData>();
    tokio::spawn(span_batch_exporter(rx, endpoint));
    // clone the borrowed map so we can mutate it
    let mut merged = extra_global_tags.clone();
    merged.insert("device_key".to_string(), device_key.to_string());
    global::init(service_name.to_string(), merged);

    SpanExportLayer { tx }
}

fn convert_to_zipkin(batch: &[SpanData]) -> String {
    #[derive(serde::Serialize)]
    struct Endpoint<'a> {
        #[serde(rename = "serviceName")]
        service_name: &'a str,
    }
    #[derive(serde::Serialize)]
    struct ZSpan<'a> {
        #[serde(rename = "traceId")]
        trace_id: &'a str,
        #[serde(rename = "id")]
        id: &'a str,
        #[serde(rename = "parentId", skip_serializing_if = "Option::is_none")]
        parent_id: Option<&'a str>,
        name: &'a str,
        timestamp: u64, // micros since epoch
        duration: u64,  // micros
        #[serde(rename = "localEndpoint")]
        local_endpoint: Endpoint<'a>,
        tags: &'a HashMap<String, String>,
    }

    let globals = global::read();
    // merge global + local tags per span
    let merged: Vec<HashMap<String, String>> = batch
        .iter()
        .map(|s| {
            let mut m = globals.global_tags.clone();
            for (k, v) in &s.local_tags {
                m.insert(k.clone(), v.clone());
            }
            m
        })
        .collect();

    let items: Vec<ZSpan> = batch
        .iter()
        .zip(merged.iter())
        .map(|(s, tags)| ZSpan {
            trace_id: &s.trace_id,
            id: &s.span_id,
            parent_id: s.parent_id.as_deref(),
            name: &s.name,
            timestamp: s.start_time_us,
            duration: s.duration_us,
            local_endpoint: Endpoint {
                service_name: &globals.service_name,
            },
            tags,
        })
        .collect();

    serde_json::to_string(&items).unwrap_or_else(|_| "[]".to_string())
}

struct ShouldExport(bool);

struct GlobalState {
    service_name: String,
    global_tags: HashMap<String, String>,
}

mod global {
    use super::GlobalState;
    use std::collections::HashMap;
    use std::sync::{OnceLock, RwLock, RwLockReadGuard};

    static G: OnceLock<RwLock<GlobalState>> = OnceLock::new();

    pub fn init(service_name: String, tags: HashMap<String, String>) {
        let _ = G.set(RwLock::new(GlobalState {
            service_name,
            global_tags: tags,
        }));
    }

    pub fn read() -> RwLockReadGuard<'static, GlobalState> {
        G.get().expect("tracer not initialized").read().unwrap()
    }
}

// ------------ helpers ------------

fn now_epoch_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn new_trace_id() -> String {
    let mut b = [0u8; 16]; // 128-bit
    OsRng.fill_bytes(&mut b);
    hex::encode(b)
}

fn new_span_id() -> String {
    let mut b = [0u8; 8]; // 64-bit
    OsRng.fill_bytes(&mut b);
    hex::encode(b)
}

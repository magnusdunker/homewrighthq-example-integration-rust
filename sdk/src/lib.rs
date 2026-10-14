//! SDK til eksterne TheHome-integrationer.
//!
//! En ekstern integration er et selvstændigt program. TheHome starter det og
//! taler med det over JSON-RPC 2.0 på stdin og stdout, én besked pr. linje.
//! stderr er integrationens log. Programmet stopper, når stdin lukkes.
//!
//! Implementér [`Integration`] og kald [`run`] fra `main`:
//!
//! ```no_run
//! use thehome_plugin::*;
//!
//! struct Mine;
//!
//! #[async_trait]
//! impl Integration for Mine {
//!     async fn setup_entry(&self, host: &Host, entry: Entry) -> Result<(), Error> {
//!         let id = host.add_entity(&entry.entry_id, Entity::new("sensor", "mine-1"), false).await?;
//!         if let Some(id) = id {
//!             host.set_state(&id, "42", Map::new()).await?;
//!         }
//!         Ok(())
//!     }
//! }
//!
//! fn main() {
//!     run(Mine);
//! }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

pub use async_trait::async_trait;
pub use serde_json::{json, Map, Value};

/// Versionen af protokollen, som denne SDK taler.
pub const API_VERSION: u64 = 1;

const HOST_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// fejl

/// En fejl til TheHome. `kind` afgør, hvad TheHome gør:
/// `not_ready` prøver igen senere, `auth_failed` beder brugeren logge ind
/// igen, `invalid_argument`, `not_supported`, `unavailable` og `not_found`
/// vises ved tjenestekald, og `failed` er alt andet.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Error {
    pub kind: String,
    pub message: String,
}

impl Error {
    pub fn new(kind: &str, message: impl std::fmt::Display) -> Self {
        Self { kind: kind.into(), message: message.to_string() }
    }
    pub fn not_ready(m: impl std::fmt::Display) -> Self {
        Self::new("not_ready", m)
    }
    pub fn auth_failed(m: impl std::fmt::Display) -> Self {
        Self::new("auth_failed", m)
    }
    pub fn failed(m: impl std::fmt::Display) -> Self {
        Self::new("failed", m)
    }
    pub fn invalid(m: impl std::fmt::Display) -> Self {
        Self::new("invalid_argument", m)
    }
    pub fn not_supported(m: impl std::fmt::Display) -> Self {
        Self::new("not_supported", m)
    }
    pub fn unavailable(m: impl std::fmt::Display) -> Self {
        Self::new("unavailable", m)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Error {}

fn to_wire(e: &Error) -> Value {
    json!({ "code": -32000, "message": e.message, "data": { "kind": e.kind } })
}

// ---------------------------------------------------------------------------
// data fra TheHome

/// En opsætning af integrationen (en "config entry").
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Entry {
    pub entry_id: String,
    #[serde(default)]
    pub domain: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub options: Value,
    #[serde(default)]
    pub unique_id: Option<String>,
}

impl Entry {
    /// Værdi fra options først, ellers fra data.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.options.get(key).or_else(|| self.data.get(key))
    }
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.get(key).and_then(Value::as_str)
    }
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.get(key).and_then(Value::as_f64)
    }
}

/// Et opsætningsforløb i gang.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct Flow {
    pub flow_id: String,
    /// `user`, `options`, `reauth`, `zeroconf` ...
    pub source: String,
    #[serde(default)]
    pub discovery: Option<Value>,
    /// Data samlet i tidligere trin.
    #[serde(default)]
    pub data: Map<String, Value>,
    #[serde(default)]
    pub unique_id: Option<String>,
    /// Ved options- og reauth-forløb: opsætningen, der ændres.
    #[serde(default)]
    pub entry: Option<Entry>,
}

/// Svaret på et trin i et opsætningsforløb.
#[derive(Clone, Debug, Serialize)]
pub struct FlowReply {
    pub result: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
}

impl FlowReply {
    fn of(result: Value) -> Self {
        Self { result, data: None, unique_id: None }
    }

    /// Vis en formular. `schema` er et JSON Schema-objekt (`type: object`).
    pub fn form(step_id: &str, schema: Value) -> Self {
        Self::of(json!({ "type": "form", "step_id": step_id, "schema": schema }))
    }

    /// Vis formularen igen med fejl, f.eks. `{"base": "cannot_connect"}`.
    pub fn form_with_errors(step_id: &str, schema: Value, errors: Value) -> Self {
        Self::of(json!({ "type": "form", "step_id": step_id, "schema": schema, "errors": errors }))
    }

    pub fn menu(step_id: &str, options: &[&str]) -> Self {
        Self::of(json!({ "type": "menu", "step_id": step_id, "options": options }))
    }

    /// Afslut og opret opsætningen. I et options-forløb bliver `data` de nye options.
    pub fn create(title: &str, data: Value) -> Self {
        Self::of(json!({ "type": "create_entry", "title": title, "data": data }))
    }

    pub fn abort(reason: &str) -> Self {
        Self::of(json!({ "type": "abort", "reason": reason }))
    }

    /// Gem data til de næste trin.
    pub fn with_data(mut self, data: Map<String, Value>) -> Self {
        self.data = Some(data);
        self
    }

    /// Sæt et unikt id. TheHome afbryder med `already_configured`, hvis en
    /// anden opsætning har det.
    pub fn with_unique_id(mut self, id: impl Into<String>) -> Self {
        self.unique_id = Some(id.into());
        self
    }
}

/// En enhed. Felterne svarer til TheHomes enhedsregister.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Device {
    /// F.eks. `[("mit_domæne", "serienummer")]`. Holder enheden stabil.
    pub identifiers: Vec<(String, String)>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sw_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub serial_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_area: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration_url: Option<String>,
    /// Identifier for the device this one is connected through, e.g. a hub.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via_device: Option<(String, String)>,
}

/// En entity. Felterne svarer til TheHomes entity-register.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Entity {
    /// `sensor`, `switch`, `light`, `button`, `climate` ...
    pub domain: String,
    /// Stabilt id, der aldrig må ændre sig.
    pub unique_id: String,
    /// Navn efter enhedens navn. `None` betyder enhedens hoved-entity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit_of_measurement: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_category: Option<String>,
    /// Slået fra, når den oprettes. Brugeren kan slå den til.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub disabled_by_default: bool,
    /// Faste attributter, f.eks. `supported_features` eller `options`.
    #[serde(skip_serializing_if = "Value::is_null")]
    pub capabilities: Value,
}

impl Entity {
    pub fn new(domain: &str, unique_id: impl Into<String>) -> Self {
        Self { domain: domain.into(), unique_id: unique_id.into(), ..Default::default() }
    }
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.into());
        self
    }
    pub fn device(mut self, device_id: &str) -> Self {
        self.device_id = Some(device_id.into());
        self
    }
    pub fn icon(mut self, icon: &str) -> Self {
        self.icon = Some(icon.into());
        self
    }
    pub fn device_class(mut self, c: &str) -> Self {
        self.device_class = Some(c.into());
        self
    }
    pub fn unit(mut self, u: &str) -> Self {
        self.unit_of_measurement = Some(u.into());
        self
    }
    pub fn state_class(mut self, c: &str) -> Self {
        self.state_class = Some(c.into());
        self
    }
    pub fn capabilities(mut self, c: Value) -> Self {
        self.capabilities = c;
        self
    }
    /// `diagnostic` eller `config`.
    pub fn category(mut self, c: &str) -> Self {
        self.entity_category = Some(c.into());
        self
    }
    pub fn disabled_by_default(mut self) -> Self {
        self.disabled_by_default = true;
        self
    }
}

// ---------------------------------------------------------------------------
// integrationen

/// Det, en integration implementerer. Kun `setup_entry` er påkrævet.
#[async_trait]
pub trait Integration: Send + Sync + 'static {
    /// Opsætningsforløbet. Første kald har `step` = kilden (`user` ...) og
    /// `input` = `None`. De næste har den sidste formulars `step_id`.
    async fn config_flow(&self, _host: &Host, _flow: Flow, _step: &str, _input: Option<Value>) -> Result<FlowReply, Error> {
        Ok(FlowReply::abort("not_supported"))
    }

    /// Indstillinger for en eksisterende opsætning (`flow.entry` er sat).
    async fn options_flow(&self, _host: &Host, _flow: Flow, _step: &str, _input: Option<Value>) -> Result<FlowReply, Error> {
        Ok(FlowReply::abort("not_supported"))
    }

    /// Sæt en opsætning op: registrér enheder og entities, start arbejde.
    async fn setup_entry(&self, host: &Host, entry: Entry) -> Result<(), Error>;

    /// Stop arbejdet for en opsætning. TheHome fjerner selv dens tilstande.
    async fn unload_entry(&self, _host: &Host, _entry_id: &str) -> Result<(), Error> {
        Ok(())
    }

    /// Et tjenestekald på en entity tilføjet med `handler = true`,
    /// f.eks. `switch.turn_on`.
    async fn entity_service(&self, _host: &Host, _entry_id: &str, entity_id: &str, service: &str, _data: Map<String, Value>) -> Result<(), Error> {
        Err(Error::not_supported(format!("{service} på {entity_id}")))
    }

    /// Et kald af en tjeneste registreret med [`Host::register_service`].
    async fn domain_service(&self, _host: &Host, _entry_id: &str, service: &str, _data: Map<String, Value>, _entity_ids: Vec<String>) -> Result<Value, Error> {
        Err(Error::not_supported(service))
    }

    /// Fejlsøgningsdata. Skjul hemmeligheder.
    async fn diagnostics(&self, _host: &Host, _entry: Entry) -> Value {
        Value::Null
    }

    /// Brugeren sletter en enhed.
    async fn remove_device(&self, _host: &Host, _entry: Entry, _device: Value) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// forbindelsen til TheHome

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, Error>>>>>;

/// Forbindelsen til TheHome. Billig at klone.
#[derive(Clone)]
pub struct Host {
    tx: mpsc::UnboundedSender<String>,
    pending: Pending,
    next_id: Arc<AtomicU64>,
    data_dir: Arc<Mutex<PathBuf>>,
}

impl Host {
    /// Integrationens egen mappe til filer. Den bevares ved opdateringer.
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.lock().unwrap().clone()
    }

    /// Kald en metode i TheHome og vent på svaret.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, Error> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        let msg = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        if self.tx.send(msg.to_string()).is_err() {
            return Err(Error::not_ready("forbindelsen til TheHome er lukket"));
        }
        match tokio::time::timeout(HOST_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(Error::not_ready("forbindelsen til TheHome er lukket")),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(Error::failed(format!("{method}: TheHome svarede ikke")))
            }
        }
    }

    /// Registrér en enhed. Returnerer dens id.
    pub async fn register_device(&self, entry_id: &str, device: Device) -> Result<String, Error> {
        let v = self.request("device/register", json!({ "entry_id": entry_id, "device": device })).await?;
        v.get("id").and_then(Value::as_str).map(String::from).ok_or_else(|| Error::failed("intet enheds-id"))
    }

    /// Tilføj en entity. Med `handler` sender TheHome dens tjenestekald til
    /// [`Integration::entity_service`]. Returnerer entity_id, eller `None`
    /// hvis brugeren har slået den fra.
    pub async fn add_entity(&self, entry_id: &str, entity: Entity, handler: bool) -> Result<Option<String>, Error> {
        let v = self.request("entity/add", json!({ "entry_id": entry_id, "entity": entity, "handler": handler })).await?;
        Ok(v.get("entity_id").and_then(Value::as_str).map(String::from))
    }

    pub async fn set_state(&self, entity_id: &str, state: impl Into<String>, attributes: Map<String, Value>) -> Result<(), Error> {
        self.request("state/set", json!({ "entity_id": entity_id, "state": state.into(), "attributes": attributes })).await.map(|_| ())
    }

    pub async fn set_unavailable(&self, entity_id: &str) -> Result<(), Error> {
        self.request("state/unavailable", json!({ "entity_id": entity_id })).await.map(|_| ())
    }

    /// Tilstanden af en vilkårlig entity, eller `Null`.
    pub async fn get_state(&self, entity_id: &str) -> Result<Value, Error> {
        self.request("state/get", json!({ "entity_id": entity_id })).await
    }

    /// Registrér tjenesten `<domæne>.<service>`. Kald ender i
    /// [`Integration::domain_service`].
    pub async fn register_service(&self, entry_id: &str, service: &str, description: Option<Value>) -> Result<(), Error> {
        self.request("service/register", json!({ "entry_id": entry_id, "service": service, "description": description })).await.map(|_| ())
    }

    /// Kald en tjeneste i TheHome, f.eks. `light.turn_on`.
    pub async fn call_service(&self, domain: &str, service: &str, data: Value) -> Result<Value, Error> {
        self.request("hub/call_service", json!({ "domain": domain, "service": service, "data": data })).await
    }

    pub async fn fire_event(&self, event_type: &str, data: Value) -> Result<(), Error> {
        self.request("event/fire", json!({ "event_type": event_type, "data": data })).await.map(|_| ())
    }

    /// Bed brugeren rette noget. `severity`: `warning` eller `error`.
    pub async fn create_issue(&self, entry_id: &str, issue_id: &str, severity: &str, title: &str, description: &str) -> Result<(), Error> {
        self.request(
            "issue/create",
            json!({ "entry_id": entry_id, "issue_id": issue_id, "severity": severity, "title": title, "description": description }),
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_issue(&self, entry_id: &str, issue_id: &str) -> Result<(), Error> {
        self.request("issue/delete", json!({ "entry_id": entry_id, "issue_id": issue_id })).await.map(|_| ())
    }

    /// Gem ny data (f.eks. et fornyet token) på opsætningen.
    pub async fn update_entry_data(&self, entry_id: &str, data: Value) -> Result<(), Error> {
        self.request("entry/update", json!({ "entry_id": entry_id, "data": data })).await.map(|_| ())
    }
}

/// Skriv til TheHomes log. `level`: `ERROR`, `WARN`, `INFO` eller `DEBUG`.
pub fn log(level: &str, message: impl std::fmt::Display) {
    eprintln!("{level} {message}");
}

// ---------------------------------------------------------------------------
// løkken

/// Kør integrationen, indtil TheHome lukker stdin eller beder om at stoppe.
pub fn run<I: Integration>(integration: I) {
    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");
    rt.block_on(serve_io(integration, tokio::io::stdin(), tokio::io::stdout()));
}

/// Som [`run`], men over vilkårlige strømme. Til tests, der spiller TheHome
/// over et rør (f.eks. `tokio::io::duplex`). Returnerer, når `input` lukkes
/// eller TheHome sender `shutdown`.
pub async fn serve_io<I, R, W>(integration: I, input: R, output: W)
where
    I: Integration,
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let integration = Arc::new(integration);
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let host = Host {
        tx,
        pending: Arc::default(),
        next_id: Arc::new(AtomicU64::new(1)),
        data_dir: Arc::new(Mutex::new(std::env::var("THEHOME_DATA_DIR").map(PathBuf::from).unwrap_or_else(|_| PathBuf::from("data")))),
    };

    let writer = tokio::spawn(async move {
        let mut out = output;
        while let Some(line) = rx.recv().await {
            if out.write_all(line.as_bytes()).await.is_err() || out.write_all(b"\n").await.is_err() || out.flush().await.is_err() {
                break;
            }
        }
    });

    let mut lines = BufReader::new(input).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(msg) = serde_json::from_str::<Value>(&line) else {
            log("WARN", format!("ugyldig besked fra TheHome: {line}"));
            continue;
        };
        let Some(method) = msg.get("method").and_then(Value::as_str).map(String::from) else {
            // svar på et af vores egne kald
            if let Some(id) = msg.get("id").and_then(Value::as_u64) {
                if let Some(tx) = host.pending.lock().unwrap().remove(&id) {
                    let res = match msg.get("error").filter(|e| !e.is_null()) {
                        Some(e) => Err(Error {
                            kind: e.pointer("/data/kind").and_then(Value::as_str).unwrap_or("failed").into(),
                            message: e.get("message").and_then(Value::as_str).unwrap_or_default().into(),
                        }),
                        None => Ok(msg.get("result").cloned().unwrap_or(Value::Null)),
                    };
                    let _ = tx.send(res);
                }
            }
            continue;
        };
        if method == "shutdown" {
            break;
        }
        let id = msg.get("id").cloned().filter(|v| !v.is_null());
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        let host = host.clone();
        let integration = integration.clone();
        tokio::spawn(async move {
            let res = handle(&*integration, &host, &method, params).await;
            if let Some(id) = id {
                let reply = match res {
                    Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                    Err(e) => json!({ "jsonrpc": "2.0", "id": id, "error": to_wire(&e) }),
                };
                let _ = host.tx.send(reply.to_string());
            }
        });
    }
    drop(host);
    writer.abort();
}

fn param<T: serde::de::DeserializeOwned>(p: &Value, key: &str) -> Result<T, Error> {
    serde_json::from_value(p.get(key).cloned().unwrap_or(Value::Null)).map_err(|e| Error::invalid(format!("{key}: {e}")))
}

fn str_param(p: &Value, key: &str) -> String {
    p.get(key).and_then(Value::as_str).unwrap_or_default().to_string()
}

async fn handle<I: Integration>(i: &I, host: &Host, method: &str, p: Value) -> Result<Value, Error> {
    match method {
        "initialize" => {
            if let Some(dir) = p.get("data_dir").and_then(Value::as_str) {
                *host.data_dir.lock().unwrap() = PathBuf::from(dir);
            }
            Ok(json!({ "api_version": API_VERSION }))
        }
        "config_flow" | "options_flow" => {
            let flow: Flow = param(&p, "flow")?;
            let step = str_param(&p, "step");
            let input = p.get("input").cloned().filter(|v| !v.is_null());
            let reply = if method == "config_flow" {
                i.config_flow(host, flow, &step, input).await?
            } else {
                i.options_flow(host, flow, &step, input).await?
            };
            Ok(json!(reply))
        }
        "setup_entry" => i.setup_entry(host, param(&p, "entry")?).await.map(|_| Value::Null),
        "unload_entry" => i.unload_entry(host, &str_param(&p, "entry_id")).await.map(|_| Value::Null),
        "entity/call_service" => {
            let data = p.get("data").and_then(Value::as_object).cloned().unwrap_or_default();
            i.entity_service(host, &str_param(&p, "entry_id"), &str_param(&p, "entity_id"), &str_param(&p, "service"), data)
                .await
                .map(|_| Value::Null)
        }
        "service/call" => {
            let data = p.get("data").and_then(Value::as_object).cloned().unwrap_or_default();
            let ids: Vec<String> = param(&p, "entity_ids").unwrap_or_default();
            i.domain_service(host, &str_param(&p, "entry_id"), &str_param(&p, "service"), data, ids).await
        }
        "diagnostics" => Ok(i.diagnostics(host, param(&p, "entry")?).await),
        "remove_device" => i.remove_device(host, param(&p, "entry")?, p.get("device").cloned().unwrap_or(Value::Null)).await.map(|_| Value::Null),
        _ => Err(Error::not_supported(format!("ukendt metode {method}"))),
    }
}

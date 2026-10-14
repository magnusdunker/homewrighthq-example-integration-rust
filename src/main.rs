//! Eksempel-tæller: en ekstern integration til TheHome.
//!
//! Hver opsætning får en enhed med
//! - en sensor, der tæller én op med et fast interval,
//! - en kontakt, der sætter tælleren på pause,
//! - en knap, der nulstiller den.
//!
//! Den viser også et opsætningsforløb med validering, et options-forløb,
//! en tjeneste (`example_counter.set`), en hændelse og en fil i
//! integrationens datamappe, så tallet overlever en genstart.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use thehome_plugin::*;

const DOMAIN: &str = "example_counter";

struct Counter {
    count: u64,
    running: bool,
    count_id: Option<String>,
    running_id: Option<String>,
    reset_id: Option<String>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct Example {
    counters: Arc<Mutex<HashMap<String, Counter>>>,
}

fn user_schema(name: &str, interval: u64) -> Value {
    json!({
        "type": "object",
        "required": ["name", "interval"],
        "properties": {
            "name": { "type": "string", "title": "Navn", "default": name },
            "interval": { "type": "integer", "title": "Interval (sekunder)", "minimum": 1, "maximum": 3600, "default": interval,
                          "description": "Hvor tit tælleren tæller én op" }
        }
    })
}

fn options_schema(interval: u64) -> Value {
    json!({
        "type": "object",
        "required": ["interval"],
        "properties": {
            "interval": { "type": "integer", "title": "Interval (sekunder)", "minimum": 1, "maximum": 3600, "default": interval }
        }
    })
}

/// `example_counter.set` gælder alle opsætninger; målet afgør hvilken.
/// TheHome knytter tjenesten til den opsætning, der registrerede den.
async fn register_set_service(host: &Host, entry_id: &str) -> Result<(), Error> {
    host.register_service(
        entry_id,
        "set",
        Some(json!({
            "name": "Sæt tæller",
            "description": "Sætter en tæller til en bestemt værdi",
            "target": { "entity": { "integration": DOMAIN, "domain": "sensor" } },
            "fields": { "value": { "name": "Værdi", "required": true, "example": 100, "selector": { "number": { "min": 0, "max": 1000000 } } } }
        })),
    )
    .await
}

fn interval_of(entry: &Entry) -> u64 {
    entry.get_f64("interval").map(|v| v as u64).unwrap_or(10).clamp(1, 3600)
}

impl Example {
    fn file(host: &Host, entry_id: &str) -> std::path::PathBuf {
        host.data_dir().join(format!("{entry_id}.json"))
    }

    fn load(host: &Host, entry_id: &str) -> u64 {
        std::fs::read_to_string(Self::file(host, entry_id))
            .ok()
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .and_then(|v| v.get("count").and_then(Value::as_u64))
            .unwrap_or(0)
    }

    fn save(host: &Host, entry_id: &str, count: u64) {
        let _ = std::fs::create_dir_all(host.data_dir());
        if let Err(e) = std::fs::write(Self::file(host, entry_id), json!({ "count": count }).to_string()) {
            log("WARN", format!("kunne ikke gemme tælleren: {e}"));
        }
    }

    /// Send tællerens og kontaktens tilstand til TheHome.
    async fn publish(&self, host: &Host, entry_id: &str) -> Result<(), Error> {
        let (count, running, count_id, running_id) = {
            let c = self.counters.lock().unwrap();
            let Some(c) = c.get(entry_id) else { return Ok(()) };
            (c.count, c.running, c.count_id.clone(), c.running_id.clone())
        };
        if let Some(id) = count_id {
            host.set_state(&id, count.to_string(), Map::new()).await?;
        }
        if let Some(id) = running_id {
            host.set_state(&id, if running { "on" } else { "off" }, Map::new()).await?;
        }
        Ok(())
    }

    async fn set_count(&self, host: &Host, entry_id: &str, value: u64) -> Result<(), Error> {
        {
            let mut c = self.counters.lock().unwrap();
            let c = c.get_mut(entry_id).ok_or_else(|| Error::not_ready("opsætningen kører ikke"))?;
            c.count = value;
        }
        Self::save(host, entry_id, value);
        self.publish(host, entry_id).await
    }

    async fn reset(&self, host: &Host, entry_id: &str) -> Result<(), Error> {
        self.set_count(host, entry_id, 0).await?;
        let reset_id = self.counters.lock().unwrap().get(entry_id).and_then(|c| c.reset_id.clone());
        if let Some(id) = reset_id {
            // en knaps tilstand er tidspunktet for sidste tryk
            host.set_state(&id, chrono::Utc::now().to_rfc3339(), Map::new()).await?;
        }
        host.fire_event("example_counter_reset", json!({ "entry_id": entry_id })).await
    }
}

#[async_trait]
impl Integration for Example {
    async fn config_flow(&self, _host: &Host, _flow: Flow, step: &str, input: Option<Value>) -> Result<FlowReply, Error> {
        match (step, input) {
            ("user", None) => Ok(FlowReply::form("user", user_schema("Tæller", 10))),
            ("user", Some(v)) => {
                let name = v.get("name").and_then(Value::as_str).unwrap_or_default().trim().to_string();
                let interval = v.get("interval").and_then(Value::as_u64).unwrap_or(10);
                if name.is_empty() {
                    return Ok(FlowReply::form_with_errors("user", user_schema("", interval), json!({ "name": "required" })));
                }
                Ok(FlowReply::create(&name, json!({ "name": name, "interval": interval })).with_unique_id(name.to_lowercase()))
            }
            (s, _) => Err(Error::invalid(format!("ukendt trin {s}"))),
        }
    }

    async fn options_flow(&self, _host: &Host, flow: Flow, _step: &str, input: Option<Value>) -> Result<FlowReply, Error> {
        let current = flow.entry.as_ref().map(interval_of).unwrap_or(10);
        match input {
            None => Ok(FlowReply::form("init", options_schema(current))),
            Some(v) => {
                let interval = v.get("interval").and_then(Value::as_u64).unwrap_or(current);
                Ok(FlowReply::create("", json!({ "interval": interval })))
            }
        }
    }

    async fn setup_entry(&self, host: &Host, entry: Entry) -> Result<(), Error> {
        let id = entry.entry_id.clone();
        let name = entry.get_str("name").unwrap_or(&entry.title).to_string();
        let interval = interval_of(&entry);

        let device = host
            .register_device(&id, Device {
                identifiers: vec![(DOMAIN.into(), id.clone())],
                name: name.clone(),
                manufacturer: Some("TheHome".into()),
                model: Some("Eksempel-tæller".into()),
                sw_version: Some(env!("CARGO_PKG_VERSION").into()),
                ..Default::default()
            })
            .await?;
        let count_id = host
            .add_entity(&id, Entity::new("sensor", format!("{id}-count")).name("Antal").device(&device).icon("mdi:counter").state_class("total_increasing"), false)
            .await?;
        let running_id = host
            .add_entity(&id, Entity::new("switch", format!("{id}-running")).name("Tæller").device(&device).icon("mdi:play-pause"), true)
            .await?;
        let reset_id = host
            .add_entity(&id, Entity::new("button", format!("{id}-reset")).name("Nulstil").device(&device).icon("mdi:restart"), true)
            .await?;
        register_set_service(host, &id).await?;

        let count = Self::load(host, &id);
        self.counters.lock().unwrap().insert(
            id.clone(),
            Counter { count, running: true, count_id, running_id, reset_id: reset_id.clone(), task: None },
        );
        if let Some(r) = &reset_id {
            host.set_state(r, "unknown", Map::new()).await?;
        }
        self.publish(host, &id).await?;

        let counters = self.counters.clone();
        let h = host.clone();
        let entry_id = id.clone();
        let task = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(interval));
            tick.tick().await;
            loop {
                tick.tick().await;
                let next = {
                    let mut c = counters.lock().unwrap();
                    match c.get_mut(&entry_id) {
                        Some(c) if c.running => {
                            c.count += 1;
                            Some((c.count, c.count_id.clone()))
                        }
                        Some(_) => None,
                        None => return,
                    }
                };
                if let Some((n, Some(sensor))) = next {
                    Example::save(&h, &entry_id, n);
                    if let Err(e) = h.set_state(&sensor, n.to_string(), Map::new()).await {
                        log("WARN", format!("kunne ikke opdatere {sensor}: {e}"));
                    }
                }
            }
        });
        if let Some(c) = self.counters.lock().unwrap().get_mut(&id) {
            c.task = Some(task);
        }
        log("INFO", format!("{name} tæller hvert {interval}. sekund fra {count}"));
        Ok(())
    }

    async fn unload_entry(&self, host: &Host, entry_id: &str) -> Result<(), Error> {
        let other = {
            let mut c = self.counters.lock().unwrap();
            if let Some(t) = c.remove(entry_id).and_then(|c| c.task) {
                t.abort();
            }
            c.keys().next().cloned()
        };
        // TheHome fjerner tjenesten med opsætningen; de andre skal stadig have den
        if let Some(other) = other {
            register_set_service(host, &other).await?;
        }
        Ok(())
    }

    async fn entity_service(&self, host: &Host, entry_id: &str, entity_id: &str, service: &str, _data: Map<String, Value>) -> Result<(), Error> {
        let (is_switch, is_button) = {
            let c = self.counters.lock().unwrap();
            let c = c.get(entry_id).ok_or_else(|| Error::unavailable(entity_id))?;
            (c.running_id.as_deref() == Some(entity_id), c.reset_id.as_deref() == Some(entity_id))
        };
        match (is_switch, is_button, service) {
            (true, _, "turn_on" | "turn_off" | "toggle") => {
                if let Some(c) = self.counters.lock().unwrap().get_mut(entry_id) {
                    c.running = match service {
                        "turn_on" => true,
                        "turn_off" => false,
                        _ => !c.running,
                    };
                }
                self.publish(host, entry_id).await
            }
            (_, true, "press") => self.reset(host, entry_id).await,
            _ => Err(Error::not_supported(format!("{service} på {entity_id}"))),
        }
    }

    async fn domain_service(&self, host: &Host, _entry_id: &str, service: &str, data: Map<String, Value>, entity_ids: Vec<String>) -> Result<Value, Error> {
        match service {
            "set" => {
                let value = data.get("value").and_then(Value::as_f64).ok_or_else(|| Error::invalid("value mangler"))?;
                if value < 0.0 {
                    return Err(Error::invalid("value skal være 0 eller mere"));
                }
                let targets: Vec<String> = {
                    let c = self.counters.lock().unwrap();
                    c.iter()
                        .filter(|(_, c)| [&c.count_id, &c.running_id].iter().any(|id| id.as_ref().is_some_and(|id| entity_ids.contains(id))))
                        .map(|(id, _)| id.clone())
                        .collect()
                };
                if targets.is_empty() {
                    return Err(Error::invalid("vælg en tæller som mål"));
                }
                for entry_id in targets {
                    self.set_count(host, &entry_id, value as u64).await?;
                }
                Ok(Value::Null)
            }
            _ => Err(Error::not_supported(service)),
        }
    }

    async fn diagnostics(&self, _host: &Host, entry: Entry) -> Value {
        let c = self.counters.lock().unwrap();
        match c.get(&entry.entry_id) {
            Some(c) => json!({ "count": c.count, "running": c.running, "interval": interval_of(&entry) }),
            None => json!({ "loaded": false }),
        }
    }
}

fn main() {
    run(Example::default());
}

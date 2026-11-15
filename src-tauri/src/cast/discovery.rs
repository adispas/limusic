//! Finding Cast devices on the local network: mDNS, service `_googlecast._tcp`. Each answer
//! carries the device's id, friendly name and model in its TXT record.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use mdns_sd::{ServiceDaemon, ServiceEvent};

const SERVICE: &str = "_googlecast._tcp.local.";

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub id: String,
    pub name: String,
    /// "Google Nest Mini", "Chromecast", "Google Cast Group"…
    pub model: String,
    #[serde(skip)]
    pub addr: SocketAddr,
    /// mDNS instance name, which is what a removal names.
    #[serde(skip)]
    instance: String,
}

pub type Devices = Arc<Mutex<BTreeMap<String, Device>>>;

/// A running browse. Dropping it stops looking (devices already found stay known, so a connect
/// right after closing the picker still works).
pub struct Discovery {
    daemon: ServiceDaemon,
}

impl Discovery {
    /// Starts browsing; `changed` gets the whole list whenever it changes, sorted by name.
    pub fn start(
        devices: Devices,
        changed: impl Fn(Vec<Device>) + Send + 'static,
    ) -> anyhow::Result<Self> {
        let daemon = ServiceDaemon::new()?;
        let events = daemon.browse(SERVICE)?;
        tauri::async_runtime::spawn(async move {
            while let Ok(event) = events.recv_async().await {
                let touched = match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let txt =
                            |k: &str| info.get_property_val_str(k).unwrap_or_default().to_owned();
                        // IPv4 first: every Cast device has one, and it is what the LAN proxy binds.
                        let mut addrs: Vec<IpAddr> =
                            info.get_addresses_v4().into_iter().map(IpAddr::V4).collect();
                        addrs.sort();
                        let Some(ip) = addrs.first().copied() else { continue };
                        let instance = info.get_fullname().to_owned();
                        let id = Some(txt("id"))
                            .filter(|s| !s.is_empty())
                            .unwrap_or_else(|| instance.clone());
                        let name = Some(txt("fn")).filter(|s| !s.is_empty()).unwrap_or_else(|| {
                            instance.trim_end_matches(SERVICE).trim_end_matches('.').to_owned()
                        });
                        let device = Device {
                            id: id.clone(),
                            name,
                            model: txt("md"),
                            addr: SocketAddr::new(ip, info.get_port()),
                            instance,
                        };
                        devices.lock().unwrap().insert(id, device);
                        true
                    }
                    ServiceEvent::ServiceRemoved(_, instance) => {
                        let mut d = devices.lock().unwrap();
                        let before = d.len();
                        d.retain(|_, dev| dev.instance != instance);
                        before != d.len()
                    }
                    _ => false,
                };
                if touched {
                    changed(list(&devices));
                }
            }
        });
        Ok(Discovery { daemon })
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.daemon.stop_browse(SERVICE);
        let _ = self.daemon.shutdown();
    }
}

pub fn list(devices: &Devices) -> Vec<Device> {
    let mut v: Vec<Device> = devices.lock().unwrap().values().cloned().collect();
    v.sort_by_key(|d| d.name.to_lowercase());
    v
}

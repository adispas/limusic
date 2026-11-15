//! Google Cast: play to a Chromecast, a Google TV or a Nest speaker on the same network.
//!
//! - Devices are found over mDNS ([`discovery`]) while the picker is open.
//! - A session runs Google's Default Media Receiver on the device ([`protocol`] speaks to it), so
//!   there is no receiver app to register or host.
//! - The device streams from this computer: the audio proxy serves the same registrations it
//!   serves mpv on a LAN address too (`audioproxy::lan_url`), because a googlevideo URL only
//!   works for the client that resolved it.
//! - The player hands everything it would do to mpv to [`CastOutput`] instead (`player::remote`),
//!   and the device's status comes back as the player's own events. The queue, radio, repeat,
//!   history, scrobbling and Listen Together keep working as they do locally.
//!
//! Casting is audio only. What the device can't do (loudness gain, tempo, pitch, reverb,
//! crossfade, the music video) simply doesn't apply while casting. Local files and karaoke
//! instrumentals are served to the device too.

mod discovery;
pub mod protocol;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use player::{RemoteHandle, RemoteOutput};
use serde_json::{json, Value};
use tauri::{AppHandle, Emitter, State};
use tokio::sync::mpsc;

use crate::state::AppState;
use discovery::{Device, Devices, Discovery};
use protocol::{Channel, Message, NS_CONNECTION, NS_MEDIA, NS_RECEIVER, RECEIVER};

const DEFAULT_MEDIA_RECEIVER: &str = "CC1AD845";

/// What the player asks of the device. Queued, so the player never waits on the network.
enum Cmd {
    Load {
        url: String,
        start: Option<f64>,
        paused: bool,
        live: bool,
    },
    Play,
    Pause,
    Seek(f64),
    Stop,
    Volume(i64),
    /// End the session: stop the receiver app and close the connection.
    Close,
}

struct CastOutput {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl RemoteOutput for CastOutput {
    fn load(&self, url: &str, start: Option<f64>, paused: bool, live: bool) {
        let _ = self.tx.send(Cmd::Load { url: url.to_owned(), start, paused, live });
    }
    fn play(&self) {
        let _ = self.tx.send(Cmd::Play);
    }
    fn pause(&self) {
        let _ = self.tx.send(Cmd::Pause);
    }
    fn seek(&self, secs: f64) {
        let _ = self.tx.send(Cmd::Seek(secs));
    }
    fn stop(&self) {
        let _ = self.tx.send(Cmd::Stop);
    }
    fn set_volume(&self, percent: i64) {
        let _ = self.tx.send(Cmd::Volume(percent));
    }
}

struct Session {
    device: Device,
    tx: mpsc::UnboundedSender<Cmd>,
    id: u64,
}

/// The app's casting state (managed by Tauri, one per app).
pub struct Cast {
    app: AppHandle,
    devices: Devices,
    discovery: Mutex<Option<Discovery>>,
    session: Mutex<Option<Session>>,
    /// Serializes connect/disconnect, which take seconds and touch the player.
    switching: tokio::sync::Mutex<()>,
    next_id: AtomicU64,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CastStatus {
    devices: Vec<Device>,
    connected: Option<Device>,
}

impl Cast {
    pub fn new(app: AppHandle) -> Arc<Self> {
        Arc::new(Cast {
            app,
            devices: Devices::default(),
            discovery: Mutex::new(None),
            session: Mutex::new(None),
            switching: tokio::sync::Mutex::new(()),
            next_id: AtomicU64::new(1),
        })
    }

    fn connected(&self) -> Option<Device> {
        self.session.lock().unwrap().as_ref().map(|s| s.device.clone())
    }

    fn emit_session(&self) {
        let _ = self.app.emit("cast-session", json!({ "connected": self.connected() }));
    }

    /// A problem the user should hear about, as a code the UI translates ("lost", "takenOver"…)
    /// or a message.
    fn emit_error(&self, message: &str) {
        let _ = self.app.emit("cast-error", json!({ "message": message }));
    }

    pub fn discover(&self, on: bool) -> Result<(), String> {
        let mut d = self.discovery.lock().unwrap();
        if !on {
            *d = None;
            return Ok(());
        }
        if d.is_none() {
            let app = self.app.clone();
            *d = Some(
                Discovery::start(self.devices.clone(), move |list| {
                    let _ = app.emit("cast-devices", list);
                })
                .map_err(|e| e.to_string())?,
            );
        }
        Ok(())
    }

    pub fn status(&self) -> CastStatus {
        CastStatus { devices: discovery::list(&self.devices), connected: self.connected() }
    }

    pub async fn connect(self: &Arc<Self>, state: Arc<AppState>, id: &str) -> anyhow::Result<()> {
        let _guard = self.switching.lock().await;
        let device = self
            .devices
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("The device is no longer on the network."))?;
        if self.connected().is_some_and(|d| d.id == device.id) {
            return Ok(());
        }

        let (channel, inbox) = Channel::open(device.addr).await?;
        let status = channel
            .request(RECEIVER, NS_RECEIVER, json!({"type": "GET_STATUS"}), Duration::from_secs(10))
            .await?
            .data;
        let mut app = find_app(&status["status"]);
        if app.is_none() {
            let reply = channel
                .request(
                    RECEIVER,
                    NS_RECEIVER,
                    json!({"type": "LAUNCH", "appId": DEFAULT_MEDIA_RECEIVER}),
                    Duration::from_secs(20),
                )
                .await?
                .data;
            app = find_app(&reply["status"]);
        }
        let (session_id, transport) =
            app.ok_or_else(|| anyhow::anyhow!("The device didn't start the player."))?;
        channel.send(&transport, NS_CONNECTION, json!({"type": "CONNECT"}));
        if crate::audioproxy::address_facing(device.addr.ip()).is_none() {
            channel.send(RECEIVER, NS_RECEIVER, json!({"type": "STOP", "sessionId": session_id}));
            anyhow::bail!("noNetwork");
        }

        // The previous device, if any, gives the track to this one (`attach_remote` carries it).
        if let Some(old) = self.session.lock().unwrap().take() {
            let _ = old.tx.send(Cmd::Close);
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let handle = state.player.attach_remote(Arc::new(CastOutput { tx: tx.clone() }));
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        *self.session.lock().unwrap() = Some(Session { device: device.clone(), tx, id });
        tracing::info!(device = %device.name, model = %device.model, "cast: connected");

        let task = SessionTask {
            cast: self.clone(),
            state,
            id,
            device,
            channel,
            transport,
            session_id,
            handle,
            media_session: None,
            loading: false,
            finished_for: None,
            clock: Clock::default(),
        };
        tauri::async_runtime::spawn(task.run(inbox, rx));
        self.emit_session();
        Ok(())
    }

    /// Stop casting: the track comes back to this computer where the device left it, paused.
    pub async fn disconnect(&self, state: &AppState) {
        let _guard = self.switching.lock().await;
        self.end(state, None);
    }

    /// Ends the session `id` (or whichever is current, for `None`), unless another replaced it.
    fn end(&self, state: &AppState, id: Option<u64>) -> bool {
        let session = {
            let mut s = self.session.lock().unwrap();
            if id.is_some() && s.as_ref().map(|s| s.id) != id {
                return false;
            }
            s.take()
        };
        let Some(session) = session else { return false };
        state.player.detach_remote();
        let _ = session.tx.send(Cmd::Close);
        crate::audioproxy::stop_lan();
        tracing::info!(device = %session.device.name, "cast: disconnected");
        self.emit_session();
        true
    }

    /// On quit: the speaker stops instead of playing out what it buffered.
    pub fn shutdown(&self) {
        if let Some(s) = self.session.lock().unwrap().take() {
            let _ = s.tx.send(Cmd::Close);
        }
    }
}

fn find_app(status: &Value) -> Option<(String, String)> {
    status["applications"]
        .as_array()?
        .iter()
        .find(|a| a["appId"] == DEFAULT_MEDIA_RECEIVER)
        .and_then(|a| {
            Some((a["sessionId"].as_str()?.to_owned(), a["transportId"].as_str()?.to_owned()))
        })
}

/// The device reports its position on changes and when asked; in between it is interpolated, so
/// the seek bar moves as smoothly as it does for local playback.
#[derive(Default)]
struct Clock {
    position: f64,
    at: Option<Instant>,
    playing: bool,
}

impl Clock {
    fn set(&mut self, position: f64, playing: bool) {
        self.position = position;
        self.at = Some(Instant::now());
        self.playing = playing;
    }
    fn now(&self) -> Option<f64> {
        let at = self.at?;
        Some(if self.playing { self.position + at.elapsed().as_secs_f64() } else { self.position })
    }
}

struct SessionTask {
    cast: Arc<Cast>,
    state: Arc<AppState>,
    id: u64,
    device: Device,
    channel: Channel,
    transport: String,
    session_id: String,
    handle: RemoteHandle,
    /// The media session the device gave the track it is playing.
    media_session: Option<i64>,
    /// A LOAD is in flight: the statuses until its reply describe the previous track.
    loading: bool,
    /// The media session already reported finished (the device repeats an idle status).
    finished_for: Option<i64>,
    clock: Clock,
}

impl SessionTask {
    async fn run(
        mut self,
        mut inbox: mpsc::UnboundedReceiver<Message>,
        mut rx: mpsc::UnboundedReceiver<Cmd>,
    ) {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        let mut since_poll = 0u32;
        let mut pending: VecDeque<Cmd> = VecDeque::new();
        loop {
            // Commands the player queued while a LOAD was in flight run first, in order.
            let cmd = if let Some(c) = pending.pop_front() {
                Some(c)
            } else {
                tokio::select! {
                    msg = inbox.recv() => match msg {
                        Some(m) => { self.on_message(m); continue; }
                        None => { self.lost("lost"); return; }
                    },
                    c = rx.recv() => c,
                    _ = tick.tick() => {
                        if let Some(p) = self.clock.now().filter(|_| self.clock.playing) {
                            self.handle.position(p);
                        }
                        since_poll += 1;
                        if since_poll >= 4 {
                            since_poll = 0;
                            self.poll();
                        }
                        continue;
                    }
                }
            };
            let Some(cmd) = cmd else { return };
            match cmd {
                Cmd::Load { url, start, paused, live } => {
                    // A skip queued behind this one makes it moot: only the newest load runs.
                    while let Ok(c) = rx.try_recv() {
                        pending.push_back(c);
                    }
                    if pending
                        .iter()
                        .any(|c| matches!(c, Cmd::Load { .. } | Cmd::Stop | Cmd::Close))
                    {
                        continue;
                    }
                    self.load(&url, start, paused, live).await;
                }
                Cmd::Play => self.media("PLAY", json!({})),
                Cmd::Pause => self.media("PAUSE", json!({})),
                Cmd::Seek(s) => {
                    self.clock.set(s, self.clock.playing);
                    self.media("SEEK", json!({"currentTime": s}));
                }
                Cmd::Stop => {
                    self.media("STOP", json!({}));
                    self.media_session = None;
                    self.clock = Clock::default();
                }
                Cmd::Volume(percent) => {
                    let level = (percent.clamp(0, 100) as f64) / 100.0;
                    self.channel.command(
                        RECEIVER,
                        NS_RECEIVER,
                        json!({"type": "SET_VOLUME", "volume": {"level": level}}),
                    );
                }
                Cmd::Close => {
                    let _ = self
                        .channel
                        .request(
                            RECEIVER,
                            NS_RECEIVER,
                            json!({"type": "STOP", "sessionId": self.session_id}),
                            Duration::from_secs(3),
                        )
                        .await;
                    return;
                }
            }
        }
    }

    fn media(&self, kind: &str, mut data: Value) {
        let Some(id) = self.media_session else { return };
        data["type"] = json!(kind);
        data["mediaSessionId"] = json!(id);
        self.channel.command(&self.transport, NS_MEDIA, data);
    }

    fn poll(&self) {
        if let Some(id) = self.media_session {
            self.channel.command(
                &self.transport,
                NS_MEDIA,
                json!({"type": "GET_STATUS", "mediaSessionId": id}),
            );
        }
    }

    async fn load(&mut self, url: &str, start: Option<f64>, paused: bool, live: bool) {
        let ip = self.device.addr.ip();
        let (content_url, content_type, stream_type) = if let Some(lan) =
            crate::audioproxy::lan_url(url, ip)
        {
            let ct = crate::audioproxy::stream_content_type(url)
                .await
                .unwrap_or_else(|| "audio/webm".to_owned());
            (lan, ct, "BUFFERED")
        } else if url.starts_with("http://") || url.starts_with("https://") {
            // Not one of ours: a podcast's own file or a live playlist, which the device fetches
            // itself.
            if live {
                (url.to_owned(), "application/x-mpegURL".to_owned(), "LIVE")
            } else {
                (url.to_owned(), guess_type(url).to_owned(), "BUFFERED")
            }
        } else if let Some(lan) = crate::audioproxy::lan_file_url(url, ip) {
            // A file on this computer: local music, or a finished karaoke instrumental.
            (lan, crate::audioproxy::file_content_type(url).to_owned(), "BUFFERED")
        } else {
            self.handle.failed("This track couldn't be shared with the cast device.".to_owned());
            return;
        };

        let mut metadata = json!({"metadataType": 3}); // MUSIC_TRACK
        if let Some(video_id) = crate::audioproxy::video_id_of(url) {
            if let Some((title, artists, album, art)) = self.state.cast_metadata(&video_id).await {
                metadata["title"] = json!(title);
                metadata["artist"] = json!(artists);
                if let Some(a) = album {
                    metadata["albumName"] = json!(a);
                }
                if let Some(a) = art {
                    metadata["images"] = json!([{ "url": a }]);
                }
            }
        }

        self.loading = true;
        let mut request = json!({
            "type": "LOAD",
            "autoplay": !paused,
            "media": {
                "contentId": content_url,
                "contentUrl": content_url,
                "contentType": content_type,
                "streamType": stream_type,
                "metadata": metadata,
            },
        });
        if let Some(s) = start.filter(|_| !live) {
            request["currentTime"] = json!(s);
        }
        let reply =
            self.channel.request(&self.transport, NS_MEDIA, request, Duration::from_secs(30)).await;
        self.loading = false;
        match reply {
            Ok(m) if m.data["type"] == "MEDIA_STATUS" => {
                self.media_session = m.data["status"][0]["mediaSessionId"].as_i64();
                self.finished_for = None;
                self.clock.set(start.unwrap_or(0.0), !paused);
                self.apply_statuses(&m.data["status"]);
            }
            Ok(m) => {
                tracing::warn!(reply = %m.data, "cast: LOAD refused");
                self.handle.failed("The cast device couldn't play this track.".to_owned());
            }
            Err(e) => {
                tracing::warn!(error = %e, "cast: LOAD failed");
                if !self.channel.is_closed() {
                    self.handle.failed(format!("The cast device didn't start this track ({e})."));
                }
            }
        }
    }

    fn on_message(&mut self, m: Message) {
        let kind = m.data["type"].as_str().unwrap_or_default();
        if m.namespace == NS_RECEIVER && kind == "RECEIVER_STATUS" {
            let ours = m.data["status"]["applications"].as_array().is_some_and(|apps| {
                apps.iter().any(|a| a["sessionId"] == self.session_id.as_str())
            });
            if !ours {
                // Someone cast something else, or stopped it from the Google Home app.
                self.lost("takenOver");
            }
            return;
        }
        if m.namespace == NS_CONNECTION && kind == "CLOSE" && m.source == self.transport {
            self.lost("lost");
            return;
        }
        if m.namespace == NS_MEDIA
            && kind == "MEDIA_STATUS"
            && m.source == self.transport
            && !self.loading
        {
            self.apply_statuses(&m.data["status"]);
        }
    }

    fn apply_statuses(&mut self, statuses: &Value) {
        let Some(list) = statuses.as_array() else { return };
        for s in list {
            let id = s["mediaSessionId"].as_i64();
            if id.is_none() || id != self.media_session {
                continue; // the previous track's session ending
            }
            if let Some(d) = s["media"]["duration"].as_f64() {
                self.handle.duration(d);
            }
            let position = s["currentTime"].as_f64();
            match s["playerState"].as_str().unwrap_or_default() {
                "PLAYING" => {
                    if let Some(p) = position {
                        self.clock.set(p, true);
                        self.handle.position(p);
                    }
                    self.handle.playing(true);
                }
                "PAUSED" => {
                    if let Some(p) = position {
                        self.clock.set(p, false);
                        self.handle.position(p);
                    }
                    self.handle.playing(false);
                }
                "BUFFERING" => {
                    if let Some(p) = position {
                        self.clock.set(p, false); // don't run the clock ahead of a stalled stream
                    }
                }
                "IDLE" => {
                    self.clock.playing = false;
                    match s["idleReason"].as_str() {
                        Some("FINISHED") if self.finished_for != id => {
                            self.finished_for = id;
                            self.handle.finished();
                        }
                        Some("ERROR") if self.finished_for != id => {
                            self.finished_for = id;
                            self.handle
                                .failed("The cast device couldn't play this track.".to_owned());
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
    }

    /// The session ended without the user stopping it: bring playback back here and say why.
    fn lost(&self, code: &str) {
        if self.cast.end(&self.state, Some(self.id)) {
            self.cast.emit_error(code);
        }
    }
}

fn guess_type(url: &str) -> &'static str {
    let path = url.split(['?', '#']).next().unwrap_or(url).to_ascii_lowercase();
    if path.ends_with(".m4a") || path.ends_with(".mp4") || path.ends_with(".aac") {
        "audio/mp4"
    } else if path.ends_with(".ogg") || path.ends_with(".opus") {
        "audio/ogg"
    } else if path.ends_with(".webm") {
        "audio/webm"
    } else if path.ends_with(".flac") {
        "audio/flac"
    } else {
        "audio/mpeg" // podcasts, mostly
    }
}

// --- Tauri commands (ui/src/lib/cast.svelte.ts)

type CastState<'a> = State<'a, Arc<Cast>>;

#[tauri::command]
pub fn cast_discover(cast: CastState<'_>, on: bool) -> Result<(), String> {
    cast.discover(on)
}

#[tauri::command]
pub fn cast_status(cast: CastState<'_>) -> CastStatus {
    cast.status()
}

#[tauri::command]
pub async fn cast_connect(
    cast: CastState<'_>,
    state: State<'_, Arc<AppState>>,
    id: String,
) -> Result<(), String> {
    let cast = cast.inner().clone();
    cast.connect(state.inner().clone(), &id).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn cast_disconnect(
    cast: CastState<'_>,
    state: State<'_, Arc<AppState>>,
) -> Result<(), String> {
    cast.disconnect(&state).await;
    Ok(())
}

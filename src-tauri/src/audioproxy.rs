//! Loopback HTTP proxy for the *audio* stream: mpv fetches its bytes from here, never straight from
//! googlevideo.
//!
//! **Why this exists.** googlevideo throttles a *streaming* download (the open-ended
//! `Range: bytes=X-`, or a plain GET, that ffmpeg sends) to roughly 2x realtime. A *bounded* range
//! (`bytes=X-Y`) is served at full speed instead: measured against the same URL, the middle of a
//! 60 MB mix came back at 32 KB/s open-ended and 9.4 MB/s for a 4 MiB bounded range, and a whole
//! 122 MB mix pulled in chunks took 51 s (~2.4 MB/s) instead of hours. So the throttle is on the *request shape*, not the client: VISIONOS throttles exactly
//! like WEB_REMIX, and the URLs carry no `n`-parameter to blame.
//!
//! ffmpeg's HTTP source cannot be told to chunk its reads. This module can: mpv opens
//! `http://127.0.0.1:<ephemeral>/<token>/a/<id>`, and the proxy answers its open-ended request by
//! fetching bounded chunks upstream and streaming them back. Seeks become new loopback requests
//! that are served at full speed (a few tens of ms for the first ~40 KB a WebM needs to restart)
//! instead of waiting ~1.3 s for the same bytes at 2x realtime, and mpv's forward cache fills in
//! seconds rather than at playback rate.
//!
//! Same shape as [`crate::videoproxy`]: loopback-only, a random per-launch token in the path so no
//! other local process can drive it, and upstream headers (User-Agent, and the cookie an upload
//! needs) attached here rather than by mpv.
//!
//! Set `LIMUSIC_NO_AUDIO_PROXY=1` to fall back to handing mpv the googlevideo URL directly.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures_util::StreamExt;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::header;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::sync::mpsc;

/// Streamed, never buffered: only one upstream chunk lives in RAM at a time, so a two-hour mix
/// costs the same few MB as a three-minute song.
type ProxyBody = BoxBody<Bytes, io::Error>;

/// How much of the file to ask upstream for in one range request. Large enough to amortise the
/// per-request round trip and land in the "served at full speed" regime, small enough that a seek
/// abandons little work when mpv drops the connection.
const CHUNK: u64 = 4 * 1024 * 1024;

/// How long one upstream read may make no progress before the chunk is declared dead.
///
/// It has to be here: `http::client()` sets no timeout on purpose (they are per call site) and
/// reqwest's read timeout is client-wide, so nothing else bounds this. Without it a connection that
/// goes quiet without closing hangs the pump forever, and mpv is left holding a loopback socket
/// that neither errors nor delivers a byte, with no track-failed event and so no recovery: the
/// silent stall issue #188 is about, moved inside our own process.
///
/// A stall timeout, not a total one. A whole-request deadline would have to be generous enough for
/// a 4 MiB chunk on a slow connection, which is long enough to be useless as a hang guard; this
/// fires on no progress, so a slow-but-moving download is never cut off.
const STALL: Duration = Duration::from_secs(20);

/// `send()` (or one body read) with the stall guard on it.
pub(crate) async fn no_stall<F: std::future::Future<Output = T>, T>(f: F) -> Result<T, io::Error> {
    tokio::time::timeout(STALL, f).await.map_err(|_| io::Error::other("upstream stalled"))
}

/// Keep at most this many stream URLs registered. A registration is the URL plus its headers (a
/// cookie on an upload), so this is a few KB; the FIFO only has to be deeper than the current and
/// gapless-next tracks, which are always the two most recent.
const MAX_REGISTRATIONS: usize = 64;

/// `(port, token)` of the running server. Set once at startup; unset if the bind failed, which
/// just means [`register`] returns `None` and callers keep the direct URL.
static ENDPOINT: OnceLock<(u16, String)> = OnceLock::new();

static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

/// What a resolved URL answers: its total length and content type. Learned from the first chunk
/// response's `Content-Range` and reused for every later connection, so a seek does not pay a
/// separate round trip just to learn the size.
#[derive(Clone)]
struct Meta {
    total: u64,
    content_type: Option<String>,
}

struct Registration {
    url: String,
    headers: HashMap<String, String>,
    /// The track it plays, when the caller said (`register_track`): a cast device is shown its
    /// title and artwork by it.
    video_id: Option<String>,
    meta: Mutex<Option<Meta>>,
}

struct Registry {
    map: HashMap<String, Arc<Registration>>,
    order: VecDeque<String>,
}

/// Bind the loopback listener and start serving. Binds synchronously so [`register`] is usable the
/// moment this returns, then hands the socket to tokio.
pub fn start() {
    let listener = match TcpListener::bind((Ipv4Addr::LOCALHOST, 0)) {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, "audio proxy: bind failed (streams go direct)");
            return;
        }
    };
    let port = match listener.local_addr() {
        Ok(a) => a.port(),
        Err(e) => {
            tracing::warn!(error = %e, "audio proxy: local_addr failed");
            return;
        }
    };
    if let Err(e) = listener.set_nonblocking(true) {
        tracing::warn!(error = %e, "audio proxy: set_nonblocking failed");
        return;
    }
    let token = format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>());
    if ENDPOINT.set((port, token)).is_err() {
        return; // already started
    }
    let _ = REGISTRY.set(Mutex::new(Registry { map: HashMap::new(), order: VecDeque::new() }));
    tracing::info!(port, "audio proxy listening on loopback");

    tauri::async_runtime::spawn(async move {
        let Ok(listener) = tokio::net::TcpListener::from_std(listener) else { return };
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    // EMFILE/ENFILE/ENOBUFS return immediately, so `continue` alone would spin.
                    tracing::warn!(error = %e, "audio proxy: accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            tauri::async_runtime::spawn(async move {
                let svc = service_fn(serve);
                let _ = http1::Builder::new()
                    // hyper 1.x panics when it arms a timeout without a timer installed.
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(15))
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
}

/// Register one YouTube stream URL (with the headers it needs) and return the loopback URL mpv
/// should be handed. `None` means the direct URL must be used: the proxy never came up, the
/// kill-switch is set, or the URL is not one of YouTube's stream hosts (an RSS-feed podcast's
/// enclosure is not throttled, and may not honour ranges, #294).
pub fn register(url: &str, headers: &HashMap<String, String>) -> Option<String> {
    register_with(url, headers, None)
}

/// [`register`], noting which track the stream is.
pub fn register_track(
    url: &str,
    headers: &HashMap<String, String>,
    video_id: &str,
) -> Option<String> {
    register_with(url, headers, Some(video_id.to_owned()))
}

fn register_with(
    url: &str,
    headers: &HashMap<String, String>,
    video_id: Option<String>,
) -> Option<String> {
    if !crate::orchestrator::is_youtube_stream(url)
        || std::env::var_os("LIMUSIC_NO_AUDIO_PROXY").is_some()
    {
        return None;
    }
    let (port, token) = ENDPOINT.get()?;
    let id = format!("{:016x}", rand::random::<u64>());
    let reg = Arc::new(Registration {
        url: url.to_owned(),
        headers: headers.clone(),
        video_id,
        meta: Mutex::new(None),
    });
    {
        let mut r = REGISTRY.get()?.lock().ok()?;
        while r.order.len() >= MAX_REGISTRATIONS {
            if let Some(old) = r.order.pop_front() {
                r.map.remove(&old);
            }
        }
        r.map.insert(id.clone(), reg);
        r.order.push_back(id.clone());
    }
    Some(format!("http://127.0.0.1:{port}/{token}/a/{id}"))
}

/// `/<token>/<kind>/<id>` to `(kind, id)`, once the token matches this launch's.
fn route<'a>(path: &'a str, token: &str) -> Option<(&'a str, &'a str)> {
    let (t, rest) = path.strip_prefix('/')?.split_once('/')?;
    let (kind, id) = rest.split_once('/')?;
    (t == token && !id.is_empty()).then_some((kind, id))
}

/// `/<token>/a/<id>` to the registration id.
fn id_from<'a>(path: &'a str, token: &str) -> Option<&'a str> {
    route(path, token).filter(|(kind, _)| *kind == "a").map(|(_, id)| id)
}

/// Karaoke instrumentals still being written (`/<token>/k/<id>`): the song that is playing, served
/// while the rest of it is prepared (`karaoke::KaraokeStream`).
static GROWING: OnceLock<Mutex<HashMap<String, Arc<karaoke::stream::GrowingWav>>>> =
    OnceLock::new();

/// The loopback URL mpv plays `wav` from, or `None` if the proxy never came up.
pub fn register_growing(wav: Arc<karaoke::stream::GrowingWav>) -> Option<String> {
    let (port, token) = ENDPOINT.get()?;
    let id = format!("{:016x}", rand::random::<u64>());
    GROWING.get_or_init(Default::default).lock().ok()?.insert(id.clone(), wav);
    Some(format!("http://127.0.0.1:{port}/{token}/k/{id}"))
}

/// Forget a stream registered by [`register_growing`]. A connection already reading keeps it.
pub fn unregister_growing(url: &str) {
    if let (Some(id), Some(m)) = (url.rsplit('/').next(), GROWING.get()) {
        if let Ok(mut m) = m.lock() {
            m.remove(id);
        }
    }
}

/// Serves a growing WAV. Bytes not written yet are waited for (the separation is outrunning
/// playback, or playback would not be on it), until the job is abandoned or mpv hangs up.
async fn serve_growing(
    req: &Request<Incoming>,
    id: &str,
) -> Result<Response<ProxyBody>, StatusCode> {
    let wav = GROWING
        .get()
        .and_then(|m| m.lock().ok().and_then(|m| m.get(id).cloned()))
        .ok_or(StatusCode::NOT_FOUND)?;
    let total = wav.len();
    let want = parse_range(req.headers().get(header::RANGE).and_then(|v| v.to_str().ok()));
    let Some((start, end)) = resolve_window(want, total) else {
        let mut r = empty(StatusCode::RANGE_NOT_SATISFIABLE);
        if let Ok(v) = format!("bytes */{total}").parse() {
            r.headers_mut().insert(header::CONTENT_RANGE, v);
        }
        return Ok(r);
    };
    let ranged = !matches!(want, Want::Full);
    tracing::debug!(target: "app_lib::karaoke", start, end, total, method = %req.method(), "karaoke stream request");
    let mut builder = Response::builder()
        .status(if ranged { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK }.as_u16())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start + 1)
        .header(header::CONTENT_TYPE, "audio/wav");
    if ranged {
        builder = builder.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{total}"));
    }
    let body = if req.method() == Method::HEAD {
        Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
    } else {
        let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(8);
        tauri::async_runtime::spawn(async move { pump_growing(wav, start, end, tx).await });
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item.map(Frame::data), rx))
        });
        BodyExt::boxed(StreamBody::new(stream))
    };
    builder.body(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn pump_growing(
    wav: Arc<karaoke::stream::GrowingWav>,
    start: u64,
    end: u64,
    tx: mpsc::Sender<Result<Bytes, io::Error>>,
) {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = match tokio::fs::File::open(wav.path()).await {
        Ok(f) => f,
        Err(e) => {
            let _ = tx.send(Err(e)).await;
            return;
        }
    };
    let mut buf = vec![0u8; 256 * 1024];
    let mut pos = start;
    let mut waited = false;
    while pos <= end {
        let ready = wav.servable_from(pos);
        if ready == 0 {
            if wav.is_abandoned() || tx.is_closed() {
                return;
            }
            if !waited {
                waited = true;
                tracing::debug!(target: "app_lib::karaoke", at = pos, "karaoke stream: waiting for bytes");
            }
            // ponytail: a 50 ms poll, not a wakeup per write. Chunks land seconds apart.
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let n = ready.min(buf.len() as u64).min(end - pos + 1) as usize;
        let read = async {
            file.seek(io::SeekFrom::Start(pos)).await?;
            file.read_exact(&mut buf[..n]).await
        };
        if let Err(e) = read.await {
            let _ = tx.send(Err(e)).await;
            return;
        }
        if tx.send(Ok(Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
            return;
        }
        pos += n as u64;
    }
}

fn empty(status: StatusCode) -> Response<ProxyBody> {
    let mut r = Response::new(Empty::<Bytes>::new().map_err(|e| match e {}).boxed());
    *r.status_mut() = status;
    r
}

async fn serve(req: Request<Incoming>) -> Result<Response<ProxyBody>, Infallible> {
    Ok(handle(req).await.unwrap_or_else(empty))
}

async fn handle(req: Request<Incoming>) -> Result<Response<ProxyBody>, StatusCode> {
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    let (_, token) = ENDPOINT.get().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(("k", id)) = route(req.uri().path(), token) {
        return serve_growing(&req, id).await;
    }
    let id = id_from(req.uri().path(), token).ok_or(StatusCode::NOT_FOUND)?.to_owned();
    serve_stream(req, &id).await
}

/// One registered stream, by its registration id: the part shared by the loopback listener and
/// the LAN one a cast device streams from.
async fn serve_stream(req: Request<Incoming>, id: &str) -> Result<Response<ProxyBody>, StatusCode> {
    let reg = {
        let r = REGISTRY
            .get()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        r.map.get(id).cloned()
    }
    .ok_or(StatusCode::NOT_FOUND)?;

    let meta = meta_for(&reg).await?;

    let want = parse_range(req.headers().get(header::RANGE).and_then(|v| v.to_str().ok()));
    let Some((start, end)) = resolve_window(want, meta.total) else {
        // The only malformed-but-well-formed case: a range that starts past the end of the file.
        let mut r = empty(StatusCode::RANGE_NOT_SATISFIABLE);
        if let Ok(v) = format!("bytes */{}", meta.total).parse() {
            r.headers_mut().insert(header::CONTENT_RANGE, v);
        }
        return Ok(r);
    };

    let ranged = !matches!(want, Want::Full);
    let status = if ranged { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK };
    let len = end - start + 1;

    let mut builder = Response::builder()
        .status(status.as_u16())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, len);
    if let Some(ct) = meta.content_type.as_deref() {
        builder = builder.header(header::CONTENT_TYPE, ct);
    }
    if ranged {
        builder =
            builder.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{}", meta.total));
    }

    let body = if req.method() == Method::HEAD {
        Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
    } else {
        // Pull upstream in a task and hand the bytes over a bounded channel: the client body then
        // holds only the receiver (Send + Sync, which `boxed` requires), and when mpv drops the
        // connection the receiver drops, the send fails, and the task stops fetching.
        let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(16);
        let pump = reg.clone();
        tauri::async_runtime::spawn(async move { pump_chunks(pump, start, end, tx).await });
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item.map(Frame::data), rx))
        });
        BodyExt::boxed(StreamBody::new(stream))
    };
    builder.body(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

// --- Casting --------------------------------------------------------------------------------
//
// A Cast device can't use the loopback URL, and can't fetch googlevideo itself either (the URL is
// bound to the client that resolved it), so it streams the same registrations from a second
// listener on this computer's LAN address. That listener answers only `/<lan token>/a/<id>`:
// registered audio streams, never the karaoke files, with its own token so the loopback one never
// leaves the machine.

struct Lan {
    ip: IpAddr,
    port: u16,
    token: String,
    stop: tokio::sync::watch::Sender<bool>,
}

static LAN: Mutex<Option<Lan>> = Mutex::new(None);

/// This computer's address on the route to `device`: the interface the OS would send from. A UDP
/// "connect" sends nothing; it only picks the route.
pub fn address_facing(device: IpAddr) -> Option<IpAddr> {
    let probe = UdpSocket::bind(if device.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" }).ok()?;
    probe.connect(SocketAddr::new(device, 8009)).ok()?;
    let ip = probe.local_addr().ok()?.ip();
    (!ip.is_unspecified() && !ip.is_loopback()).then_some(ip)
}

/// The URL a cast device at `device` should play for this loopback URL, starting the LAN listener
/// (or moving it to another network's address) as needed. `None` for anything but a registered
/// audio stream, or when there is no route to the device.
pub fn lan_url(loopback: &str, device: IpAddr) -> Option<String> {
    let (port, token) = ENDPOINT.get()?;
    // A registered stream (`a`), or karaoke's instrumental still being written (`k`).
    let rest = loopback.strip_prefix(&format!("http://127.0.0.1:{port}/{token}/"))?;
    let (kind, id) = rest.split_once('/').filter(|(k, _)| matches!(*k, "a" | "k"))?;
    lan_path(device, kind, id)
}

/// The URL a cast device should play for a file on this computer: local music, or a finished
/// karaoke instrumental. `None` if it isn't a readable file or there is no route to the device.
pub fn lan_file_url(path: &str, device: IpAddr) -> Option<String> {
    let path = std::path::PathBuf::from(path.strip_prefix("file://").unwrap_or(path));
    if !path.is_file() {
        return None;
    }
    let id = format!("{:016x}", rand::random::<u64>());
    {
        let mut files = LAN_FILES.lock().ok()?;
        if files.len() >= MAX_REGISTRATIONS {
            files.clear(); // the current and next track are re-registered as they load
        }
        files.insert(id.clone(), path);
    }
    lan_path(device, "f", &id)
}

/// Local files a cast device may read, by LAN id.
static LAN_FILES: std::sync::LazyLock<Mutex<HashMap<String, std::path::PathBuf>>> =
    std::sync::LazyLock::new(Default::default);

fn lan_path(device: IpAddr, kind: &str, id: &str) -> Option<String> {
    let ip = address_facing(device)?;
    let mut lan = LAN.lock().ok()?;
    if lan.as_ref().map(|l| l.ip) != Some(ip) {
        if let Some(old) = lan.take() {
            let _ = old.stop.send(true);
        }
        *lan = Some(start_lan(ip)?);
    }
    let l = lan.as_ref()?;
    let host = match l.ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{v6}]"),
    };
    Some(format!("http://{host}:{}/{}/{kind}/{id}", l.port, l.token))
}

/// Close the LAN listener (casting stopped). Connections already streaming finish on their own.
pub fn stop_lan() {
    if let Some(old) = LAN.lock().ok().and_then(|mut l| l.take()) {
        let _ = old.stop.send(true);
    }
}

/// The track a loopback URL was registered for, if [`register_track`] said.
pub fn video_id_of(loopback: &str) -> Option<String> {
    let (port, token) = ENDPOINT.get()?;
    let id = loopback.strip_prefix(&format!("http://127.0.0.1:{port}/{token}/a/"))?;
    REGISTRY.get()?.lock().ok()?.map.get(id)?.video_id.clone()
}

/// Content type of a registered stream (probing upstream once, like the first mpv request would):
/// what the cast device must be told before it fetches a byte.
pub async fn stream_content_type(loopback: &str) -> Option<String> {
    let (port, token) = ENDPOINT.get()?;
    if loopback.starts_with(&format!("http://127.0.0.1:{port}/{token}/k/")) {
        return Some("audio/wav".to_owned());
    }
    let id = loopback.strip_prefix(&format!("http://127.0.0.1:{port}/{token}/a/"))?;
    let reg = REGISTRY.get()?.lock().ok()?.map.get(id).cloned()?;
    meta_for(&reg).await.ok()?.content_type
}

fn start_lan(ip: IpAddr) -> Option<Lan> {
    let listener = match TcpListener::bind((ip, 0)) {
        Ok(l) => l,
        Err(e) => {
            tracing::warn!(error = %e, %ip, "cast: LAN listener bind failed");
            return None;
        }
    };
    let port = listener.local_addr().ok()?.port();
    listener.set_nonblocking(true).ok()?;
    let token = format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>());
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let lan_token = token.clone();
    tracing::info!(%ip, port, "cast: serving audio on the LAN");
    tauri::async_runtime::spawn(async move {
        let Ok(listener) = tokio::net::TcpListener::from_std(listener) else { return };
        loop {
            let (stream, _) = tokio::select! {
                r = listener.accept() => match r {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "cast: LAN accept failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                },
                _ = stopped.changed() => return,
            };
            let token = lan_token.clone();
            tauri::async_runtime::spawn(async move {
                let svc = service_fn(move |req| {
                    let token = token.clone();
                    async move {
                        Ok::<_, Infallible>(handle_lan(req, &token).await.unwrap_or_else(empty))
                    }
                });
                let _ = http1::Builder::new()
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(15))
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    Some(Lan { ip, port, token, stop })
}

async fn handle_lan(
    req: Request<Incoming>,
    token: &str,
) -> Result<Response<ProxyBody>, StatusCode> {
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        return Err(StatusCode::METHOD_NOT_ALLOWED);
    }
    let (kind, id) = route(req.uri().path(), token).ok_or(StatusCode::NOT_FOUND)?;
    let (kind, id) = (kind.to_owned(), id.to_owned());
    let mut resp = match kind.as_str() {
        "a" => serve_stream(req, &id).await?,
        "k" => serve_growing(&req, &id).await?,
        "f" => serve_file(&req, &id).await?,
        _ => return Err(StatusCode::NOT_FOUND),
    };
    // The Default Media Receiver plays a plain file through an <audio> element, which needs no
    // CORS; a receiver that reads it through Media Source Extensions does.
    resp.headers_mut()
        .insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, header::HeaderValue::from_static("*"));
    Ok(resp)
}

/// A local file for a cast device, with ranges (it seeks by asking for one).
async fn serve_file(req: &Request<Incoming>, id: &str) -> Result<Response<ProxyBody>, StatusCode> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let path =
        LAN_FILES.lock().ok().and_then(|f| f.get(id).cloned()).ok_or(StatusCode::NOT_FOUND)?;
    let mut file = tokio::fs::File::open(&path).await.map_err(|_| StatusCode::NOT_FOUND)?;
    let total = file.metadata().await.map_err(|_| StatusCode::NOT_FOUND)?.len();
    let want = parse_range(req.headers().get(header::RANGE).and_then(|v| v.to_str().ok()));
    let Some((start, end)) = resolve_window(want, total) else {
        let mut r = empty(StatusCode::RANGE_NOT_SATISFIABLE);
        if let Ok(v) = format!("bytes */{total}").parse() {
            r.headers_mut().insert(header::CONTENT_RANGE, v);
        }
        return Ok(r);
    };
    let ranged = !matches!(want, Want::Full);
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or_default().to_ascii_lowercase();
    let content_type = match ext.as_str() {
        "mp3" => "audio/mpeg",
        "m4a" | "mp4" | "aac" | "alac" => "audio/mp4",
        "flac" => "audio/flac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "webm" => "audio/webm",
        "wav" => "audio/wav",
        _ => "application/octet-stream",
    };
    let mut builder = Response::builder()
        .status(if ranged { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK }.as_u16())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_LENGTH, end - start + 1)
        .header(header::CONTENT_TYPE, content_type);
    if ranged {
        builder = builder.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{total}"));
    }
    let body = if req.method() == Method::HEAD {
        Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
    } else {
        file.seek(io::SeekFrom::Start(start))
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let (tx, rx) = mpsc::channel::<Result<Bytes, io::Error>>(8);
        tauri::async_runtime::spawn(async move {
            let mut left = end - start + 1;
            let mut buf = vec![0u8; 64 * 1024];
            while left > 0 {
                let want = buf.len().min(left as usize);
                match file.read(&mut buf[..want]).await {
                    Ok(0) => break,
                    Ok(n) => {
                        left -= n as u64;
                        if tx.send(Ok(Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
                            break; // the device hung up (a seek, the next track)
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
        let stream = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|item| (item.map(Frame::data), rx))
        });
        BodyExt::boxed(StreamBody::new(stream))
    };
    builder.body(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Content type of a local file, as [`serve_file`] will send it.
pub fn file_content_type(path: &str) -> &'static str {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match ext.as_str() {
        "mp3" => "audio/mpeg",
        "m4a" | "mp4" | "aac" | "alac" => "audio/mp4",
        "flac" => "audio/flac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "webm" => "audio/webm",
        _ => "audio/wav",
    }
}

/// The cached [`Meta`], probing upstream once per registration if nothing has learned it yet.
async fn meta_for(reg: &Registration) -> Result<Meta, StatusCode> {
    if let Some(m) = reg.meta.lock().ok().and_then(|g| g.clone()) {
        return Ok(m);
    }
    let m = probe(reg).await?;
    if let Ok(mut g) = reg.meta.lock() {
        *g = Some(m.clone());
    }
    Ok(m)
}

/// Ask upstream for a single byte (`bytes=0-0`) just to read the total length and content type off
/// the answer. `meta_for` keeps the result, so only the first connection to a registration pays it.
async fn probe(reg: &Registration) -> Result<Meta, StatusCode> {
    let resp = upstream_get(reg, 0, 0).await?;
    let status = resp.status();
    if status == reqwest::StatusCode::PARTIAL_CONTENT {
        let total = resp
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(total_from_content_range)
            .ok_or(StatusCode::BAD_GATEWAY)?;
        Ok(Meta { total, content_type: content_type(&resp) })
    } else {
        // Anything but a 206 is a dead end here, including a 200 from a server that ignored the
        // range: every byte this proxy serves comes from a bounded range (`pump_chunks` accepts
        // only 206), so taking the whole-file answer would just move the failure from this handled
        // spot to an I/O error mid-body. Only googlevideo URLs are ever registered and those
        // honour ranges, so in practice this is an expired URL (they last ~6h) or a refusal. mpv
        // errors and the app re-resolves.
        tracing::debug!(status = %status, "audio proxy: upstream refused");
        Err(StatusCode::BAD_GATEWAY)
    }
}

/// A bounded upstream range request with the registration's headers. Range and content-encoding do
/// not mix: without the explicit `identity` reqwest offers gzip/br and the server could answer 200
/// with an encoded whole file, which is exactly the throttled shape this proxy exists to avoid.
fn upstream_request(reg: &Registration, start: u64, end: u64) -> reqwest::RequestBuilder {
    let mut req = crate::http::client()
        .get(&reg.url)
        .header(header::RANGE.as_str(), format!("bytes={start}-{end}"))
        .header(header::ACCEPT_ENCODING.as_str(), "identity");
    for (k, v) in &reg.headers {
        req = req.header(k.as_str(), v.as_str());
    }
    req
}

async fn upstream_get(
    reg: &Registration,
    start: u64,
    end: u64,
) -> Result<reqwest::Response, StatusCode> {
    let sent = no_stall(upstream_request(reg, start, end).send()).await.map_err(|e| {
        tracing::debug!(error = %e, "audio proxy: upstream stalled");
        StatusCode::GATEWAY_TIMEOUT
    })?;
    sent.map_err(|e| {
        tracing::debug!(error = %e, "audio proxy: upstream failed");
        StatusCode::BAD_GATEWAY
    })
}

fn content_type(resp: &reqwest::Response) -> Option<String> {
    resp.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).map(str::to_owned)
}

fn total_from_content_range(value: &str) -> Option<u64> {
    // `bytes 0-0/122414474`, or `bytes 0-0/*` when the length is unknown.
    value.rsplit_once('/')?.1.trim().parse().ok()
}

/// The last byte to ask for in a chunk that starts at `pos`, clamped to the window's last byte.
fn chunk_end(pos: u64, end: u64) -> u64 {
    (pos + CHUNK - 1).min(end)
}

/// Walk `pos` to `end` inclusive in bounded upstream ranges, forwarding each response's bytes into
/// the channel. Ends when the window is done or the receiver is gone (a seek dropped the
/// connection), which is also what cancels the in-flight upstream request.
async fn pump_chunks(
    reg: Arc<Registration>,
    start: u64,
    end: u64,
    tx: mpsc::Sender<Result<Bytes, io::Error>>,
) {
    let mut pos = start;
    while pos <= end {
        let last = chunk_end(pos, end);
        let resp = match no_stall(upstream_request(&reg, pos, last).send()).await {
            Ok(Ok(r)) if r.status() == reqwest::StatusCode::PARTIAL_CONTENT => r,
            Ok(Ok(r)) => {
                let _ = tx
                    .send(Err(io::Error::other(format!(
                        "audio proxy: upstream status {}",
                        r.status()
                    ))))
                    .await;
                return;
            }
            Ok(Err(e)) => {
                let _ = tx.send(Err(io::Error::other(e))).await;
                return;
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        };
        pos = last + 1;
        let mut body = resp.bytes_stream();
        loop {
            let item = match no_stall(body.next()).await {
                Ok(Some(item)) => item.map_err(io::Error::other),
                Ok(None) => break,
                Err(e) => Err(e),
            };
            let failed = item.is_err();
            if tx.send(item).await.is_err() {
                return; // client gone, stop pulling and drop the connection upstream
            }
            if failed {
                return;
            }
        }
    }
}

/// What the client asked for, once parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Want {
    /// No `Range` header, or one this proxy does not understand: the whole file.
    Full,
    /// `bytes=X-`
    From(u64),
    /// `bytes=X-Y`
    Bounded(u64, u64),
    /// `bytes=-N` (the last N bytes)
    Suffix(u64),
}

fn parse_range(value: Option<&str>) -> Want {
    let Some(v) = value else { return Want::Full };
    let Some(spec) = v.strip_prefix("bytes=") else { return Want::Full };
    // A multi-range request is legal but nothing here (or in ffmpeg) sends one; take the first.
    let spec = spec.split(',').next().unwrap_or(spec).trim();
    let Some((a, b)) = spec.split_once('-') else { return Want::Full };
    let (a, b) = (a.trim(), b.trim());
    if a.is_empty() {
        return match b.parse() {
            Ok(n) => Want::Suffix(n),
            Err(_) => Want::Full,
        };
    }
    let Ok(start) = a.parse() else { return Want::Full };
    if b.is_empty() {
        Want::From(start)
    } else {
        match b.parse::<u64>() {
            Ok(end) if end >= start => Want::Bounded(start, end),
            _ => Want::Full,
        }
    }
}

/// The inclusive window to serve for `want`, or `None` when the request cannot be satisfied.
fn resolve_window(want: Want, total: u64) -> Option<(u64, u64)> {
    if total == 0 {
        return None;
    }
    match want {
        Want::Full => Some((0, total - 1)),
        Want::From(s) => (s < total).then_some((s, total - 1)),
        Want::Bounded(s, e) => (s < total).then(|| (s, e.min(total - 1))),
        Want::Suffix(n) => (n > 0).then(|| (total.saturating_sub(n), total - 1)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What a cast device sees of a local file: the LAN listener, a range, and nothing without
    /// the token.
    #[test]
    fn local_files_reach_a_cast_device_with_ranges() {
        // Any routable address: the LAN listener binds the interface on the route to it.
        let device: IpAddr = "192.0.2.50".parse().unwrap();
        if address_facing(device).is_none() {
            eprintln!("no network route here, skipping");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("song.flac");
        let bytes: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();

        tauri::async_runtime::block_on(async {
            let url = lan_file_url(path.to_str().unwrap(), device).expect("LAN url");
            assert!(url.contains("/f/") && !url.contains("127.0.0.1"), "{url}");
            assert_eq!(file_content_type(path.to_str().unwrap()), "audio/flac");
            let client = reqwest::Client::builder().no_proxy().build().unwrap();

            let r = client.get(&url).header("Range", "bytes=1000-1999").send().await.unwrap();
            assert_eq!(r.status(), 206);
            assert_eq!(r.headers()["content-range"], "bytes 1000-1999/200000");
            assert_eq!(r.headers()["content-type"], "audio/flac");
            assert_eq!(r.headers()["access-control-allow-origin"], "*");
            assert_eq!(r.bytes().await.unwrap().as_ref(), &bytes[1000..2000]);

            let whole = client.get(&url).send().await.unwrap();
            assert_eq!(whole.status(), 200);
            assert_eq!(whole.bytes().await.unwrap().len(), bytes.len());

            let (base, id) = url.rsplit_once("/f/").unwrap();
            let (host, _) = base.rsplit_once('/').unwrap();
            let wrong = client.get(format!("{host}/nope/f/{id}")).send().await.unwrap();
            assert_eq!(wrong.status(), 404);
            stop_lan();
        });
    }

    #[test]
    fn only_the_right_token_and_kind_route() {
        assert_eq!(id_from("/abc/a/deadbeef", "abc"), Some("deadbeef"));
        assert_eq!(id_from("/wrong/a/deadbeef", "abc"), None);
        assert_eq!(id_from("/abc/x/deadbeef", "abc"), None);
        assert_eq!(id_from("/abc/a/", "abc"), None);
        assert_eq!(id_from("/abc/a", "abc"), None);
        assert_eq!(route("/abc/k/beef", "abc"), Some(("k", "beef")));
        assert_eq!(id_from("/abc/k/beef", "abc"), None, "a stream id is not an audio one");
    }

    #[test]
    fn ranges_parse_the_ways_ffmpeg_sends_them() {
        assert_eq!(parse_range(None), Want::Full);
        assert_eq!(parse_range(Some("bytes=0-")), Want::From(0));
        assert_eq!(parse_range(Some("bytes=1234-")), Want::From(1234));
        assert_eq!(parse_range(Some("bytes=10-19")), Want::Bounded(10, 19));
        assert_eq!(parse_range(Some("bytes=0-0")), Want::Bounded(0, 0));
        assert_eq!(parse_range(Some("bytes=-500")), Want::Suffix(500));
        // Anything unusable is treated as no range, per the spec's ignore-unknown rule.
        assert_eq!(parse_range(Some("items=0-")), Want::Full);
        assert_eq!(parse_range(Some("bytes=oops")), Want::Full);
        assert_eq!(parse_range(Some("bytes=19-10")), Want::Full);
    }

    #[test]
    fn windows_clamp_and_reject() {
        assert_eq!(resolve_window(Want::Full, 1000), Some((0, 999)));
        assert_eq!(resolve_window(Want::From(0), 1000), Some((0, 999)));
        assert_eq!(resolve_window(Want::From(500), 1000), Some((500, 999)));
        assert_eq!(resolve_window(Want::Bounded(0, 5000), 1000), Some((0, 999)));
        assert_eq!(resolve_window(Want::Suffix(100), 1000), Some((900, 999)));
        assert_eq!(resolve_window(Want::From(1000), 1000), None);
        assert_eq!(resolve_window(Want::Suffix(0), 1000), None);
        assert_eq!(resolve_window(Want::Full, 0), None);
    }

    /// The chunks have to tile the window exactly: no gap to stall on, no overlap to duplicate.
    #[test]
    fn chunks_tile_the_window() {
        assert_eq!(chunk_end(0, 10 * 1024 * 1024), CHUNK - 1);
        assert_eq!(chunk_end(CHUNK, 10 * 1024 * 1024), 2 * CHUNK - 1);
        // Last chunk is clamped to the window's inclusive last byte, not one before it.
        assert_eq!(chunk_end(8 * 1024 * 1024, 10 * 1024 * 1024), 10 * 1024 * 1024);
        assert_eq!(chunk_end(0, 99), 99);

        let mut pos = 0u64;
        let end = 9 * 1024 * 1024 + 123;
        let mut seen = 0u64;
        while pos <= end {
            let last = chunk_end(pos, end);
            assert!(last <= end);
            seen += last - pos + 1;
            pos = last + 1;
        }
        assert_eq!(pos, end + 1);
        assert_eq!(seen, end + 1);
    }

    #[test]
    fn total_is_read_from_content_range() {
        assert_eq!(total_from_content_range("bytes 0-0/122414474"), Some(122414474));
        assert_eq!(total_from_content_range("bytes 0-0/*"), None);
        assert_eq!(total_from_content_range("nonsense"), None);
    }
}

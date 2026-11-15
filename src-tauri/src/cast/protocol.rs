//! The Google Cast v2 protocol, the part casting needs: TLS to the device on port 8009,
//! length-prefixed protobuf frames (`CastMessage`), a JSON string payload on a namespace.
//!
//! The message has six fields worth encoding, so it is done by hand rather than with a protobuf
//! code generator.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

pub const NS_CONNECTION: &str = "urn:x-cast:com.google.cast.tp.connection";
pub const NS_HEARTBEAT: &str = "urn:x-cast:com.google.cast.tp.heartbeat";
pub const NS_RECEIVER: &str = "urn:x-cast:com.google.cast.receiver";
pub const NS_MEDIA: &str = "urn:x-cast:com.google.cast.media";

pub const SENDER: &str = "sender-0";
pub const RECEIVER: &str = "receiver-0";

#[derive(Debug, Clone)]
pub struct Message {
    pub source: String,
    pub destination: String,
    pub namespace: String,
    pub data: Value,
}

// --- protobuf: CastMessage { protocol_version=1 (enum), source_id=2, destination_id=3,
//                             namespace=4, payload_type=5 (enum, 0 = STRING), payload_utf8=6 }

fn varint(out: &mut Vec<u8>, mut n: u64) {
    while n > 0x7f {
        out.push((n as u8 & 0x7f) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn string_field(out: &mut Vec<u8>, field: u64, value: &str) {
    varint(out, (field << 3) | 2);
    varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn enum_field(out: &mut Vec<u8>, field: u64, value: u64) {
    varint(out, field << 3);
    varint(out, value);
}

/// One frame: the 4-byte big-endian length, then the message.
pub fn encode(source: &str, destination: &str, namespace: &str, data: &Value) -> Vec<u8> {
    let mut body = Vec::new();
    enum_field(&mut body, 1, 0);
    string_field(&mut body, 2, source);
    string_field(&mut body, 3, destination);
    string_field(&mut body, 4, namespace);
    enum_field(&mut body, 5, 0);
    string_field(&mut body, 6, &data.to_string());
    let mut frame = (body.len() as u32).to_be_bytes().to_vec();
    frame.extend(body);
    frame
}

/// A frame's message (without the length prefix). `None` for anything but a JSON payload.
pub fn decode(body: &[u8]) -> Option<Message> {
    let mut fields: HashMap<u64, &[u8]> = HashMap::new();
    let mut i = 0usize;
    let read_varint = |i: &mut usize| -> Option<u64> {
        let mut result = 0u64;
        let mut shift = 0;
        loop {
            let b = *body.get(*i)?;
            *i += 1;
            result |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(result);
            }
            shift += 7;
            if shift > 63 {
                return None;
            }
        }
    };
    while i < body.len() {
        let key = read_varint(&mut i)?;
        match key & 7 {
            0 => {
                read_varint(&mut i)?;
            }
            2 => {
                let len = read_varint(&mut i)? as usize;
                let bytes = body.get(i..i.checked_add(len)?)?;
                fields.insert(key >> 3, bytes);
                i += len;
            }
            _ => return None, // nothing the Cast protocol sends
        }
    }
    let text = |f: u64| {
        fields.get(&f).map(|b| String::from_utf8_lossy(b).into_owned()).unwrap_or_default()
    };
    let payload = fields.get(&6)?; // binary payloads (device auth) aren't used here
    Some(Message {
        source: text(2),
        destination: text(3),
        namespace: text(4),
        data: serde_json::from_slice(payload).ok()?,
    })
}

/// Cast devices present a certificate from Google's device CA, which no public root store holds.
/// Google's own senders check it with a separate device-auth exchange that playback doesn't need,
/// so the certificate is accepted as is: the connection is still encrypted, and it only ever goes
/// to a device the user picked on their own network.
#[derive(Debug)]
struct AcceptDeviceCertificate(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptDeviceCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

type Waiters = Arc<Mutex<HashMap<u64, oneshot::Sender<Message>>>>;

/// One open connection. Cloning shares it. Incoming messages that aren't replies to
/// [`Channel::request`] (statuses the device pushes) arrive on the receiver `open` returns, which
/// closes when the connection does.
#[derive(Clone)]
pub struct Channel {
    out: mpsc::UnboundedSender<Vec<u8>>,
    waiters: Waiters,
    next_id: Arc<AtomicU64>,
}

impl Channel {
    pub async fn open(
        addr: SocketAddr,
    ) -> anyhow::Result<(Channel, mpsc::UnboundedReceiver<Message>)> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptDeviceCertificate(provider)))
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let tcp = tokio::time::timeout(Duration::from_secs(8), TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow::anyhow!("the device didn't answer"))??;
        let _ = tcp.set_nodelay(true);
        let name = ServerName::IpAddress(addr.ip().into());
        let tls = tokio::time::timeout(Duration::from_secs(8), connector.connect(name, tcp))
            .await
            .map_err(|_| anyhow::anyhow!("the device didn't answer"))??;
        let (mut reader, mut writer) = tokio::io::split(tls);

        let (out, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (in_tx, in_rx) = mpsc::unbounded_channel::<Message>();
        let waiters: Waiters = Arc::default();

        // Writer: frames out, in order. Ends when every sender is gone or the socket fails.
        tokio::spawn(async move {
            while let Some(frame) = out_rx.recv().await {
                // An empty frame is the reader saying the connection is over.
                if frame.is_empty() || writer.write_all(&frame).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });

        let channel = Channel {
            out: out.clone(),
            waiters: waiters.clone(),
            next_id: Arc::new(AtomicU64::new(1)),
        };

        // Reader: frames in. Answers the device's heartbeat, hands replies to their waiters and
        // everything else to the session. A device that goes quiet for 16 s is gone (it pings,
        // and answers ours, every 5 s).
        let pong = out.clone();
        tokio::spawn(async move {
            let mut head = [0u8; 4];
            loop {
                let read =
                    tokio::time::timeout(Duration::from_secs(16), reader.read_exact(&mut head))
                        .await;
                if !matches!(read, Ok(Ok(_))) {
                    break;
                }
                let len = u32::from_be_bytes(head) as usize;
                if len > 1 << 20 {
                    break;
                }
                let mut body = vec![0u8; len];
                if reader.read_exact(&mut body).await.is_err() {
                    break;
                }
                let Some(msg) = decode(&body) else { continue };
                if msg.namespace == NS_HEARTBEAT {
                    if msg.data["type"] == "PING" {
                        let _ = pong.send(encode(
                            &msg.destination,
                            &msg.source,
                            NS_HEARTBEAT,
                            &json!({"type": "PONG"}),
                        ));
                    }
                    continue;
                }
                if let Some(id) = msg.data["requestId"].as_u64().filter(|id| *id > 0) {
                    if let Some(w) = waiters.lock().unwrap().remove(&id) {
                        let _ = w.send(msg.clone());
                    }
                }
                if in_tx.send(msg).is_err() {
                    break;
                }
            }
            waiters.lock().unwrap().clear(); // fails every pending request
            let _ = pong.send(Vec::new()); // and stops the writer, which closes the channel
        });

        // Heartbeat: ours, every 5 s, until the connection is gone.
        let ping = out.clone();
        tokio::spawn(async move {
            let mut every = tokio::time::interval(Duration::from_secs(5));
            loop {
                every.tick().await;
                if ping
                    .send(encode(SENDER, RECEIVER, NS_HEARTBEAT, &json!({"type": "PING"})))
                    .is_err()
                {
                    break;
                }
            }
        });

        channel.send(RECEIVER, NS_CONNECTION, json!({"type": "CONNECT"}));
        Ok((channel, in_rx))
    }

    pub fn send(&self, destination: &str, namespace: &str, data: Value) {
        let _ = self.out.send(encode(SENDER, destination, namespace, &data));
    }

    /// Send with a fresh `requestId` and wait for the reply carrying it.
    pub async fn request(
        &self,
        destination: &str,
        namespace: &str,
        mut data: Value,
        timeout: Duration,
    ) -> anyhow::Result<Message> {
        if self.is_closed() {
            anyhow::bail!("the connection to the device closed");
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        data["requestId"] = json!(id);
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().unwrap().insert(id, tx);
        self.send(destination, namespace, data);
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(msg)) => Ok(msg),
            Ok(Err(_)) => anyhow::bail!("the connection to the device closed"),
            Err(_) => {
                self.waiters.lock().unwrap().remove(&id);
                anyhow::bail!("the device didn't answer")
            }
        }
    }

    /// Fire-and-forget with a `requestId` (media commands want one; their reply is a status the
    /// session sees anyway).
    pub fn command(&self, destination: &str, namespace: &str, mut data: Value) {
        data["requestId"] = json!(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.send(destination, namespace, data);
    }

    pub fn is_closed(&self) -> bool {
        self.out.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        let data = json!({"type": "LOAD", "title": "é".repeat(200), "n": 3});
        let frame = encode("a", "b", NS_MEDIA, &data);
        assert_eq!(u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize, frame.len() - 4);
        let msg = decode(&frame[4..]).unwrap();
        assert_eq!(
            (msg.source.as_str(), msg.destination.as_str(), msg.namespace.as_str()),
            ("a", "b", NS_MEDIA)
        );
        assert_eq!(msg.data, data);
    }

    #[test]
    fn junk_is_refused() {
        assert!(decode(&[0xff, 0xff, 0xff]).is_none());
        assert!(decode(&[0x0d, 1, 2, 3, 4]).is_none()); // wire type 5
    }
}

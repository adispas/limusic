# Casting (Google Cast)

Limusic can play to Chromecasts, Google TVs and Nest speakers on the same network. The cast button
sits in the player bar, next to the devices button.

## How it works

- **Discovery:** mDNS (`_googlecast._tcp`), only while the picker is open (`src-tauri/src/cast/discovery.rs`).
- **Receiver:** Google's Default Media Receiver (`CC1AD845`), so there is no receiver app to
  register or host. The Cast v2 protocol is spoken directly over TLS (`cast/protocol.rs`).
- **Streaming:** a googlevideo URL only works for the client that resolved it, so the device
  streams from this computer. The audio proxy serves the same registrations it serves mpv on a LAN
  listener too, under its own random token, answering only audio streams (`/a/`), karaoke
  instrumentals (`/k/`) and local files handed to the device (`/f/`). See `audioproxy::lan_url`.
- **The player stays in charge:** `crates/player/src/remote.rs` adds a remote output to `Player`.
  While one is attached, every call that would drive mpv goes to the device instead, and the
  device's status comes back as the usual `PlayerEvent`s. The queue, radio, repeat, gapless
  advance, history, scrobbling, media keys and Listen Together work unchanged.
- **Handover:** starting to cast moves the current track to the device at the same position; stopping
  (or losing the device) brings it back to mpv, paused where the device was.

Casting is audio only. Loudness normalization, tempo, pitch, reverb, crossfade and the music video
don't apply while casting.

## Troubleshooting

- **No devices:** the device and the computer must be on the same network. Guest networks and
  routers with "client/AP isolation" block both discovery and streaming.
- **Windows:** allow Limusic through Windows Defender Firewall on *private* networks when asked.
  The device connects to this computer to fetch the audio, so a blocked prompt means silence.

//! Playing somewhere other than this computer: a Cast device (see the app's `cast` module).
//!
//! While a remote output is attached, every [`Player`] call that would drive mpv is answered here
//! instead, and the remote's own reports come back as the same [`PlayerEvent`]s mpv sends. The app
//! keeps running its queue, radio, repeat and history exactly as it does for local playback,
//! without knowing where the sound is.
//!
//! mpv stays loaded but silent, and its events are muted, so a stop issued for the handover isn't
//! read as the track ending. Detaching loads the track back into mpv where the remote left off,
//! paused.
//!
//! What a remote can't do is ignored: the loudness gain, tempo, pitch and reverb (the device plays
//! the stream as it is), crossfade (tracks change gaplessly) and the music video.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::{Decks, Error, Player, PlayerEvent};

/// Where the sound goes while attached. Calls are fire-and-forget, made from whatever thread the
/// app drives the player on: an implementation queues them and must not block.
pub trait RemoteOutput: Send + Sync {
    /// Start `url`, exactly as the app handed it to [`Player::load`] (usually the loopback audio
    /// proxy's URL, which the implementation turns into one the device can reach).
    fn load(&self, url: &str, start: Option<f64>, paused: bool, live: bool);
    fn play(&self);
    fn pause(&self);
    fn seek(&self, secs: f64);
    fn stop(&self);
    /// The volume slider's percent, 0-100.
    fn set_volume(&self, percent: i64);
}

#[derive(Default)]
struct State {
    /// The file the remote is playing (or loading), as the app named it.
    path: Option<String>,
    /// The gapless lookahead: started by the remote itself the moment `path` finishes.
    next: Option<String>,
    live: bool,
    /// The pause flag, as mpv's would be: set by play/pause, not by the track running out.
    paused: bool,
    /// Nothing loaded (never started, ran out, failed, stopped).
    idle: bool,
    /// What the app was last told: [`PlayerEvent::Playing`] is only sent on a change.
    playing: bool,
    position: f64,
    duration: f64,
    /// Kept for the trip back to mpv.
    headers: HashMap<String, String>,
    gain: Option<f64>,
}

pub(crate) struct Remote {
    out: Arc<dyn RemoteOutput>,
    st: Mutex<State>,
}

/// What the remote reports back. Cloned into the implementation's session task; calls after the
/// remote was detached (or replaced) are ignored.
#[derive(Clone)]
pub struct RemoteHandle {
    decks: Arc<Decks>,
    remote: Arc<Remote>,
}

impl RemoteHandle {
    fn current(&self) -> bool {
        self.decks.remote.lock().unwrap().as_ref().is_some_and(|r| Arc::ptr_eq(r, &self.remote))
    }

    fn send(&self, e: PlayerEvent) {
        let _ = self.decks.tx.send_unmuted(e);
    }

    pub fn position(&self, secs: f64) {
        if !self.current() || !secs.is_finite() {
            return;
        }
        self.remote.st.lock().unwrap().position = secs;
        self.send(PlayerEvent::Position(secs));
    }

    pub fn duration(&self, secs: f64) {
        if !self.current() || !secs.is_finite() || secs <= 0.0 {
            return;
        }
        let changed = {
            let mut st = self.remote.st.lock().unwrap();
            let changed = (st.duration - secs).abs() > 0.01;
            st.duration = secs;
            changed
        };
        if changed {
            self.send(PlayerEvent::Duration(secs));
        }
    }

    /// The device is playing (`true`) or paused (`false`). Buffering is neither: don't call.
    pub fn playing(&self, now: bool) {
        if !self.current() {
            return;
        }
        let (playing_changed, pause_changed) = {
            let mut st = self.remote.st.lock().unwrap();
            if st.idle {
                return; // a status for the track before a stop or a failure
            }
            let pause_changed = st.paused == now; // paused from elsewhere (the Google Home app)
            st.paused = !now;
            let playing_changed = st.playing != now;
            st.playing = now;
            (playing_changed, pause_changed)
        };
        if pause_changed {
            self.send(PlayerEvent::Paused(!now));
        }
        if playing_changed {
            self.send(PlayerEvent::Playing(now));
        }
    }

    /// The track played to its end. Starts the lookahead (or the same track, on repeat-one) the
    /// way mpv's playlist would, so the app sees the gapless advance it expects.
    pub fn finished(&self) {
        if !self.current() {
            return;
        }
        let looping = self.decks.loop_file.load(Ordering::Relaxed);
        let mut st = self.remote.st.lock().unwrap();
        if looping {
            if let Some(path) = st.path.clone() {
                st.position = 0.0;
                let live = st.live;
                drop(st);
                self.remote.out.load(&path, None, false, live);
                self.send(PlayerEvent::Looped);
                return;
            }
        }
        if let Some(next) = st.next.take() {
            st.path = Some(next.clone());
            st.live = false;
            st.idle = false;
            st.paused = false;
            st.position = 0.0;
            st.duration = 0.0;
            drop(st);
            self.remote.out.load(&next, None, false, false);
            self.send(PlayerEvent::TrackEnded);
            return;
        }
        st.idle = true;
        let was_playing = std::mem::replace(&mut st.playing, false);
        drop(st);
        self.send(PlayerEvent::TrackEnded);
        if was_playing {
            self.send(PlayerEvent::Playing(false));
        }
    }

    /// The device couldn't play the track (its stream died, the format was refused).
    pub fn failed(&self, message: String) {
        if !self.current() {
            return;
        }
        let was_playing = {
            let mut st = self.remote.st.lock().unwrap();
            st.idle = true;
            st.next = None;
            std::mem::replace(&mut st.playing, false)
        };
        self.send(PlayerEvent::TrackFailed(message));
        if was_playing {
            self.send(PlayerEvent::Playing(false));
        }
    }
}

impl Remote {
    fn load(
        &self,
        url: &str,
        headers: &HashMap<String, String>,
        gain: Option<f64>,
        start: Option<f64>,
        live: bool,
    ) {
        let paused = {
            let mut st = self.st.lock().unwrap();
            st.path = Some(url.to_owned());
            st.next = None;
            st.live = live;
            st.idle = false;
            st.position = start.unwrap_or(0.0);
            st.duration = 0.0;
            st.headers = headers.clone();
            st.gain = gain;
            st.paused
        };
        self.out.load(url, start.filter(|_| !live), paused, live);
    }

    /// The pause flag changed here (play/pause/toggle). mpv reports that as `Paused` at once,
    /// whatever the track is doing, and the app relies on it (Listen Together broadcasts it).
    fn set_paused(&self, decks: &Decks, paused: bool) {
        let changed = {
            let mut st = self.st.lock().unwrap();
            let changed = st.paused != paused;
            st.paused = paused;
            changed && !st.idle
        };
        if paused {
            self.out.pause();
        } else {
            self.out.play();
        }
        if changed {
            let _ = decks.tx.send_unmuted(PlayerEvent::Paused(paused));
        }
    }
}

impl Player {
    pub(crate) fn remote(&self) -> Option<Arc<Remote>> {
        self.decks.remote.lock().unwrap().clone()
    }

    /// Whether the sound is going to a remote output.
    pub fn is_remote(&self) -> bool {
        self.decks.remote.lock().unwrap().is_some()
    }

    /// Move playback to `out`: the track playing here continues there from the same position
    /// (paused if it was paused), and mpv goes quiet. Replaces any remote already attached.
    pub fn attach_remote(&self, out: Arc<dyn RemoteOutput>) -> RemoteHandle {
        let previous = self.decks.remote.lock().unwrap().take();
        let carried = match &previous {
            // From one device to another: carry the remote's own state over.
            Some(r) => {
                r.out.stop();
                let st = r.st.lock().unwrap();
                (!st.idle).then(|| {
                    (st.path.clone(), st.position, !st.paused, st.live, st.headers.clone(), st.gain)
                })
            }
            None => {
                let mpv = self.decks.active_mpv();
                let idle = mpv.get_property::<bool>("idle-active").unwrap_or(true);
                (!idle).then(|| {
                    let path = mpv.get_property::<String>("path").ok();
                    let pos = mpv.get_property::<f64>("time-pos").unwrap_or(0.0);
                    let paused = mpv.get_property::<bool>("pause").unwrap_or(false);
                    let live = path.is_some() && *self.decks.live_url.lock().unwrap() == path;
                    let headers = self.decks.headers.lock().unwrap().clone();
                    let gain = self.decks.af.lock().unwrap().gain
                        [self.decks.active.load(Ordering::SeqCst)];
                    (path, pos, !paused, live, headers, gain)
                })
            }
        }
        .and_then(|(path, pos, playing, live, headers, gain)| {
            path.map(|p| (p, pos, playing, live, headers, gain))
        });

        // Muted first, so the stop below isn't heard as the track ending.
        self.decks.tx.mute(true);
        self.drop_preload(true);
        let _ = self.decks.active_mpv().command("stop", &[]);

        let remote =
            Arc::new(Remote { out, st: Mutex::new(State { idle: true, ..State::default() }) });
        *self.decks.remote.lock().unwrap() = Some(remote.clone());
        let volume = self.decks.volume.load(Ordering::Relaxed);
        remote.out.set_volume(volume);
        if let Some((path, pos, playing, live, headers, gain)) = carried {
            {
                let mut st = remote.st.lock().unwrap();
                st.paused = !playing;
                st.playing = playing; // the app already thinks so: no event for it
            }
            remote.load(&path, &headers, gain, Some(pos), live);
        }
        RemoteHandle { decks: self.decks.clone(), remote }
    }

    /// Bring playback back here: the remote stops, and the track it was on is loaded into mpv at
    /// the position it reached, paused. Returns that position, or `None` if nothing was playing.
    pub fn detach_remote(&self) -> Option<f64> {
        let remote = self.decks.remote.lock().unwrap().take()?;
        remote.out.stop();
        let st = std::mem::take(&mut *remote.st.lock().unwrap());
        self.decks.tx.mute(false);
        let path = st.path.filter(|_| !st.idle)?;
        let mpv = self.decks.active_mpv();
        let _ = mpv.set_property("pause", true);
        if self.load(&path, &st.headers, st.gain, Some(st.position), st.live).is_err() {
            return None;
        }
        if st.playing {
            let _ = self.decks.tx.send_unmuted(PlayerEvent::Paused(true));
            let _ = self.decks.tx.send_unmuted(PlayerEvent::Playing(false));
        }
        let _ = self.decks.tx.send_unmuted(PlayerEvent::Position(st.position));
        Some(st.position)
    }

    // --- the Player calls a remote answers (see each caller in lib.rs / video.rs)

    pub(crate) fn remote_load(
        &self,
        r: &Remote,
        url: &str,
        headers: &HashMap<String, String>,
        gain_db: Option<f64>,
        start: Option<f64>,
        live: bool,
    ) -> Result<(), Error> {
        r.load(url, headers, gain_db, start, live);
        Ok(())
    }

    pub(crate) fn remote_enqueue(&self, r: &Remote, url: &str) {
        r.st.lock().unwrap().next = Some(url.to_owned());
    }

    pub(crate) fn remote_clear_playlist(&self, r: &Remote) {
        r.st.lock().unwrap().next = None;
    }

    pub(crate) fn remote_stop(&self, r: &Remote) {
        let was_playing = {
            let mut st = r.st.lock().unwrap();
            st.idle = true;
            st.next = None;
            std::mem::replace(&mut st.playing, false)
        };
        r.out.stop();
        if was_playing {
            let _ = self.decks.tx.send_unmuted(PlayerEvent::Playing(false));
        }
    }

    pub(crate) fn remote_is_idle(&self, r: &Remote) -> bool {
        r.st.lock().unwrap().idle
    }

    pub(crate) fn remote_is_paused(&self, r: &Remote) -> bool {
        r.st.lock().unwrap().paused
    }

    pub(crate) fn remote_set_paused(&self, r: &Remote, paused: bool) {
        r.set_paused(&self.decks, paused);
    }

    pub(crate) fn remote_toggle(&self, r: &Remote) {
        let paused = r.st.lock().unwrap().paused;
        r.set_paused(&self.decks, !paused);
    }

    pub(crate) fn remote_seek(&self, r: &Remote, secs: f64) {
        let live = {
            let mut st = r.st.lock().unwrap();
            if !st.live {
                st.position = secs;
            }
            st.live
        };
        if !live {
            r.out.seek(secs);
        }
    }

    pub(crate) fn remote_set_volume(&self, r: &Remote, percent: i64) {
        r.out.set_volume(percent);
    }

    pub(crate) fn remote_set_gain(&self, r: &Remote, gain_db: Option<f64>) {
        r.st.lock().unwrap().gain = gain_db;
    }

    pub(crate) fn remote_clock(&self, r: &Remote) -> Option<f64> {
        let st = r.st.lock().unwrap();
        (!st.idle).then_some(st.position)
    }

    pub(crate) fn remote_current_path(&self, r: &Remote) -> Option<String> {
        let st = r.st.lock().unwrap();
        st.path.clone().filter(|_| !st.idle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Records what the player asked of the "device".
    #[derive(Default)]
    struct Fake(Mutex<Vec<String>>);

    impl Fake {
        fn take(&self) -> Vec<String> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    impl RemoteOutput for Fake {
        fn load(&self, url: &str, start: Option<f64>, paused: bool, _live: bool) {
            self.0.lock().unwrap().push(format!("load {url} {start:?} paused={paused}"));
        }
        fn play(&self) {
            self.0.lock().unwrap().push("play".into());
        }
        fn pause(&self) {
            self.0.lock().unwrap().push("pause".into());
        }
        fn seek(&self, secs: f64) {
            self.0.lock().unwrap().push(format!("seek {secs}"));
        }
        fn stop(&self) {
            self.0.lock().unwrap().push("stop".into());
        }
        fn set_volume(&self, percent: i64) {
            self.0.lock().unwrap().push(format!("volume {percent}"));
        }
    }

    fn events(rx: &mut tokio::sync::mpsc::UnboundedReceiver<PlayerEvent>) -> Vec<String> {
        std::thread::sleep(Duration::from_millis(30));
        let mut out = Vec::new();
        while let Ok(e) = rx.try_recv() {
            match e {
                // mpv's own position/duration chatter from before the handover isn't the point
                PlayerEvent::Position(_) | PlayerEvent::Duration(_) => {}
                other => out.push(format!("{other:?}")),
            }
        }
        out
    }

    fn player() -> (Player, tokio::sync::mpsc::UnboundedReceiver<PlayerEvent>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut p = Player::new(dir.path().to_str().unwrap()).unwrap();
        p.mpv().set_property("ao", "null").unwrap();
        let rx = p.take_events().unwrap();
        (p, rx, dir)
    }

    #[test]
    fn the_queue_runs_on_the_remote_like_on_mpv() {
        let (p, mut rx, _dir) = player();
        let fake = Arc::new(Fake::default());
        let h = p.attach_remote(fake.clone());
        assert!(p.is_remote() && p.is_idle());
        assert_eq!(fake.take(), ["volume 100"], "nothing was playing, so nothing moves over");

        // A track starts the way `state::start_current` starts one: load, then play.
        p.load("http://127.0.0.1:1/t/a/one", &HashMap::new(), Some(-3.0), Some(12.0), false)
            .unwrap();
        p.play().unwrap();
        assert_eq!(
            fake.take(),
            ["load http://127.0.0.1:1/t/a/one Some(12.0) paused=false", "play"]
        );
        assert!(!p.is_idle() && !p.is_paused());
        assert_eq!(p.current_path().as_deref(), Some("http://127.0.0.1:1/t/a/one"));
        assert_eq!(p.clock(), Some(12.0));

        h.playing(true);
        h.position(30.5);
        assert_eq!(p.clock(), Some(30.5));
        assert_eq!(events(&mut rx), ["Playing(true)"]);

        // Pause from the app: mpv would report the flag at once.
        p.pause().unwrap();
        assert_eq!(fake.take(), ["pause"]);
        assert_eq!(events(&mut rx), ["Paused(true)"]);
        h.playing(false); // the device confirms
        assert_eq!(events(&mut rx), ["Playing(false)"]);
        p.toggle().unwrap();
        h.playing(true);
        assert_eq!(fake.take(), ["play"]);
        assert_eq!(events(&mut rx), ["Paused(false)", "Playing(true)"]);

        // The gapless lookahead starts the moment the track ends, before the app hears of it.
        p.enqueue("http://127.0.0.1:1/t/a/two").unwrap();
        h.finished();
        assert_eq!(fake.take(), ["load http://127.0.0.1:1/t/a/two None paused=false"]);
        assert_eq!(events(&mut rx), ["TrackEnded"]);
        assert!(!p.is_idle(), "on_track_ended must see the gapless advance");
        assert_eq!(p.current_path().as_deref(), Some("http://127.0.0.1:1/t/a/two"));

        // Repeat-one restarts the track and says so.
        p.set_loop_file(true).unwrap();
        h.finished();
        assert_eq!(fake.take(), ["load http://127.0.0.1:1/t/a/two None paused=false"]);
        assert_eq!(events(&mut rx), ["Looped"]);
        p.set_loop_file(false).unwrap();

        // Nothing queued behind it: the queue ran out.
        h.finished();
        assert!(fake.take().is_empty());
        assert_eq!(events(&mut rx), ["TrackEnded", "Playing(false)"]);
        assert!(p.is_idle());
        h.playing(true); // a late status for the track that ended
        assert!(events(&mut rx).is_empty());

        // A failed track: the app retries or moves on by itself.
        p.load("http://127.0.0.1:1/t/a/three", &HashMap::new(), None, None, false).unwrap();
        h.playing(true);
        events(&mut rx);
        h.failed("boom".into());
        assert_eq!(events(&mut rx), ["TrackFailed(\"boom\")", "Playing(false)"]);
        assert!(p.is_idle());
    }

    #[test]
    fn handing_back_resumes_here_paused_where_the_remote_was() {
        let (p, mut rx, _dir) = player();
        let fake = Arc::new(Fake::default());
        let h = p.attach_remote(fake.clone());
        p.load("av://lavfi:sine=f=440:d=60", &HashMap::new(), None, None, false).unwrap();
        p.play().unwrap();
        h.playing(true);
        h.position(21.0);
        events(&mut rx);

        assert_eq!(p.detach_remote(), Some(21.0));
        assert!(!p.is_remote());
        assert!(fake.take().contains(&"stop".to_owned()));
        let ev = events(&mut rx);
        assert!(
            ev.contains(&"Paused(true)".to_owned()) && ev.contains(&"Playing(false)".to_owned()),
            "{ev:?}"
        );
        // mpv has the track again, paused at the remote's position.
        std::thread::sleep(Duration::from_millis(300));
        assert!(p.is_paused());
        assert_eq!(p.current_path().as_deref(), Some("av://lavfi:sine=f=440:d=60"));
        let pos = p.mpv().get_property::<f64>("time-pos").unwrap_or(0.0);
        assert!((pos - 21.0).abs() < 1.0, "resumed at {pos}");

        // A handle from the old session can't touch anything any more.
        h.finished();
        h.playing(true);
        assert!(events(&mut rx).iter().all(|e| !e.contains("TrackEnded")));
    }

    #[test]
    fn what_mpv_was_playing_moves_to_the_remote() {
        let (p, mut rx, _dir) = player();
        p.load("av://lavfi:sine=f=440:d=60", &HashMap::new(), None, Some(5.0), false).unwrap();
        p.play().unwrap();
        std::thread::sleep(Duration::from_millis(400));
        events(&mut rx);

        let fake = Arc::new(Fake::default());
        let _h = p.attach_remote(fake.clone());
        let calls = fake.take();
        assert_eq!(calls[0], "volume 100");
        assert!(
            calls[1].starts_with("load av://lavfi:sine=f=440:d=60 Some(5.")
                && calls[1].ends_with("paused=false"),
            "{calls:?}"
        );
        // mpv stopping for the handover is not the track ending.
        std::thread::sleep(Duration::from_millis(200));
        assert!(events(&mut rx).is_empty());
        assert!(!p.is_idle() && !p.is_paused());
    }
}

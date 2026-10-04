use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::future::join_all;
use gpui::{App, Context, Entity, Task};
use music::connect::{
    Collection, Command, Connect, Device, Elsewhere, Event, Naming, NowPlaying, RepeatMode, Roster,
    Start,
};
use music::{MusicApi, Track};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    AppSettings, ConnectName, Io, Origin, Playback, Queue, Repeat, Session, SessionEvent, Whence,
    join,
};

/// How far the position may stray from where steady playback would have put it before it is
/// reported again as a seek.
const SEEK_SLACK: Duration = Duration::from_secs(2);
/// How many upcoming tracks the other devices are told about.
const UPCOMING: usize = 10;

/// Where a start handed over by another device begins, with the tracks it plays read in.
struct Loaded {
    tracks: Vec<Track>,
    index: usize,
    origin: Option<Origin>,
    position: Duration,
    paused: bool,
}

/// The provider's device network as the app sees it: this app listed as a device, playback
/// reported to the account's other apps, their commands carried out, and the account's other
/// devices to hand playback to. Only a provider that has such a network gives it anything to do.
pub struct Devices {
    playback: Entity<Playback>,
    queue: Entity<Queue>,
    settings: Entity<AppSettings>,
    session: Entity<Session>,
    io: Io,
    link: Option<Arc<dyn Connect>>,
    /// Whether this app is on the device list, as far as the link was told.
    shown: bool,
    /// The name last handed to the link.
    named: Option<Naming>,
    roster: Roster,
    /// The track playing on another device, once it is read in.
    remote: Option<Track>,
    /// Whether this app took playback, which only the listener pressing play does. A track
    /// restored paused at launch must not pull playback off another device.
    claimed: bool,
    sent: Option<NowPlaying>,
    stamp: Instant,
    events: Option<Task<()>>,
    starting: Option<Task<()>>,
    fetching: Option<Task<()>>,
    queuing: HashMap<String, Task<()>>,
}

impl Devices {
    pub fn new(
        playback: Entity<Playback>,
        queue: Entity<Queue>,
        settings: Entity<AppSettings>,
        session: Entity<Session>,
        io: Io,
        cx: &mut Context<Self>,
    ) -> Self {
        cx.subscribe(&session, |this, _, event, cx| match event {
            SessionEvent::LocalChanged => {}
            SessionEvent::SignedIn | SessionEvent::SignedOut | SessionEvent::Reconnected => {
                this.relink(cx)
            }
        })
        .detach();
        cx.observe(&playback, |this, _, cx| this.publish(cx))
            .detach();
        cx.observe(&queue, |this, _, cx| this.publish(cx)).detach();
        cx.observe(&settings, |this, _, cx| this.apply(cx)).detach();

        let mut devices = Self {
            playback,
            queue,
            settings,
            session,
            io,
            link: None,
            shown: false,
            named: None,
            roster: Roster::default(),
            remote: None,
            claimed: false,
            sent: None,
            stamp: Instant::now(),
            events: None,
            starting: None,
            fetching: None,
            queuing: HashMap::new(),
        };
        devices.relink(cx);
        devices
    }

    /// Whether the signed-in provider has a device network, wanted or not.
    pub fn supported(&self) -> bool {
        self.link.is_some()
    }

    /// Whether the signed-in provider has a device network and the listener wants it.
    pub fn available(&self) -> bool {
        self.link.is_some() && self.shown
    }

    /// Whether this app's playback is what the account's other devices are being told about.
    pub fn publishing(&self) -> bool {
        self.available() && self.sent.is_some()
    }

    /// The account's other devices.
    pub fn devices(&self) -> &[Device] {
        &self.roster.devices
    }

    /// The other device that has playback, if one does.
    pub fn elsewhere(&self) -> Option<&Elsewhere> {
        self.roster.elsewhere.as_ref()
    }

    /// The track playing on that device, once it is read in.
    pub fn remote_track(&self) -> Option<&Track> {
        self.remote.as_ref()
    }

    /// Moves playback onto this app from whichever device has it.
    pub fn transfer_here(&self) {
        if let Some(link) = &self.link {
            link.transfer(&link.device());
        }
    }

    /// Moves playback onto another device.
    pub fn transfer(&self, device: &str) {
        if let Some(link) = &self.link {
            link.transfer(device);
        }
    }

    /// Sends a command to another device.
    pub fn control(&self, device: &str, command: Command) {
        if let Some(link) = &self.link {
            link.control(device, command);
        }
    }

    /// Takes the device network of the signed-in provider, or lets go of the last one when
    /// there is none or the session was replaced.
    fn relink(&mut self, cx: &mut Context<Self>) {
        let link = self
            .session
            .read(cx)
            .client()
            .and_then(|client| client.connect());
        let same = match (&self.link, &link) {
            (Some(old), Some(new)) => Arc::ptr_eq(old, new),
            (None, None) => true,
            _ => false,
        };
        if same {
            return;
        }

        self.events = None;
        self.starting = None;
        self.fetching = None;
        self.queuing.clear();
        self.roster = Roster::default();
        self.remote = None;
        self.claimed = false;
        self.sent = None;
        self.shown = false;
        self.named = None;
        self.link = link;

        if let Some(events) = self.link.as_ref().and_then(|link| link.events()) {
            self.events = Some(self.listen(events, cx));
        }
        self.apply(cx);
        cx.notify();
    }

    /// Lists the app on the device network, or takes it off, as the setting says.
    fn apply(&mut self, cx: &mut Context<Self>) {
        let Some(link) = self.link.clone() else {
            return;
        };
        let settings = self.settings.read(cx);
        let wanted = settings.spotify_connect();
        let naming = match settings.spotify_connect_name() {
            ConnectName::Sonora => Naming::App,
            ConnectName::Both => Naming::AppOnComputer,
            ConnectName::Computer => Naming::Computer,
            ConnectName::Custom => {
                Naming::Custom(settings.spotify_connect_custom_name().to_owned())
            }
        };
        if self.named.as_ref() != Some(&naming) {
            link.rename(naming.clone());
            self.named = Some(naming);
        }
        if wanted == self.shown {
            return;
        }
        self.shown = wanted;
        link.enable(wanted);
        if wanted {
            self.publish(cx);
        } else {
            self.claimed = false;
            self.sent = None;
            self.roster = Roster::default();
            self.remote = None;
        }
        cx.notify();
    }

    fn listen(&mut self, mut events: UnboundedReceiver<Event>, cx: &mut Context<Self>) -> Task<()> {
        cx.spawn(async move |this, cx| {
            while let Some(event) = events.recv().await {
                if this
                    .update(cx, |this, cx| this.on_event(event, cx))
                    .is_err()
                {
                    break;
                }
            }
        })
    }

    fn on_event(&mut self, event: Event, cx: &mut Context<Self>) {
        match event {
            Event::Roster(roster) => self.set_roster(roster, cx),
            Event::Command(command) => self.run(command, cx),
        }
    }

    fn set_roster(&mut self, roster: Roster, cx: &mut Context<Self>) {
        self.roster = roster;
        let wanted = self
            .elsewhere()
            .and_then(|elsewhere| elsewhere.track.clone());
        if self.remote.as_ref().and_then(|track| track.id.clone()) != wanted {
            self.remote = None;
            self.fetching = None;
            if let (Some(id), Some(client)) = (wanted, self.session.read(cx).client()) {
                self.fetching = Some(self.read_remote(client, id, cx));
            }
        }
        cx.notify();
    }

    fn read_remote(
        &self,
        client: Arc<dyn MusicApi>,
        id: String,
        cx: &mut Context<Self>,
    ) -> Task<()> {
        let io = self.io.clone();
        cx.spawn(async move |this, cx| {
            let wanted = id.clone();
            let found = join(io.spawn(async move { client.track(&wanted).await })).await;
            this.update(cx, |this, cx| {
                this.fetching = None;
                match found {
                    Ok(track) if track.id.as_deref() == Some(id.as_str()) => {
                        let current = this.elsewhere().and_then(|e| e.track.as_deref());
                        if current == Some(id.as_str()) {
                            this.remote = Some(track);
                            cx.notify();
                        }
                    }
                    Ok(_) => {}
                    Err(error) => log::warn!("connect: cannot read the remote track: {error:#}"),
                }
            })
            .ok();
        })
    }

    /// Carries out what another device asked of this one.
    fn run(&mut self, command: Command, cx: &mut Context<Self>) {
        match command {
            Command::Play => self.playback.update(cx, |playback, cx| playback.resume(cx)),
            Command::Pause => self.playback.update(cx, |playback, cx| playback.pause(cx)),
            Command::Next => self.playback.update(cx, |playback, cx| playback.next(cx)),
            Command::Previous => self
                .playback
                .update(cx, |playback, cx| playback.previous(cx)),
            Command::Seek(at) => self
                .playback
                .update(cx, |playback, cx| playback.seek(at, cx)),
            Command::Volume(level) => self
                .playback
                .update(cx, |playback, cx| playback.set_volume(level, cx)),
            Command::Shuffle(on) => self.queue.update(cx, |queue, cx| queue.set_shuffle(on, cx)),
            Command::Repeat(mode) => self.playback.update(cx, |playback, cx| {
                playback.set_repeat(
                    match mode {
                        RepeatMode::Off => Repeat::Off,
                        RepeatMode::Context => Repeat::All,
                        RepeatMode::Track => Repeat::One,
                    },
                    cx,
                )
            }),
            Command::Enqueue(id) => self.enqueue(id, cx),
            Command::Start(start) => self.start(start, cx),
            Command::Released => {
                // another device took over, so this one stops without calling playback back
                self.claimed = false;
                self.playback.update(cx, |playback, cx| playback.pause(cx));
                self.publish(cx);
            }
        }
    }

    fn enqueue(&mut self, id: String, cx: &mut Context<Self>) {
        let Some(client) = self.session.read(cx).client() else {
            return;
        };
        let io = self.io.clone();
        let key = id.clone();
        let slot = id.clone();
        let task = cx.spawn(async move |this, cx| {
            let found = join(io.spawn(async move { client.track(&id).await })).await;
            this.update(cx, |this, cx| {
                this.queuing.remove(&key);
                match found {
                    Ok(track) => this
                        .playback
                        .update(cx, |playback, cx| playback.enqueue(track, cx)),
                    Err(error) => log::warn!("connect: cannot queue {key}: {error:#}"),
                }
            })
            .ok();
        });
        self.queuing.insert(slot, task);
    }

    /// Plays what another device handed over, from where it left off.
    fn start(&mut self, start: Start, cx: &mut Context<Self>) {
        let Some(client) = self.session.read(cx).client() else {
            return;
        };
        self.claimed = true;
        let io = self.io.clone();
        self.starting = Some(cx.spawn(async move |this, cx| {
            let loaded = join(io.spawn(async move { load(client, start).await })).await;
            this.update(cx, |this, cx| {
                this.starting = None;
                match loaded {
                    Ok(loaded) => this.playback.update(cx, |playback, cx| {
                        playback.start(loaded.tracks, loaded.index, loaded.origin, cx);
                        if !loaded.position.is_zero() {
                            playback.seek(loaded.position, cx);
                        }
                        if loaded.paused {
                            playback.pause(cx);
                        }
                    }),
                    Err(error) => {
                        log::warn!("connect: cannot start what was handed over: {error:#}");
                        this.claimed = false;
                    }
                }
            })
            .ok();
        }));
    }

    /// Tells the account's other apps what plays, when something does that this app took.
    fn publish(&mut self, cx: &mut Context<Self>) {
        let Some(link) = self.link.clone() else {
            return;
        };
        if !self.shown {
            return;
        }

        let Some(now) = self.now_playing(cx) else {
            if self.sent.take().is_some() {
                link.publish(None);
                cx.notify();
            }
            return;
        };
        if !self.changed(&now) {
            return;
        }
        self.stamp = Instant::now();
        self.sent = Some(now.clone());
        link.publish(Some(now));
        cx.notify();
    }

    /// Whether `now` says more than the last report did. The position moving on its own is not
    /// news, a jump is.
    fn changed(&self, now: &NowPlaying) -> bool {
        let Some(sent) = &self.sent else {
            return true;
        };
        let expected = match sent.playing {
            true => sent.position + self.stamp.elapsed(),
            false => sent.position,
        };
        if now.position.abs_diff(expected) > SEEK_SLACK {
            return true;
        }
        let mut same = now.clone();
        same.position = sent.position;
        same != *sent
    }

    fn now_playing(&mut self, cx: &App) -> Option<NowPlaying> {
        let playback = self.playback.read(cx);
        let track = playback.track()?;
        let id = track.id.clone().filter(|id| !music::is_local_id(id))?;
        let playing = playback.wants_playing();
        self.claimed |= playing;
        if !self.claimed {
            return None;
        }

        let queue = self.queue.read(cx);
        let upcoming = queue
            .upcoming()
            .filter_map(|track| track.id.clone())
            .filter(|id| !music::is_local_id(id))
            .take(UPCOMING)
            .collect();
        let context = playback.origin(cx).and_then(|origin| match origin.whence {
            Whence::Album => Some(Collection::Album(origin.id.clone())),
            Whence::Playlist => Some(Collection::Playlist(origin.id.clone())),
            Whence::Saved => Some(Collection::Saved),
            _ => None,
        });
        Some(NowPlaying {
            track: id,
            upcoming,
            context,
            playing,
            position: playback.live_position(),
            duration: track.duration,
            volume: playback.volume(),
            shuffle: queue.shuffle(),
            repeat: match playback.repeat() {
                Repeat::Off => RepeatMode::Off,
                Repeat::All => RepeatMode::Context,
                Repeat::One => RepeatMode::Track,
            },
        })
    }
}

/// Reads in what a start names: the collection if it has one the track is in, otherwise the
/// track and the tracks that follow it.
async fn load(client: Arc<dyn MusicApi>, start: Start) -> Result<Loaded> {
    let (tracks, origin) = match &start.collection {
        Some(Collection::Album(id)) => (client.album_tracks(id).await?, Some(Origin::album(id))),
        Some(Collection::Playlist(id)) => (
            client.playlist_tracks(id).await?,
            Some(Origin::playlist(id)),
        ),
        Some(Collection::Saved) => (client.saved_tracks().await?, Some(Origin::saved())),
        None => (Vec::new(), None),
    };

    let at = |tracks: &[Track]| {
        tracks
            .iter()
            .position(|track| track.id.is_some() && track.id == start.track)
    };
    let (tracks, origin, index) = match at(&tracks) {
        Some(index) => (tracks, origin, index),
        None if start.collection.is_some() && start.track.is_none() => (tracks, origin, 0),
        None => {
            let ids = start.track.iter().chain(&start.upcoming);
            let found = join_all(ids.map(|id| client.track(id))).await;
            let tracks = found.into_iter().filter_map(Result::ok).collect::<Vec<_>>();
            (tracks, None, 0)
        }
    };
    anyhow::ensure!(!tracks.is_empty(), "the start has no track to play");

    Ok(Loaded {
        tracks,
        index,
        origin,
        position: start.position,
        paused: start.paused,
    })
}

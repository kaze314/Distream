use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::{Bytes, Read};
use std::sync::Arc;
use std::time::Instant;
use bytes::Buf;
use sha2::{Digest, Sha256};
use srt_tokio::SrtSocket;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, RwLock};
use crate::media_server::media_data::{short_hash, HashKey, StreamBite, IP};
use crate::media_server::playout::playout_loop;
use crate::media_server::settings::Settings;
use crate::tracker::tracker_connection::Message;
use crate::tracker::{tracker_connection, tracker_manager};

pub struct StreamManager {
    pub stream_bites: Arc<RwLock<HashMap<HashKey, Arc<StreamBite>>>>,
    pub order: Arc<RwLock<VecDeque<HashKey>>>,
    file: Arc<Mutex<tokio::fs::File>>,
    identity: Mutex<TcpStream>,
    stalled: AtomicUsize,   // How many times running save_bite has found the head of the queue still missing.
    history: RwLock<VecDeque<HashKey>>, // Insertion order, for deciding what to forget.
    playout: Option<tokio::sync::mpsc::Sender<Arc<StreamBite>>>,
    pub settings: Settings
}

const MAX_SAVE_STALL: usize = 150;
const MAX_HELD_BITES: usize = 150;

impl StreamManager {
    pub async fn from(settings: Settings) -> Self {
        let stream_bites = Arc::new(RwLock::new(HashMap::new()));
        let order = Arc::new(RwLock::new(VecDeque::new()));
        let dir = Path::new(&settings.out_dir);
        tokio::fs::create_dir_all(dir).await
            .expect("Failed to create output directory");

        let path = dir.join(format!("{}.ts", settings.stream_name));

        let file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await.expect("Failed to open file")
        ));

        println!("[mgr] writing {}, tracker is {}", path.display(), settings.tracker);

        let playout = if settings.play_addr.is_empty() {
            None
        } else {
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            tokio::spawn(playout_loop(rx, settings.play_addr.clone()));
            Some(tx)
        };

        let identity = Mutex::new(
            TcpStream::connect(settings.tracker.clone())
                .await.expect("Failed to open identity connection to tracker.")
        );

        Self {
            stream_bites,
            order,
            file,
            identity,
            stalled: AtomicUsize::new(0),
            history: RwLock::new(VecDeque::new()),
            playout,
            settings,
        }
    }

    // None when the tracker has nobody serving this bite yet.
    pub async fn request_download(&self, tracker: &mut TcpStream, port: usize, hash: &HashKey) -> Option<IP> {
        tracker.write_u32(Message::RequestDownload as u32).await
            .expect("Failed to get register download.");

        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker.write_all(hash).await.expect("Failed to write to stream.");

        tracker_connection::send_string(tracker, &port.to_string()).await
            .expect("Failed to send port name with tracker.");

        let streamer_ip = tracker_connection::get_string(tracker, 100, None).await
            .expect("Failed to get streamer ip.");

        if streamer_ip.is_empty() {
            println!("[peer] tracker has no source for {} yet", short_hash(hash));
            return None;
        }

        println!("[peer] tracker says {streamer_ip} has {} (we listen on {port})", short_hash(hash));
        Some(streamer_ip)
    }

    pub async fn request_stream_info(&self, tracker: &mut TcpStream) -> Vec<HashKey> {
        tracker.write_u32(Message::RequestStreamInfo as u32).await
            .expect("Failed to get stream info.");

        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        let hashes = tracker_connection::recv_hash_info(tracker).await
            .expect("Error getting viewer waitlist");

        hashes
    }

    pub async fn get_viewers(&self) -> Vec<(IP, HashKey)> {
        let mut tracker = self.identity.lock().await;

        tracker.write_u32(Message::RequestViewerWaitlist as u32).await
            .expect("Failed to create stream with tracker.");

        tracker_connection::send_string(&mut tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        let viewers = tracker_connection::recv_viewer_info(&mut tracker).await
            .expect("Error getting viewer waitlist");

        viewers
    }
    pub async fn register_stream(&self) {
        let mut tracker = self.identity.lock().await;

        tracker.write_u32(Message::CreateStream as u32).await
            .expect("Failed to create stream with tracker.");

        // Send the name and key
        tracker_connection::send_string(&mut tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker_connection::send_string(&mut tracker, &*self.settings.stream_password).await
            .expect("Failed to send stream name with tracker.");

        println!("[origin] registered stream '{}' with tracker", self.settings.stream_name);
    }

    pub async fn register_bite(&self, bite: &StreamBite) -> HashKey {
        let hash = StreamManager::get_stream_bite_hash(bite);
        self.stream_bites.write().await.insert(hash, Arc::from(bite.clone()));
        self.order.write().await.push_back(hash);
        self.forget_old_bites(hash).await;

        {

            let mut tracker = self.identity.lock().await;

            tracker.write_u32(Message::InsertStreamBite as u32).await
                .expect("Failed to create stream with tracker.");

            tracker_connection::send_string(&mut tracker, &*self.settings.stream_name).await
                .expect("Failed to send stream name with tracker.");

            tracker_connection::send_string(&mut tracker, &*self.settings.stream_password).await
                .expect("Failed to send stream key with tracker.");

            tracker.write_all(&hash).await.expect("Failed to write to stream.");

            Self::send_register_streamer(&mut tracker, &self.settings.stream_name, &hash).await;
        }

        hash
    }

    pub async fn register_streamer(&self, hash: &HashKey) {
        let mut tracker = self.identity.lock().await;
        Self::send_register_streamer(&mut tracker, &self.settings.stream_name, hash).await;
    }

    async fn send_register_streamer(tracker: &mut TcpStream, stream_name: &str, hash: &HashKey) {
        tracker.write_u32(Message::RegisterStreamer as u32).await
            .expect("Failed to register streamer with tracker.");

        tracker_connection::send_string(tracker, stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker.write_all(hash).await.expect("Failed to write to stream.");
    }

    pub async fn store_bite(&self, hash: HashKey, bite: StreamBite) {
        self.stream_bites.write().await.insert(hash, Arc::new(bite));
        self.forget_old_bites(hash).await;
    }

    // Drop all but the newest MAX_HELD_BITES, keeping any still unwritten.
    async fn forget_old_bites(&self, hash: HashKey) {
        let mut history = self.history.write().await;
        history.push_back(hash);

        while history.len() > MAX_HELD_BITES {
            let Some(oldest) = history.pop_front() else { break };
            if self.order.read().await.contains(&oldest) {
                history.push_front(oldest);
                break;
            }

            self.stream_bites.write().await.remove(&oldest);
        }
    }

    pub async fn queue_bite(&self, hash: HashKey) {
        self.order.write().await.push_back(hash);
    }

    pub async fn save_bite(&self) -> bool {
        let Some(hash) = self.order.read().await.front().copied() else {
            return false;
        };

        let held = self.stream_bites.read().await.get(&hash).cloned();
        let Some(bite) = held else {

            let waited = self.stalled.fetch_add(1, Ordering::Relaxed) + 1;
            if waited < MAX_SAVE_STALL {
                return false;
            }

            println!("[file] gave up waiting for {}, skipping it", short_hash(&hash));
            self.order.write().await.pop_front();
            self.stalled.store(0, Ordering::Relaxed);
            return false;
        };

        self.order.write().await.pop_front();
        self.stalled.store(0, Ordering::Relaxed);
        self.file.lock().await.write_all(&bite).await.expect("Failed to write to file.");

        if let Some(player) = &self.playout {
            if player.try_send(bite.clone()).is_err() {
                println!("[play] player is behind, dropped {}", short_hash(&hash));
            }
        }

        // A backlog means the file is falling behind the broadcast.
        let behind = self.order.read().await.len();
        if behind > 2 {
            println!("[file] wrote {}, {behind} bites behind", short_hash(&hash));
        }

        true
    }

    pub fn get_stream_bite_hash(stream_bite: &StreamBite) -> HashKey {
        Sha256::digest(stream_bite).0
    }
}

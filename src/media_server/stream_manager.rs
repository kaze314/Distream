use std::collections::{HashMap, VecDeque};
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
use crate::media_server::media_data::{HashKey, StreamBite, IP};
use crate::media_server::settings::Settings;
use crate::tracker::tracker_connection::Message;
use crate::tracker::{tracker_connection, tracker_manager};

pub struct StreamManager {
    pub stream_bites: Arc<RwLock<HashMap<HashKey, Arc<StreamBite>>>>,
    pub order: Arc<RwLock<VecDeque<HashKey>>>,
    file: Arc<Mutex<tokio::fs::File>>,
    pub settings: Settings
}

impl StreamManager {
    pub async fn from(settings: Settings) -> Self {
        let stream_bites = Arc::new(RwLock::new(HashMap::new()));
        let order = Arc::new(RwLock::new(VecDeque::new()));
        let file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(format!("{}.ts", settings.stream_name))
                .await.expect("Failed to open file")
        ));

        Self {stream_bites, order, file, settings}
    }

    pub async fn request_download(&self, tracker: &mut TcpStream, port: usize, hash: &HashKey) -> IP {
        tracker.write_u32(Message::RequestDownload as u32).await
            .expect("Failed to get register download.");

        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker.write_all(hash).await.expect("Failed to write to stream.");

        tracker_connection::send_string(tracker, &port.to_string()).await
            .expect("Failed to send port name with tracker.");

        let streamer_ip = tracker_connection::get_string(tracker, 100, None).await
            .expect("Failed to get streamer ip.");

        streamer_ip
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

    pub async fn get_viewers(&self, tracker: &mut TcpStream) -> Vec<(IP, HashKey)> {
        tracker.write_u32(Message::RequestViewerWaitlist as u32).await
            .expect("Failed to create stream with tracker.");

        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        let viewers = tracker_connection::recv_viewer_info(tracker).await
            .expect("Error getting viewer waitlist");

        viewers
    }
    pub async fn register_stream(&self, tracker: &mut TcpStream) {
        tracker.write_u32(Message::CreateStream as u32).await
            .expect("Failed to create stream with tracker.");

        // Send the name and key
        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker_connection::send_string(tracker, &*self.settings.stream_password).await
            .expect("Failed to send stream name with tracker.");
    }

    pub async fn register_bite(&self, tracker: &mut TcpStream, bite: &StreamBite) -> HashKey {
        let hash = StreamManager::get_stream_bite_hash(bite);
        self.stream_bites.write().await.insert(hash, Arc::from(bite.clone()));
        self.order.write().await.push_back(hash);

        tracker.write_u32(Message::InsertStreamBite as u32).await
            .expect("Failed to create stream with tracker.");

        // Send the name and key
        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker_connection::send_string(tracker, &*self.settings.stream_password).await
            .expect("Failed to send stream name with tracker.");

        // send hash
        tracker.write_all(&hash).await.expect("Failed to write to stream.");
        self.register_streamer(tracker, &hash).await;

        hash
    }

    pub async fn register_streamer(&self, tracker: &mut TcpStream,  hash: &HashKey) {
        tracker.write_u32(Message::RegisterStreamer as u32).await
            .expect("Failed to register streamer with tracker.");

        // Send the name and key
        tracker_connection::send_string(tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker.write_all(hash).await.expect("Failed to write to stream.");
    }

    pub async fn save_bite(&self) {
        let next_hast = self.order.write().await.pop_front();
        if let Some(hash) = next_hast {
            let bite = self.stream_bites.read().await.get(&hash).expect("Missing bite").clone();
            self.file.lock().await.write_all(&bite).await.expect("Failed to write to file.");
        }

    }

    pub fn get_stream_bite_hash(stream_bite: &StreamBite) -> HashKey {
        Sha256::digest(stream_bite).0
    }
}

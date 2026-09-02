use std::collections::HashMap;
use std::io::{Bytes, Read};
use std::time::Instant;
use bytes::Buf;
use sha2::{Digest, Sha256};
use srt_tokio::SrtSocket;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use crate::media_server::media_data::{HashKey, StreamBite, IP};
use crate::media_server::settings::Settings;
use crate::tracker::tracker_connection::Message;
use crate::tracker::{tracker_connection, tracker_manager};

pub struct StreamManager {
    stream_bites: HashMap<HashKey, StreamBite>,
    tracker: TcpStream,
    settings: Settings
}

impl StreamManager {
    pub async fn from(settings: Settings) -> Self {
        let tracker = TcpStream::connect(settings.tracker.clone()).await
            .expect("Failed to connect to tracker.");

        let stream_bites = HashMap::new();

        Self {stream_bites, tracker, settings}
    }

    pub async fn update_viewers_loop(&mut self) {
        loop {
            let mut handles = Vec::new();
            let viewers = self.get_viewers().await;

            for (viewer_ip, hash) in viewers {
                let stream_bite = self.stream_bites.get(&hash).expect("Error").clone();
                let handle = tokio::spawn(
                    async move{
                        StreamManager::update_viewer(viewer_ip, stream_bite).await
                    }
                );

                handles.push(handle);
            }

            for handle in handles {
                handle.await;
            }
        }
    }

    async fn update_viewer(viewer_ip: IP, stream_bite: StreamBite) {
        let viewer_client = format!("{}:34554", viewer_ip);
        let mut viewer_conn = SrtSocket::builder()
            .local_port(34554)
            .rendezvous(viewer_client)
            .await.expect("Failed to connect to viewer.");

        viewer_conn.try_send(Instant::now(), stream_bite.into()).expect("TODO: panic message");
    }

    pub async fn get_viewers(&mut self) -> Vec<(IP, HashKey)> {
        self.tracker.write_u32(Message::RequestViewerWaitlist as u32).await
            .expect("Failed to create stream with tracker.");

        tracker_connection::send_string(&mut self.tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        let viewers = tracker_connection::recv_viewer_info(&mut self.tracker).await
            .expect("Error getting viewer waitlist");

        viewers
    }
    pub async fn register_stream(&mut self) {
        self.tracker.write_u32(Message::CreateStream as u32).await
            .expect("Failed to create stream with tracker.");

        // Send the name and key
        tracker_connection::send_string(&mut self.tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker_connection::send_string(&mut self.tracker, &*self.settings.stream_password).await
            .expect("Failed to send stream name with tracker.");
    }

    pub async fn register_bite(&mut self, bite: &StreamBite) -> HashKey {
        let hash = StreamManager::get_stream_bite_hash(bite);
        self.stream_bites.insert(hash, bite.clone());

        self.tracker.write_u32(Message::InsertStreamBite as u32).await
            .expect("Failed to create stream with tracker.");

        // Send the name and key
        tracker_connection::send_string(&mut self.tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        tracker_connection::send_string(&mut self.tracker, &*self.settings.stream_password).await
            .expect("Failed to send stream name with tracker.");

        // send hash
        self.tracker.write_all(&hash).await.expect("Failed to write to stream.");
        self.register_streamer(&hash).await;

        hash
    }

    pub async fn register_streamer(&mut self, hash: &HashKey) {
        self.tracker.write_u32(Message::RegisterStreamer as u32).await
            .expect("Failed to register streamer with tracker.");

        // Send the name and key
        tracker_connection::send_string(&mut self.tracker, &*self.settings.stream_name).await
            .expect("Failed to send stream name with tracker.");

        self.tracker.write_all(hash).await.expect("Failed to write to stream.");
    }

    pub fn get_stream_bite_hash(stream_bite: &StreamBite) -> HashKey {
        Sha256::digest(stream_bite).0
    }
}

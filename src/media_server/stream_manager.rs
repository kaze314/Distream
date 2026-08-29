use std::collections::HashMap;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use crate::media_server::media_data::{HashKey, StreamBite, IP};
use crate::media_server::settings::Settings;
use crate::tracker::tracker_manager;

pub enum ServerEvent {
    NewStream {
        name: String,
        peer: String,
    },
    StreamChunk {
        stream: String,
        chunk: Vec<u8>,
    },
}

struct StreamManager {
    stream_bites: HashMap<HashKey, StreamBite>,
    tracker: TcpStream,
    settings: Settings
}

impl StreamManager {
    pub async fn from(tracker_ip: IP, settings: Settings) -> Self {
        let tracker = TcpStream::connect(settings.tracker).await
            .expect("Failed to connect to tracker.");

        let stream_bites = HashMap::new();

        Self {stream_bites, tracker, settings}
    }
}


// Stream manager handles communication between the tracker
// and viewers.
/*
pub async fn stream_manager(mut rx: Receiver<ServerEvent>) {
    let mut stream = TcpStream::connect("127.0.0.1:3334").await.unwrap();
    let data = DistreamServerData::new(tracker_manager::LOCALHOST.to_string());


    while let Some(event) = rx.recv().await {
        match event {

            ServerEvent::NewStream { name, peer } => {
                data.new_stream(&name, peer).unwrap();
            }

            ServerEvent::StreamChunk { stream, chunk } => {
                data.insert(&stream, chunk).unwrap();
            }
            
            
        }
    }
}

*/
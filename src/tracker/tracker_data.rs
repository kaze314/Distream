use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::media_server::media_server_data::DistreamServerData;
use crate::tracker::streamer_queue::StreamerQueue;

type StreamBite = Vec<u8>;
type HashKey = [u8; 32];
type IP = String;

const MAX_PACKET_SIZE: usize = 1316;
pub const STREAM_BITE_SIZE: usize = 500*MAX_PACKET_SIZE;

struct StreamData {
    stream_name: String,
    stream_key: String,
    stream_bites: Vec<HashKey>,
    streamers: HashMap<HashKey, StreamerQueue<IP>>,
}

pub struct TrackerData {
    current_streams: HashMap<String, StreamData>,
}

impl TrackerData {
    pub fn new() -> Self {
        let current_streams = HashMap::new();
        TrackerData { current_streams }
    }

    pub fn insert_stream_bite(&mut self, stream_name: String, stream_key: String, hash: HashKey) {
        if let Some(stream_data) = self.current_streams.get_mut(&stream_name) {
            if stream_data.stream_key == stream_key {
                stream_data.stream_bites.push(hash);
            }
        }
    }

    pub fn get_stream_bites(&self, stream_name: String) -> Option<Vec<HashKey>> {
        if let Some(stream) = self.current_streams.get(&stream_name) {
            return Some(stream.stream_bites.clone());
        }
        
        None
    }
}


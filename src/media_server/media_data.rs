use std::collections::HashMap;
use crate::tracker::tracker_manager::StreamMetadata;
use sha2::{Sha256, Digest};
use tokio::net::TcpStream;

pub type StreamBite = Vec<u8>;
pub type HashKey = [u8; 32];

pub const MAX_PACKET_SIZE: usize = 1316;
pub const STREAM_BITE_SIZE: usize = 500*MAX_PACKET_SIZE;

pub type IP = String;

pub fn short_hash(hash: &HashKey) -> String {
    hash[..4].iter().map(|b| format!("{b:02x}")).collect()
}

pub struct MediaStream {
    name: String,
    tracker: IP,
    stream_bites: HashMap<HashKey, StreamBite>,
}

impl MediaStream {
    pub fn new(name: String, tracker: IP) -> Self {
        let stream_bites: HashMap<HashKey, StreamBite> = HashMap::new();
        MediaStream {
            name,
            tracker,
            stream_bites,
        }
    }

    pub fn get_stream_bite_hash(stream_bite: &StreamBite) -> HashKey {
        Sha256::digest(stream_bite).0
    }

    pub fn insert(&mut self, stream_name: &String, stream_bite: StreamBite) -> Result<(), ()> {
        let hash = MediaStream::get_stream_bite_hash(&stream_bite);
        /*
        match self.stream_data.get_mut(stream_name) {
            Some(stream) => stream.stream_bites.insert(hash, stream_bite),
            None => return Err(()),
        };
        */
        Ok(())
    }
}


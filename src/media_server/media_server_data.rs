use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sha2::{Sha256, Digest};

type StreamBite = Vec<u8>;
type HashKey = [u8; 32];

const MAX_PACKET_SIZE: usize = 1316;
pub const STREAM_BITE_SIZE: usize = 500*MAX_PACKET_SIZE;

pub struct DistreamServerData {
    stream_bites: HashMap<HashKey, StreamBite>,
}

impl DistreamServerData {
    pub fn new() -> Self {
        let stream_bites = HashMap::new();
        DistreamServerData { stream_bites }
    }

    pub fn get_stream_bite_hash(stream_bite: &StreamBite) -> HashKey {
        Sha256::digest(stream_bite).0
    }

    pub fn insert(&mut self, stream_bite: StreamBite) {
        let hash = DistreamServerData::get_stream_bite_hash(&stream_bite);
        self.stream_bites.insert(hash, stream_bite);
    }
}


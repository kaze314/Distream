use std::collections::HashMap;
use tokio::net::TcpStream;
use crate::tracker::streamer_queue::StreamerQueue;
use crate::media_server::media_data::*;

pub const LOCALHOST: &str = "127.0.0.1:3334";

// How many times the tracker references the streamer before moving on.
const STREAMER_REPEATS: usize = 3;

pub struct StreamMetadata {
    stream_name: String,
    stream_key: String,
    stream_bites: Vec<HashKey>,
    pub streamers: HashMap<HashKey, StreamerQueue<IP>>,

    // When a viewer requests a bite from a streamer, they are placed
    // into this waitlist until the streamer checks
    viewer_waitlist: HashMap<IP, Vec<(IP, HashKey)>>,
}

pub struct TrackerManager {
    pub current_streams: HashMap<String, StreamMetadata>,
}

impl TrackerManager {
    pub fn new() -> Self {
        let current_streams = HashMap::new();
        TrackerManager { current_streams }
    }

    // Update the stream by adding a hash of a stream bite.
    // This function will create a new stream if it doesn't already exist.
    pub fn insert_stream_bite(&mut self, stream_name: String, stream_key: String, hash: HashKey) {
        let mut option = self.current_streams.get_mut(&stream_name);
        if option.is_none() {
            self.create_stream(stream_name.clone(), stream_key.clone());
            option = self.current_streams.get_mut(&stream_name);
        }

        if let Some(stream_metadata) = option {
            if stream_metadata.stream_key == stream_key {
                stream_metadata.stream_bites.push(hash);

                let queue = StreamerQueue::new(STREAMER_REPEATS);
                stream_metadata.streamers.insert(hash, queue);
            }
        }
    }

    pub fn register_streamer(&mut self, stream_ip: String, stream_name: String,  hash: HashKey) {
        // If the stream or queue is not found the manager should do nothing
        if let Some(stream) = self.current_streams.get_mut(&stream_name) {
            if let Some(queue) = stream.streamers.get_mut(&hash) {
                queue.push_back(stream_ip.clone());
            }
        }
    }

    pub fn get_streamer(&mut self, viewer_ip: IP, stream_name: &String, hash: HashKey) -> Option<&IP> {
        if let Some(stream) = self.current_streams.get_mut(stream_name) {
            if let Some(queue) = stream.streamers.get_mut(&hash) {
                let streamer_ip = queue.pop_front().unwrap();

                // If there is already a vector waitlist, add the viewer to it
                // Create the vector if one does not exist
                match stream.viewer_waitlist.get_mut(streamer_ip) {
                    Some(viewers) => viewers.push((viewer_ip.clone(), hash)),
                    None => {stream.viewer_waitlist.insert(streamer_ip.clone(), vec![(viewer_ip.clone(), hash)]);}
                }

                return Some(streamer_ip)
            }
        }

        return None;
    }

    // Removes the viewer list from the waitlist and returns it
    pub fn flush_waitlist(&mut self, streamer_ip: IP, stream_name: &String) -> Option<Vec<(IP, HashKey)>> {
        if let Some(stream) = self.current_streams.get_mut(stream_name) {
            return stream.viewer_waitlist.remove(&streamer_ip);
        }

        return None;
    }


    pub fn create_stream(&mut self, stream_name: String, stream_key: String) {
        let metadata = StreamMetadata {
            stream_name: stream_name.clone(),
            stream_key: stream_key.clone(),
            stream_bites: Vec::new(),
            streamers: HashMap::new(),
            viewer_waitlist: HashMap::new(),
        };

        self.current_streams.insert(stream_name, metadata);
    }

    pub fn get_stream_bites(&self, stream_name: String) -> Option<Vec<HashKey>> {
        if let Some(stream) = self.current_streams.get(&stream_name) {
            return Some(stream.stream_bites.clone());
        }
        
        None
    }
}


use std::collections::HashMap;
use tokio::net::TcpStream;
use crate::tracker::streamer_queue::StreamerQueue;
use crate::media_server::media_data::*;

pub const LOCALHOST: &str = "127.0.0.1:3334";


// Higher values leave the load sitting on whoever registered first.
const STREAMER_REPEATS: usize = 1;

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
                // Announcing a bite twice must not wipe its source list.
                if !stream_metadata.streamers.contains_key(&hash) {
                    stream_metadata.stream_bites.push(hash);
                    stream_metadata.streamers
                        .insert(hash, StreamerQueue::new(STREAMER_REPEATS));
                }

                println!("[mgr] '{stream_name}' bite {} stored ({} total)",
                         short_hash(&hash), stream_metadata.stream_bites.len());
            }
            else {
                println!("[mgr] '{stream_name}' rejected bite {}: wrong key",
                         short_hash(&hash));
            }
        }
    }

    pub fn register_streamer(&mut self, stream_ip: String, stream_name: String,  hash: HashKey) {
        // If the stream or queue is not found the manager should do nothing
        if let Some(stream) = self.current_streams.get_mut(&stream_name) {
            match stream.streamers.get_mut(&hash) {
                Some(queue) => {
                    queue.push_back(stream_ip.clone());
                    println!("[mgr] '{stream_name}' bite {} now has {} streamer(s)",
                             short_hash(&hash), queue.len());
                }
                None => println!("[mgr] '{stream_name}' has no bite {}, dropping {stream_ip}",
                                 short_hash(&hash)),
            }
        }
        else {
            println!("[mgr] no stream '{stream_name}', dropping streamer {stream_ip}");
        }
    }

    pub fn get_streamer(&mut self, viewer_ip: IP, stream_name: &String, hash: HashKey) -> Option<&IP> {
        if let Some(stream) = self.current_streams.get_mut(stream_name) {
            if let Some(queue) = stream.streamers.get_mut(&hash) {
                let streamer_ip = match queue.pop_front() {
                    Some(ip) => ip,
                    None => {
                        println!("[mgr] '{stream_name}' bite {} has nobody serving it",
                                 short_hash(&hash));
                        return None;
                    }
                };

                println!("[mgr] {viewer_ip} <- {streamer_ip} for {}", short_hash(&hash));

                // If there is already a vector waitlist, add the viewer to it
                // Create the vector if one does not exist
                match stream.viewer_waitlist.get_mut(streamer_ip) {
                    Some(viewers) => viewers.push((viewer_ip.clone(), hash)),
                    None => {stream.viewer_waitlist.insert(streamer_ip.clone(), vec![(viewer_ip.clone(), hash)]);}
                }

                return Some(streamer_ip)
            }

            println!("[mgr] '{stream_name}' knows nothing about bite {}", short_hash(&hash));
            return None;
        }

        println!("[mgr] {viewer_ip} asked for unknown stream '{stream_name}'");
        return None;
    }

    // Removes the viewer list from the waitlist and returns it
    pub fn flush_waitlist(&mut self, streamer_ip: IP, stream_name: &String) -> Option<Vec<(IP, HashKey)>> {
        if let Some(stream) = self.current_streams.get_mut(stream_name) {
            let queued = stream.viewer_waitlist.remove(&streamer_ip);

            if queued.is_none() && !stream.viewer_waitlist.is_empty() {
                println!("[mgr] nothing queued for {streamer_ip}, but {:?} are owed viewers",
                         stream.viewer_waitlist.keys().collect::<Vec<_>>());
            }

            return queued;
        }

        return None;
    }


    pub fn create_stream(&mut self, stream_name: String, stream_key: String) {
        // Replacing would drop every bite and source the stream already has.
        if let Some(live) = self.current_streams.get(&stream_name) {
            if live.stream_key == stream_key {
                println!("[mgr] '{stream_name}' already live with {} bite(s), keeping it",
                         live.stream_bites.len());
            }
            else {
                println!("[mgr] '{stream_name}' is live under a different key, refusing");
            }

            return;
        }

        let metadata = StreamMetadata {
            stream_name: stream_name.clone(),
            stream_key: stream_key.clone(),
            stream_bites: Vec::new(),
            streamers: HashMap::new(),
            viewer_waitlist: HashMap::new(),
        };

        println!("[mgr] created stream '{stream_name}'");
        self.current_streams.insert(stream_name, metadata);
    }

    pub fn get_stream_bites(&self, stream_name: String) -> Option<Vec<HashKey>> {
        if let Some(stream) = self.current_streams.get(&stream_name) {
            return Some(stream.stream_bites.clone());
        }

        println!("[mgr] no such stream '{stream_name}'");
        None
    }
}


use tokio::sync::{mpsc, oneshot};
use crate::media_server::media_data::{HashKey, IP};
use crate::tracker::tracker_manager::TrackerManager;

pub enum TrackerEvent {
    NewStream {
        stream_name: String,
        key: String,
    },
    GetStreamBiteInfo {
        stream_name: String,
        oneshot_sender: oneshot::Sender<Option<Vec<HashKey>>>
    },
    UploadBiteInfo {
        stream_name: String,
        key: String,
        hash: HashKey,
    },
    RegisterStreamer {
        streamer_ip: IP,
        stream_name: String,
        hash: HashKey,
    },
    GetViewerWaitList {
        streamer_ip: IP,
        stream_name: String,
        oneshot_sender: oneshot::Sender<Option<Vec<(IP, HashKey)>>>
    },
    RequestDownload {
        viewer_ip: IP,
        stream_name: String,
        hash: HashKey,
        oneshot_sender: oneshot::Sender<Option<IP>>
    }
}

pub struct TrackerService {
    manager: TrackerManager,
    pub sender: mpsc::Sender<TrackerEvent>,
    receiver: mpsc::Receiver<TrackerEvent>
}

impl TrackerService {
    pub fn from(manager: TrackerManager) -> Self {
        let (sender, receiver) = mpsc::channel::<TrackerEvent>(100);

        Self { manager, sender, receiver }
    }

    pub async fn run(mut self) {
        // Loop while the channel is open
        // recv will return none if the channel has been closed
        while let Some(event) = self.receiver.recv().await {
            match event {
                TrackerEvent::NewStream { stream_name, key } => {
                    self.manager.create_stream(stream_name, key);
                    println!("[svc] streams live: {}", self.manager.current_streams.len());
                }
                TrackerEvent::GetStreamBiteInfo { stream_name, oneshot_sender } => {
                    let stream_bites = self.manager.get_stream_bites(stream_name);

                    // If the receiver is dropped the service
                    // shouldn't react.
                    let _ = oneshot_sender.send(stream_bites);
                }
                TrackerEvent::UploadBiteInfo { stream_name, key, hash } => {
                    self.manager.insert_stream_bite(stream_name, key, hash);
                },
                TrackerEvent::RegisterStreamer { streamer_ip, stream_name, hash} => {
                    self.manager.register_streamer(streamer_ip, stream_name, hash);
                }
                TrackerEvent::RequestDownload {
                    viewer_ip,
                    stream_name,
                    hash,
                    oneshot_sender
                } => {
                    let ip = self.manager.get_streamer(viewer_ip, &stream_name, hash).cloned();
                    let _ = oneshot_sender.send(ip);
                }
                TrackerEvent::GetViewerWaitList {
                    streamer_ip,
                    stream_name,
                    oneshot_sender
                } => {
                    let viewers = self.manager.flush_waitlist(streamer_ip, &stream_name);
                    let _ = oneshot_sender.send(viewers);
                }
            }
        }
    }
}

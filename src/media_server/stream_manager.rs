use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
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
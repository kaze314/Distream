use std::io::Read;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use crate::media_server::media_data::*;
use crate::tracker::tracker_service::TrackerEvent;

const MAX_STREAM_NAME_LENGTH: usize = 128;
const MIN_STREAM_KEY_LENGTH: usize = 32;
const MAX_STREAM_KEY_LENGTH: usize = 128;

#[repr(u32)]
pub enum Message {
    Fail = 0,
    Understood = 1,
    Finished = 2,
    InsertStreamBite = 3,
    RegisterStreamer = 4,
    RequestDownload = 5,
    RequestStreamInfo = 6,
    CreateStream = 7,
    RequestViewerWaitlist = 8,
}

impl TryFrom<u32> for Message {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Message::Fail),
            1 => Ok(Message::Understood),
            2 => Ok(Message::Finished),
            3 => Ok(Message::InsertStreamBite),
            4 => Ok(Message::RegisterStreamer),
            5 => Ok(Message::RequestDownload),
            6 => Ok(Message::RequestStreamInfo),
            7 => Ok(Message::CreateStream),
            8 => Ok(Message::RequestViewerWaitlist),
            _ => Err(()),
        }
    }
}

pub async fn handle_connection(
    mut socket: TcpStream,
    mut service_channel: mpsc::Sender<TrackerEvent>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut message_bytes = [0; 4];

    loop {
        let read = socket.read_exact(&mut message_bytes).await?;

        if read == 0 {
            println!("Client disconnected");
            return Ok(());
        }

        println!("Received: {:?}", &message_bytes[..read]);
        let message_id = u32::from_be_bytes(message_bytes);
        match Message::try_from(message_id) {
            Ok(message) => handle_message(&mut socket, &mut service_channel, message).await,
            Err(()) => println!("Unknown Message"),
        }

    }
}

async fn handle_message(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>,
    message: Message
) {
    match message {
        Message::CreateStream => create_stream_handler(socket, service_channel).await,
        Message::InsertStreamBite => insert_stream_bite_handler(socket, service_channel).await,
        Message::RegisterStreamer => register_streamer_handler(socket, service_channel).await,
        Message::RequestStreamInfo => request_stream_info_handler(socket, service_channel).await,
        Message::RequestDownload => request_download_handler(socket, service_channel).await,
        Message::RequestViewerWaitlist => get_viewer_waitlist_handler(socket, service_channel).await,
        _ => {}
    }
}

async fn create_stream_handler(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>
) {
    let (stream_name, stream_key) = get_name_and_key(socket).await;
    let event = TrackerEvent::NewStream {
        stream_name: stream_name,
        key: stream_key,
    };

    service_channel.send(event).await.expect("Couldn't send message");
}

async fn insert_stream_bite_handler(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>
) {
    let (stream_name, stream_key) = get_name_and_key(socket).await;
    let hash = get_hash_key(socket).await;
    let event = TrackerEvent::UploadBiteInfo {
        stream_name: stream_name,
        key: stream_key,
        hash: hash,
    };

    service_channel.send(event).await.expect("Couldn't send message");
}

async fn register_streamer_handler(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>
) {
    let streamer_ip = socket.peer_addr()
        .expect("Could not get streamer address")
        .to_string();

    let stream_name = get_string(
        socket,
        MAX_STREAM_NAME_LENGTH,
        None
    ).await.expect("Couldn't get stream name");

    let hash = get_hash_key(socket).await;
    let event = TrackerEvent::RegisterStreamer {
        streamer_ip,
        stream_name,
        hash,
    };

    service_channel.send(event).await.expect("Failed to register streamer.");
}

async fn request_stream_info_handler(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>
) {
    let stream_name = get_string(
        socket,
        MAX_STREAM_NAME_LENGTH,
        None
    ).await.expect("Couldn't get stream name");

    let (tx, rx) = oneshot::channel::<Option<Vec<HashKey>>>();
    let event = TrackerEvent::GetStreamBiteInfo {
        stream_name,
        oneshot_sender: tx,
    };

    service_channel.send(event).await
        .expect("Failed to get stream bite info.");

    let hashes = rx.await
        .expect("Failed to get stream bite info.")
        .expect("Failed to find stream.");

    send_hash_info(socket, hashes.as_slice()).await.unwrap();
}

async fn request_download_handler(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>
) {
    let viewer_ip = socket.peer_addr()
        .expect("Could not get streamer address")
        .to_string();

    let stream_name = get_string(
        socket,
        MAX_STREAM_NAME_LENGTH,
        None
    ).await.expect("Couldn't get stream name");
    let hash = get_hash_key(socket).await;

    let (tx, rx) = oneshot::channel::<IP>();
    let event = TrackerEvent::RequestDownload {
        viewer_ip,
        stream_name,
        hash,
        oneshot_sender: tx,
    };

    service_channel.send(event).await
        .expect("Failed to get stream bite info.");

    let streamer_ip = rx.await
        .expect("Failed to get stream bite info.");

    send_string(socket, &*streamer_ip).await.expect("Couldn't send streamer.");
}

async fn get_viewer_waitlist_handler(
    socket: &mut TcpStream,
    service_channel: &mut mpsc::Sender<TrackerEvent>
) {
    let streamer_ip = socket.peer_addr()
        .expect("Could not get streamer address")
        .to_string();

    let stream_name = get_string(
        socket,
        MAX_STREAM_NAME_LENGTH,
        None
    ).await.expect("Couldn't get stream name");

    let (tx, rx) = oneshot::channel::<Vec<(IP, HashKey)>>();
    let event = TrackerEvent::GetViewerWaitList {
        streamer_ip,
        stream_name,
        oneshot_sender: tx,
    };

    service_channel.send(event).await
        .expect("Failed to get stream bite info.");

    let viewers = rx.await
        .expect("Failed to get stream bite info.");

    send_viewer_info(socket, viewers.as_slice()).await.unwrap();
}

async fn get_string(socket: &mut TcpStream, max_length: usize, min_length: Option<usize>) -> Result<String, ()> {
    let mut string_length_bytes: [u8; 4] = [0; 4];
    socket.read_exact(&mut string_length_bytes).await
        .expect("Could not read string length");

    let stream_name_length = u32::from_le_bytes(string_length_bytes) as usize;

    if stream_name_length > max_length {
        return Err(());
    }

    if let Some(min_len) = min_length {
        if stream_name_length < min_len {
            return Err(());
        }
    }

    let mut string_bytes: Vec<u8> = vec![0; stream_name_length];
    socket.read_exact(&mut string_bytes).await
        .expect("Could not read stream key");

    let stream_name = String::from_utf8(string_bytes)
        .expect("Could not read stream name");

    Ok(stream_name)
}

pub async fn send_string(socket: &mut TcpStream, s: &str) -> Result<(), ()> {
    let bytes = s.as_bytes();
    let length = bytes.len() as u32;

    socket.write_all(&length.to_le_bytes()).await
        .map_err(|_| ())?;

    socket.write_all(bytes).await
        .map_err(|_| ())?;

    Ok(())
}

async fn get_hash_key(socket: &mut TcpStream) -> HashKey {
    let mut stream_bite_hash: HashKey = [0; 32];
    socket.read_exact(&mut stream_bite_hash).await
        .expect("Could not read stream hash");

    stream_bite_hash
}

// Send information on the last 100 hashes.
async fn send_hash_info(stream: &mut TcpStream, mut data: &[HashKey]) -> std::io::Result<()> {
    if data.len() > 100 {
        data = &data[data.len() - 100..];
    }
    stream.write_u32(data.len() as u32).await?;
    for hashKey in data {
        stream.write_all(hashKey).await?;
    }

    Ok(())
}

// Get information on the last 100 hashes.
async fn recv_hash_info(stream: &mut TcpStream) -> std::io::Result<Vec<HashKey>> {
    let len = stream.read_u32().await? as usize;
    let mut buf = Vec::with_capacity(len);

    for i in 0..len {
        let mut hash: HashKey = [0; 32];
        stream.read_exact(&mut hash).await?;
        buf.push(hash);
    }

    Ok(buf)
}

// Send information on the last 100 hashes.
async fn send_viewer_info(stream: &mut TcpStream, data: &[(IP, HashKey)]) -> std::io::Result<()> {
    let mut stream = BufWriter::new(stream);

    stream.write_u32(data.len() as u32).await?;
    for (ip, hash) in data {
        let bytes = ip.as_bytes();
        stream.write_u32(bytes.len() as u32).await?;
        stream.write_all(bytes).await?;

        stream.write_all(hash).await?;
    }

    stream.flush().await?;
    Ok(())
}

pub async fn recv_viewer_info(stream: &mut TcpStream) -> std::io::Result<Vec<(IP, HashKey)>> {
    let count = stream.read_u32().await? as usize;
    let mut result = Vec::with_capacity(count);

    for _ in 0..count {
        let len = stream.read_u32().await? as usize;
        let mut bytes = vec![0u8; len];
        stream.read_exact(&mut bytes).await?;
        let ip = String::from_utf8(bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut hash = [0u8; 32];
        stream.read_exact(&mut hash).await?;

        result.push((ip, hash));
    }

    Ok(result)
}


// Returns (Stream Name, Stream Key)
async fn get_name_and_key(stream: &mut TcpStream) -> (String, String) {
    let stream_name = get_string(
        stream,
        MAX_STREAM_NAME_LENGTH,
        None
    ).await.expect("Couldn't get stream name");

    let stream_key = get_string(
        stream,
        MAX_STREAM_KEY_LENGTH,
        Some(MIN_STREAM_KEY_LENGTH)
    ).await.expect("Couldn't get stream key");

    (stream_name, stream_key)
}



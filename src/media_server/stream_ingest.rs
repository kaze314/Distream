use std::collections::HashMap;
use std::io::Write;
use tokio_stream::StreamExt;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Buf;
use sha2::{Digest, Sha256};
use sha2::digest::consts::False;
use srt_tokio::{SrtListener, SrtSocket};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use crate::media_server::media_data::{HashKey, MediaStream, StreamBite, IP, STREAM_BITE_SIZE};
use crate::media_server::settings::Settings;
use crate::tracker::tracker_connection;
use crate::tracker::tracker_connection::Message;
use crate::media_server::stream_manager::StreamManager;


pub async fn update_file_loop(stream_manager: Arc<StreamManager>) {
    tokio::time::sleep(Duration::from_secs(10)).await;

    loop {
        stream_manager.save_bite().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
pub async fn update_stream_loop(stream_manager: Arc<StreamManager>) {
    let mut tracker = TcpStream::connect(stream_manager.settings.tracker.clone()).await
        .expect("Failed to connect to tracker.");

    loop {
        let hashes = stream_manager.request_stream_info(&mut tracker).await;
        for hash in hashes {
            if stream_manager.stream_bites.read().await.contains_key(&hash) {
                continue;
            }

            let streamer_ip = stream_manager.request_download(&mut tracker, &hash).await;
            let manager_clone = stream_manager.clone();

            tokio::spawn(async move {
                let (hash, bite) = download_bite(streamer_ip).await;
                manager_clone.stream_bites.write().await.insert(hash, Arc::new(bite));
            });

            let bite = stream_manager.stream_bites.read().await.get(&hash).cloned();
            if let Some(bite) = bite {
                let hash = stream_manager.register_bite(&mut tracker, &bite).await;
            }
        }
    }
}

pub async fn download_bite(streamer_ip: IP) -> (HashKey, StreamBite) {
    let streamer_ip = format!("{}:34554", streamer_ip);
    let mut streamer_conn = SrtSocket::builder()
        .local_port(34554)
        .rendezvous(streamer_ip)
        .await.expect("Failed to connect to viewer.");

    let mut bite = Vec::with_capacity(STREAM_BITE_SIZE);
    while bite.len() < STREAM_BITE_SIZE {
        if let Some((origin, message)) = streamer_conn.try_next().await.expect("Failed to get streamer.") {
            bite.extend_from_slice(&message);
        }
    }

    let hash = StreamManager::get_stream_bite_hash(&bite);
    (hash, bite)
}

pub async fn update_viewers_loop(stream_manager: Arc<StreamManager>) {
    let mut tracker = TcpStream::connect(stream_manager.settings.tracker.clone()).await
        .expect("Failed to connect to tracker.");

    loop {
        let mut handles = Vec::new();
        let viewers = stream_manager.get_viewers(&mut tracker).await;

        for (viewer_ip, hash) in viewers {
            let stream_bite = stream_manager.stream_bites.read().await.get(&hash).expect("Error").clone();
            let handle = tokio::spawn(
                async move{
                    update_viewer(viewer_ip, stream_bite).await
                }
            );

            handles.push(handle);
        }

        for handle in handles {
            handle.await;
        }
    }
}

async fn update_viewer(viewer_ip: IP, stream_bite: Arc<StreamBite>) {
    let viewer_client = format!("{}:34554", viewer_ip);
    let mut viewer_conn = SrtSocket::builder()
        .local_port(34554)
        .rendezvous(viewer_client)
        .await.expect("Failed to connect to viewer.");

    viewer_conn.try_send(Instant::now(), stream_bite.as_slice().copy_to_bytes(stream_bite.len())).expect("TODO: panic message");
}

pub async fn ingest_distream(settings: Settings) {
    let stream_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}.ts", settings.stream_name)).expect("Failed to open file.");

    let stream_manager = Arc::new(StreamManager::from(settings).await);
    let t1 = tokio::task::spawn(update_stream_loop(stream_manager.clone()));
    let t2 = tokio::task::spawn(update_viewers_loop(stream_manager.clone()));
    let t3 = tokio::task::spawn(update_viewers_loop(stream_manager));

    tokio::join!(t1, t2, t3);
}


// This socket is for standard stream ingest from obs or
// whatever streaming method.
pub async fn ingest_traditional(settings: Settings) {

    let port = 3333;
    let (_binding, mut incoming) = SrtListener::builder().bind(port).await.expect("Failed to bind");

    println!("Origin SRT Server is listening on port: {port}");

    while let Some(request) = incoming.incoming().next().await {
        let srt_socket: SrtSocket = request.accept(None).await.expect("Failed to accept socket");
        let client_desc = format!(
            "(ip_port: {}, sockid: {})",
            srt_socket.settings().remote,
            srt_socket.settings().remote_sockid.0
        );

        println!("\nNew client connected: {client_desc}");
        let settings_clone = settings.clone();
        tokio::spawn(async move { handle_traditional_stream(srt_socket, settings_clone).await });
    }
}

async fn handle_traditional_stream(
    mut socket: SrtSocket,
    settings: Settings,
) {
    let client_desc = format!(
        "(ip_port: {}, sockid: {}, streamid: {})",
        socket.settings().remote,
        socket.settings().remote_sockid.0,
        socket.settings().stream_id.clone().expect("No stream id")
    );

    println!("\nNew client connected: {client_desc}");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(format!("{}.ts", settings.stream_name)).expect("Failed to open file.");

    let stream_manager = Arc::new(StreamManager::from(settings).await);

    let mut tracker = TcpStream::connect(stream_manager.settings.tracker.clone()).await
        .expect("Failed to connect to tracker.");

    stream_manager.register_stream(&mut tracker).await;
    tokio::task::spawn(update_viewers_loop(stream_manager.clone()));

    let mut count = 0;
    let mut stream_bite = vec![0u8; STREAM_BITE_SIZE];


    while let Some((_instant, bytes)) = socket.try_next().await.unwrap() {


        file.write_all(&bytes).expect("Failed to write to file.");

        if count + bytes.len() >= STREAM_BITE_SIZE {

            let hash = stream_manager.register_bite(&mut tracker, &stream_bite).await;

            println!("New stream bite! Hash: {hash:?}");
            stream_bite = vec![0u8; STREAM_BITE_SIZE];
            count = 0;
        }

        stream_bite[count..count+bytes.len()].copy_from_slice(bytes.as_ref());
        count += bytes.len();

    }

    println!("\nClient {client_desc} disconnected");
}


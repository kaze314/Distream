
use std::io::Write;
use tokio_stream::StreamExt;
use std::fs::OpenOptions;
use std::sync::Arc;
use sha2::{Digest, Sha256};
use srt_tokio::{SrtListener, SrtSocket};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use crate::media_server::media_data::{HashKey, StreamBite, STREAM_BITE_SIZE};
use crate::media_server::settings::Settings;
use crate::tracker::tracker_connection;
use crate::tracker::tracker_connection::Message;
use crate::media_server::stream_manager::StreamManager;




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

    let mut stream_manager = StreamManager::from(settings).await;
    stream_manager.register_stream().await;


    let mut count = 0;
    let mut stream_bite = vec![0u8; STREAM_BITE_SIZE];


    while let Some((_instant, bytes)) = socket.try_next().await.unwrap() {


        file.write_all(&bytes).expect("Failed to write to file.");

        if count + bytes.len() >= STREAM_BITE_SIZE {

            let hash = stream_manager.register_bite(&stream_bite).await;

            println!("New stream bite! Hash: {hash:?}");
            stream_bite = vec![0u8; STREAM_BITE_SIZE];
            count = 0;
        }

        stream_bite[count..count+bytes.len()].copy_from_slice(bytes.as_ref());
        count += bytes.len();

    }

    println!("\nClient {client_desc} disconnected");
}

async fn handle_viewer() {

}



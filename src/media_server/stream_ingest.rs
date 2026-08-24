
use tokio_stream::StreamExt;
use std::fs::OpenOptions;
use std::sync::Arc;
use srt_tokio::{SrtListener, SrtSocket};
use tokio::sync::mpsc::Sender;
use crate::media_server::media_data::STREAM_BITE_SIZE;
use crate::media_server::stream_manager::ServerEvent;

async fn start_stream(sender: Sender<ServerEvent>) {
    let args: Vec<String> = env::args().collect();
}


// This socket is for standard stream ingest from obs or
// whatever streaming method.
async fn ingest_traditional() {

    let port = 3333;
    let (_binding, mut incoming) = SrtListener::builder().bind(port).await.expect("Failed to bind");

    println!("SRT Server is listening on port: {port}");

    let sender_arc = Arc::new(sender);
    while let Some(request) = incoming.incoming().next().await {
        let srt_socket: SrtSocket = request.accept(None).await.expect("Failed to accept socket");
        let client_desc = format!(
            "(ip_port: {}, sockid: {})",
            srt_socket.settings().remote,
            srt_socket.settings().remote_sockid.0
        );

        println!("\nNew client connected: {client_desc}");
        let sender_copy = sender_arc.clone();
        tokio::spawn(async move { handle_traditional_stream(sender_copy, srt_socket).await });
    }
}

async fn handle_traditional_stream(
    server_data: Arc<Sender<ServerEvent>>,
    mut socket: SrtSocket,
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
        .open("received_from_obs.ts").expect("Failed to open file.");

    let mut count = 0;
    let mut stream_bite = vec![0u8; STREAM_BITE_SIZE];

    {
        let mut data = server_data.lock().unwrap();
        data.new_stream(
            &"My first stream!".to_string(),
            "127.0.0.1:3334".to_string()
        ).expect("Failed to create new stream.");
    }

    while let Some((_instant, bytes)) = socket.try_next().await.unwrap() {


        file.write_all(&bytes).expect("Failed to write to file.");

        if count + bytes.len() >= STREAM_BITE_SIZE {
            let hash = DistreamServerData::get_stream_bite_hash(&stream_bite);
            let mut data = server_data.lock().unwrap();


            data.insert(
                &"My first stream!".to_string(),
                stream_bite
            ).expect("Failed to update insert stream bite");

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



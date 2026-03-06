

use std::io::Error;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Deref;
use srt_tokio::{SrtListener, SrtSocket};
use tokio_stream::StreamExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use distream::media_server::media_server_data::DistreamServerData;
use distream::media_server::media_server_data::STREAM_BITE_SIZE;


#[tokio::main]
async fn main() -> Result<(), Error> {


    let server = Arc::new(Mutex::new(DistreamServerData::new()));
    let task1 = tokio::spawn(async move { accept_streamer(server).await });
    let task2 = tokio::spawn(async move { say_hello().await });


    tokio::join!(task2, task1);
    Ok(())
}

async fn say_hello() {
    loop{
        tokio::time::sleep(Duration::from_millis(5)).await;
        println!("hello");
    }

}
async fn accept_streamer(server_data: Arc<Mutex<DistreamServerData>>) {
    let port = 3333;
    let (_binding, mut incoming) = SrtListener::builder().bind(port).await.expect("Failed to bind");

    println!("SRT Server is listening on port: {port}");

    while let Some(request) = incoming.incoming().next().await {
        let srt_socket: SrtSocket = request.accept(None).await.expect("Failed to accept socket");
        let server = server_data.clone();
        tokio::spawn(async move { handle_streamer(server, srt_socket).await });
    }
}

async fn handle_streamer(server_data: Arc<Mutex<DistreamServerData>>, mut socket: SrtSocket) {
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
        .open("received_from_obs.ts").expect("e43d");

    let mut count = 0;
    let mut stream_bite = vec![0u8; STREAM_BITE_SIZE];
    while let Some((_instant, bytes)) = socket.try_next().await.unwrap() {


        file.write_all(&bytes).expect("ed");

        if count + bytes.len() >= STREAM_BITE_SIZE {
            let hash = DistreamServerData::get_stream_bite_hash(&stream_bite);
            let mut data = server_data.lock().unwrap();


            data.insert(stream_bite);
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

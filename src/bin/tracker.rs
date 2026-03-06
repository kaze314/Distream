use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use distream::media_server::media_server_data::DistreamServerData;
use distream::tracker::tracker_data::TrackerData;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:3334").await?;
    println!("Server running on port 3334");
    let tracker_data = Arc::new(Mutex::new(TrackerData::new()));
    
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection: {addr}");
        
        let tracker_data_clone = tracker_data.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, tracker_data_clone).await {
                println!("Connection error: {e}");
            }
        });
    }
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    tracker_data: Arc<Mutex<TrackerData>>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = [0; 1024];

    loop {
        let n = socket.read(&mut buffer).await?;

        if n == 0 {
            println!("Client disconnected");
            return Ok(());
        }

        println!("Received: {:?}", &buffer[..n]);

        socket.write_all(b"Message received\n").await?;
    }
}
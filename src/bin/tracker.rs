
use tokio::net::{TcpListener, TcpStream};
use distream::tracker::tracker_manager::TrackerManager;
use distream::tracker::tracker_service::TrackerService;

use distream::tracker::tracker_connection;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bind to the standard port 3334
    let listener = TcpListener::bind("0.0.0.0:3334").await?;
    println!("[tracker] listening on 0.0.0.0:3334");


    let tracker_data = TrackerManager::new();
    let tracker_service = TrackerService::from(tracker_data);
    let sender = tracker_service.sender.clone();

    // Start the tracker service
    tokio::spawn( async move {
        println!("[svc] event loop started");
        tracker_service.run().await;
        println!("[svc] event loop ended, channel closed");
    });


    // Handle incoming connections
    let mut connections: u64 = 0;

    loop {
        let (socket, addr) = listener.accept().await?;
        connections += 1;
        println!("[tracker] accepted {addr} (#{connections})");

        let sender_clone =  sender.clone();
        tokio::spawn(async move {
            if let Err(e) = tracker_connection::handle_connection(socket, sender_clone).await {
                println!("[tracker] {addr} dropped: {e}");
            }
        });
    }
}


use std::io::Error;
use std::fs::OpenOptions;
use std::io::Write;
use std::ops::Deref;
use srt_tokio::{SrtListener, SrtSocket};
use tokio_stream::StreamExt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use distream::media_server::stream_manager::stream_manager;
use distream::media_server::stream_manager::ServerEvent;

use distream::media_server::media_data::STREAM_BITE_SIZE;
use distream::tracker::tracker_manager;

#[tokio::main]
async fn main() -> Result<(), Error> {


    let (tx, rx): (Sender<ServerEvent>, Receiver<ServerEvent>) = mpsc::channel(100);

    // Stream manager
    let task1 = tokio::spawn(async move { stream_manager(rx).await });
    let task2 = tokio::spawn(async move { start_stream(tx).await });

    tokio::join!(task2, task1);

    Ok(())
}



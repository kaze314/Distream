use std::env;
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
//use distream::media_server::stream_manager::stream_manager;

use distream::media_server::media_data::STREAM_BITE_SIZE;
use distream::tracker::tracker_manager;
use distream::media_server::settings::{process_cmd, Settings};
use distream::media_server::stream_ingest::*;

#[tokio::main]
async fn main() -> Result<(), Error> {

    let settings = process_cmd();
    println!("{:#?}", settings);

    if settings.is_origin {
        ingest_traditional(settings).await;
    }
    else{
        ingest_distream(settings).await;
    }

    Ok(())
}




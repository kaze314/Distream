use std::io::Error;

use distream::media_server::media_data::STREAM_BITE_SIZE;
use distream::media_server::settings::{process_cmd, Settings};
use distream::media_server::stream_ingest::*;

#[tokio::main]
async fn main() -> Result<(), Error> {

    let settings = process_cmd();
    println!("{:#?}", settings);
    println!("bite size: {STREAM_BITE_SIZE} bytes");

    if settings.is_origin {
        println!("mode: origin (taking SRT in on 3333)");
        ingest_traditional(settings).await;
    }
    else{
        println!("mode: peer (pulling from the swarm)");
        ingest_distream(settings).await;
    }

    Ok(())
}




use std::collections::{HashMap, HashSet};
use std::io::Write;
use tokio_stream::StreamExt;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::{Buf, Bytes};
use sha2::{Digest, Sha256};
use sha2::digest::consts::False;
use srt_tokio::options::{ByteCount, LiveBandwidthMode};
use srt_tokio::{SrtListener, SrtSocket, SrtSocketBuilder};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use crate::media_server::media_data::{short_hash, HashKey, MediaStream, StreamBite, IP, MAX_PACKET_SIZE, STREAM_BITE_SIZE};
use crate::media_server::settings::Settings;
use crate::tracker::tracker_connection;
use crate::tracker::tracker_connection::Message;
use crate::media_server::stream_manager::StreamManager;
use crate::media_server::port_pool::{PortPool, PORT_COUNT};

// Only open MAX_IN_FLIGHT ports
const MAX_IN_FLIGHT: usize = 8;

const STREAMER_PORT_OFFSET: u16 = PORT_COUNT;

const BULK_LATENCY: Duration = Duration::from_millis(200);

const INGEST_LATENCY: Duration = Duration::from_secs(2);

fn bulk_socket() -> SrtSocketBuilder {
    SrtSocket::builder().set(|options| {
        options.sender.bandwidth = LiveBandwidthMode::Unlimited;
        options.sender.peer_latency = BULK_LATENCY;
        options.sender.buffer_size = ByteCount((STREAM_BITE_SIZE * 4) as u64);
        options.sender.drop_delay = Duration::from_secs(10);
        options.receiver.too_late_packet_drop = false;

        options.receiver.latency = BULK_LATENCY;
        options.receiver.buffer_size = ByteCount((STREAM_BITE_SIZE * 4) as u64);
    })
}

pub async fn update_file_loop(stream_manager: Arc<StreamManager>) {
    loop {
        // Drain what is ready; one bite a second caps the file at 5.3 Mbit/s.
        while stream_manager.save_bite().await {}

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
pub async fn update_stream_loop(stream_manager: Arc<StreamManager>) {
    let tracker_addr = stream_manager.settings.tracker.clone();
    println!("[peer] dialling tracker {tracker_addr}");

    let mut tracker = TcpStream::connect(tracker_addr).await
        .expect("Failed to connect to tracker.");
    let port_pool = PortPool::new(stream_manager.settings.port_base);

    println!("[peer] tracker connected, polling for '{}', staying {} chunk(s) back",
             stream_manager.settings.stream_name, stream_manager.settings.lag);

    let mut last_count = 0;

    // A bite reaches stream_bites only when done, so track what is in flight.
    let in_flight: Arc<tokio::sync::Mutex<HashSet<HashKey>>> = Arc::new(tokio::sync::Mutex::new(HashSet::new()));

    // Every hash we have ever claimed a slot in the file for.
    let mut queued: HashSet<HashKey> = HashSet::new();

    loop {
        let hashes = stream_manager.request_stream_info(&mut tracker).await;
        if hashes.len() != last_count {
            println!("[peer] tracker lists {} bite(s), we hold {}",
                     hashes.len(), stream_manager.stream_bites.read().await.len());
            last_count = hashes.len();
        }

        // Skip the newest
        let ripe = hashes.len().saturating_sub(stream_manager.settings.lag);

        let mut saturated = false;

        for hash in hashes.into_iter().take(ripe) {
            if stream_manager.stream_bites.read().await.contains_key(&hash) {
                continue;
            }

            {
                let mut flight = in_flight.lock().await;
                if flight.len() >= MAX_IN_FLIGHT {
                    saturated = true;
                    break;
                }

                // Already downloading
                if !flight.insert(hash) {
                    continue;
                }
            }

            // Claim its place in the file now.
            if queued.insert(hash) {
                stream_manager.queue_bite(hash).await;
            }

            println!("[peer] missing {}, asking tracker for a source", short_hash(&hash));

            let Some(port) = port_pool.get_new() else {
                println!("[peer] every port is busy, leaving {} for later", short_hash(&hash));
                in_flight.lock().await.remove(&hash);
                saturated = true;
                break;
            };

            let streamer_ip = match stream_manager.request_download(&mut tracker, port, &hash).await {
                Some(ip) => ip,
                None => {
                    port_pool.release(port);
                    in_flight.lock().await.remove(&hash);
                    continue;
                }
            };

            let manager_clone = stream_manager.clone();
            let hash_clone = hash.clone();
            let port_clone = port_pool.clone();
            let flight_clone = in_flight.clone();
            tokio::spawn(async move {
                let downloaded = download_bite(streamer_ip, hash_clone, port).await;
                port_clone.release(port);
                flight_clone.lock().await.remove(&hash_clone);

                let Some((hash, bite)) = downloaded else {
                    return;
                };

                manager_clone.store_bite(hash, bite).await;
                println!("[dl] stored {}, now holding {} bite(s)",
                         short_hash(&hash),
                         manager_clone.stream_bites.read().await.len());

                // Identity connection, so the tracker files us as the peer
                // that polls the waitlist.
                manager_clone.register_streamer(&hash).await;
                println!("[dl] told tracker we can serve {}", short_hash(&hash));
            });
        }

        if saturated {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

pub async fn download_bite(streamer_ip: IP, hash_original: HashKey, port: usize) -> Option<(HashKey, StreamBite)> {
    let port = port as u16;
    let host = streamer_ip.split(":").next().unwrap_or(&streamer_ip);
    let streamer_ip = format!("{host}:{}", port + STREAMER_PORT_OFFSET);
    println!("[dl] want {} from {streamer_ip}, we sit on {port}", short_hash(&hash_original));

    let started = Instant::now();
    let mut streamer_conn = bulk_socket()
        .local_port(port)
        .rendezvous(streamer_ip.clone())
        .await.expect("Failed to connect to viewer.");

    let mut bite = Vec::with_capacity(STREAM_BITE_SIZE);
    let mut packets = 0;
    while bite.len() < STREAM_BITE_SIZE {
        // None means the streamer hung up
        let Some((_origin, message)) = streamer_conn.try_next().await.expect("Failed to get streamer.") else {
            println!("[dl] {streamer_ip} hung up at {}/{STREAM_BITE_SIZE} bytes after {:?}",
                     bite.len(), started.elapsed());
            return None;
        };

        bite.extend_from_slice(&message);
        packets += 1;
    }

    let hash = StreamManager::get_stream_bite_hash(&bite);
    println!("[dl] {streamer_ip} done: {} bytes in {packets} packets, took {:?}",
             bite.len(), started.elapsed());

    // Storing it under the wrong hash would leave the wanted bite missing.
    if hash != hash_original {
        println!("[dl] hash mismatch: asked for {}, got {}, dropping it", short_hash(&hash_original), short_hash(&hash));
        return None;
    }

    Some((hash, bite))
}

pub async fn update_viewers_loop(stream_manager: Arc<StreamManager>) {
    loop {
        let mut handles = Vec::new();
        let viewers = stream_manager.get_viewers().await;
        let empty = viewers.is_empty();

        if !empty {
            println!("[up] {} viewer(s) waiting on us", viewers.len());
        }

        for (viewer_ip, hash) in viewers {
            let held = stream_manager.stream_bites.read().await.get(&hash).cloned();
            let Some(stream_bite) = held else {
                println!("[up] {viewer_ip} wants {} and we do not have it", short_hash(&hash));
                continue;
            };

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

        if empty {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn update_viewer(viewer_ip: IP, stream_bite: Arc<StreamBite>) {
    let port = viewer_ip.split(":").collect::<Vec<&str>>()[1]
        .parse::<u16>().expect("Failed to parse port number");

    // They listen on the port they announced; we take the offset one.
    let local = port + STREAMER_PORT_OFFSET;
    let mut viewer_conn = bulk_socket()
        .local_port(local)
        .rendezvous(viewer_ip.clone())
        .await.expect("Failed to connect to viewer.");

    let size = stream_bite.len();
    let t = Instant::now();


    for chunk in stream_bite.chunks(MAX_PACKET_SIZE) {
        let mut payload = Bytes::copy_from_slice(chunk);


        while let Err((_, returned)) = viewer_conn.try_send(Instant::now(), payload) {
            payload = returned;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    // The receiver holds packets until their deadline; closing now drops them.
    tokio::time::sleep(BULK_LATENCY * 3).await;

    // Dropping viewer_conn here would kill the task still sending.
    if let Err(e) = viewer_conn.close_and_finish().await {
        println!("[up] {viewer_ip} did not finish: {e}");
        return;
    }

    println!("[up] pushed {size} bytes to {viewer_ip} in {:?}", t.elapsed());
}

pub async fn ingest_distream(settings: Settings) {

    println!("[peer] starting as a relay for '{}'", settings.stream_name);

    let stream_manager = Arc::new(StreamManager::from(settings).await);
    let t1 = tokio::task::spawn(update_stream_loop(stream_manager.clone()));
    let t2 = tokio::task::spawn(update_viewers_loop(stream_manager.clone()));
    let t3 = tokio::task::spawn(update_file_loop(stream_manager));

    tokio::join!(t1, t2, t3);
}

 
// This socket is for standard stream ingest from obs or
// whatever streaming method.
pub async fn ingest_traditional(settings: Settings) {

    let port = 3333;
    let (_binding, mut incoming) = SrtListener::builder()
        .set(|options| {
            options.receiver.latency = INGEST_LATENCY;
            options.receiver.too_late_packet_drop = false;
            options.sender.peer_latency = INGEST_LATENCY;


            options.receiver.buffer_size = ByteCount(32_000_000);
        })
        .bind(port).await.expect("Failed to bind");

    println!("[origin] SRT listener up on {port}, bite size is {STREAM_BITE_SIZE} bytes");

    while let Some(request) = incoming.incoming().next().await {
        let srt_socket: SrtSocket = request.accept(None).await.expect("Failed to accept socket");
        let client_desc = format!(
            "(ip_port: {}, sockid: {})",
            srt_socket.settings().remote,
            srt_socket.settings().remote_sockid.0
        );

        println!("[origin] accepted {client_desc}");
        let settings_clone = settings.clone();

        tokio::spawn(async move {
            handle_traditional_stream(srt_socket, settings_clone).await;
        });
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

    println!("[origin] ingesting from {client_desc}");
    let stream_manager = Arc::new(StreamManager::from(settings).await);

    stream_manager.register_stream().await;
    tokio::task::spawn(update_viewers_loop(stream_manager.clone()));
    tokio::task::spawn(update_file_loop(stream_manager.clone()));

    let mut count = 0;
    let mut stream_bite = vec![0u8; STREAM_BITE_SIZE];

    let mut packets: u64 = 0;
    let mut bites: u64 = 0;
    let mut total: u64 = 0;
    let started = Instant::now();

    while let Some((_instant, bytes)) = socket.try_next().await.unwrap() {

        packets += 1;
        total += bytes.len() as u64;

        if packets % 100 == 0 {
            let secs = started.elapsed().as_secs_f64();
            println!("[origin] {packets} packets, {total} bytes, {:.0} kbit/s, bite {count}/{STREAM_BITE_SIZE}",
                     (total as f64 * 8.0 / 1000.0) / secs.max(0.001));
        }

        let mut chunk = bytes.as_ref();
        while !chunk.is_empty() {
            let take = (STREAM_BITE_SIZE - count).min(chunk.len());
            stream_bite[count..count + take].copy_from_slice(&chunk[..take]);
            count += take;
            chunk = &chunk[take..];

            if count < STREAM_BITE_SIZE {
                continue;
            }

            let hash = stream_manager.register_bite(&stream_bite).await;
            bites += 1;

            println!("[origin] bite {} announced ({bites} so far)", short_hash(&hash));
            stream_bite = vec![0u8; STREAM_BITE_SIZE];
            count = 0;
        }

    }

    println!("[origin] {client_desc} disconnected after {packets} packets / {bites} bites / {total} bytes");
}


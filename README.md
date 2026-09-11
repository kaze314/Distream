# Distream

Distream is a P2P live streaming network POC. It allows users to view and create live streams without the need of a centralized server to distribute packets. A delay of under 10 seconds can be achieved with over 100 viewers with standard video quality. Even better performance can be achieved with some improvements. Every viewer who receives a piece of the stream becomes a streamer and distributes the packets to other clients.  The project was designed with ease of use in mind, and as such, port forwarding is not required for streamers or viewers.

# Design
Distream consists of two components, a tracker and a media server.

## Tracker
The tracker is responsible for facilitating the connection between viewers(clients who are requesting a piece of the stream) and streamers(clients with the video). The tracker never sends or receives any data from the stream. It receives the hash of a stream bite(an arbitrary number of MPEG packets) and contains a list of viewers and streamers. There are a few main interfaces the media server can access:

 - **NewStream** - Called once by the owner of the stream.
 - **GetStreamBiteInfo** - Used by viewers to get the list of hashes
 - **UploadBiteInfo** - Upload the hash of a new stream bite by the owner
 - **RegisterStreamer** - Add a viewer to the list of streamers.
 - **GetViewerWaitList** - Get the viewer queue waiting for a streamer to respond.
 - **RequestDownload** - Request a stream bite from a streamer.

## Media Server
The media server handles sending and receiving of stream bites. Depending on the users settings, the media server will serve different functions. If the user wants to create a new stream, a new connection will be created for a broadcast app such as OBS to receive the stream data. If the user wants to watch the live stream in VLC. A UDP live stream will be created witch sends pieces of the stream. In all cases, the media server will communicate with the tracker using the interfaces listed above to distribute the stream.

The connection from the broadcast app and to other peers uses the [SRT](https://github.com/Haivision/srt)  protocol. SRT was found to be a strong choice due to its ability to ensure reliability without stalling the system. Its NAT traversal in rendezvous mode was essential to ensuring accessibly. 

# Usage
Distream can be downloaded and ran in the command line. 

Build the project with:
```bash
cargo build
```
### Tracker
The tracker can be ran with:
```bash
cargo run --bin tracker
```

### Origin

The owner of the stream. Accepts the encoder on SRT port `3333`, cuts the stream into pieces and
informs the tracker. The password is the stream key and must be at least 32 characters.
```bash
cargo run --bin media_server -- --stream --tracker IP:PORT --name STREAM_NAME --password PASSWORD
```

In a broadcast application such as OBS, stream to the server:
```
srt://<origin-host>:3333?streamid=live&latency=2000000
```

### Peer
A member of the network. Used to interact with a stream. 
```bash
cargo run --bin media_server -- --tracker IP:PORT --name STREAM_NAME --ports 34662 --play 127.0.0.1:1234
```

**--play**  will create a UDP live stream which you can watch with VLC on ``udp://@:1234``

## Disclaimer
This project exists as a proof-of-concept. Large scale usage across multiple networks has not been tested. Security was not a focus when distream was designed, there are likely flaws that an attacker can exploit, both on the program and design level. There are several improvements that can be made to support streams with a large amount (>1000) of viewers however they are out of the scope of what I wanted to achieve.

use std::env;
use crate::media_server::port_pool::PORT_BASE;

const MAX_AUTO_LAG: usize = 4;

#[derive(Debug, Default, Clone)]
pub struct Settings {
    // The name of stream that will be requested from the tracker.
    pub stream_name: String,

    // Determines if the user is the origin of the stream.
    pub is_origin: bool,

    // Password for the stream. This is only needed if the user is the origin.
    pub stream_password: String,

    // tcp host:port
    pub tracker: String,

    // Where <stream_name>.ts goes
    pub out_dir: String,

    // udp host:port
    pub play_addr: String,

    // First rendezvous port this peer may use. Needs its own range if another
    // peer is running on the same machine.
    pub port_base: u16,

    // How many chunks behind the newest one this peer stays. See PLAYOUT_LAG.
    pub lag: usize,
}

pub fn process_cmd() -> Settings {
    let mut args = env::args().skip(1).peekable();
    let mut settings = Settings::default();
    let mut lag_given = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--stream" => settings.is_origin = true,
            "--password" => settings.stream_password = args.next().unwrap(),
            "--name" => settings.stream_name = args.next().unwrap(),
            "--tracker" => settings.tracker = args.next().unwrap(),
            "--out" => settings.out_dir = args.next().unwrap(),
            "--play" => settings.play_addr = args.next()
                .unwrap_or_else(|| "127.0.0.1:1234".to_string()),
            "--ports" => settings.port_base = args.next()
                .and_then(|p| p.parse().ok())
                .expect("--ports needs a port number"),
            "--lag" => {
                settings.lag = args.next()
                    .and_then(|l| l.parse().ok())
                    .expect("--lag needs a number of chunks");
                lag_given = true;
            }
            _ => {
                println!("Unknown command: {}", arg);
            }
        }
    }


    if settings.port_base == 0 {
        settings.port_base = PORT_BASE;
    }

    // Every viewer chasing the newest chunk asks for it while the origin is
    // still its only holder, so the origin serves all of them and no peer ever relays.
    // Adding lag helps.
    if !lag_given && !settings.is_origin {

        settings.lag = std::process::id() as usize % (MAX_AUTO_LAG + 1);
    }

    if settings.out_dir.is_empty() {
        settings.out_dir = if settings.is_origin { "origin" } else { "peer" }.to_string();
    }

    if settings.stream_name.is_empty() {
        println!("warning: no --name given, the stream file will be '.ts'");
    }

    if settings.tracker.is_empty() {
        println!("warning: no --tracker given, connecting will fail");
    }

    if settings.is_origin && settings.stream_password.len() < 32 {
        println!("warning: --password is {} chars, the tracker wants at least 32",
                 settings.stream_password.len());
    }

    settings
}
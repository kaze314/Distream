use std::env;

#[derive(Debug, Default, Clone)]
pub struct Settings {
    pub stream_name: String,
    pub is_origin: bool,
    pub stream_password: String,
    pub tracker: String,
}

pub fn process_cmd() -> Settings {
    let mut args = env::args().skip(1).peekable();
    let mut settings = Settings::default();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--stream" => settings.is_origin = true,
            "--password" => settings.stream_password = args.next().unwrap(),
            "--name" => settings.stream_name = args.next().unwrap(),
            "--tracker" => settings.tracker = args.next().unwrap(),
            _ => {
                println!("Unknown command: {}", arg);
            }
        }
    }

    return settings;
}
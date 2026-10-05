use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

pub fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind a free test port");
    listener.local_addr().unwrap().port()
}

pub struct RunningServer {
    child: Child,
}

impl RunningServer {
    pub fn start(args: &[String]) -> Self {
        let executable = std::env::var("CARGO_BIN_EXE_redis_like_cli")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                let mut path = std::env::current_exe().expect("locate integration test");
                path.pop();
                path.pop();
                path.push(if cfg!(windows) {
                    "redis-like-cli.exe"
                } else {
                    "redis-like-cli"
                });
                path
            });
        let child = Command::new(executable)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start server");
        Self { child }
    }

    #[allow(dead_code)]
    pub fn stop(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn wait_for_server(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set test read timeout");
            return stream;
        }
        assert!(Instant::now() < deadline, "server did not start on port {port}");
        sleep(Duration::from_millis(25));
    }
}

pub fn encode_command(parts: &[&str]) -> Vec<u8> {
    let mut command = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        command.extend(format!("${}\r\n{}\r\n", part.len(), part).as_bytes());
    }
    command
}

fn read_line(stream: &mut TcpStream) -> Vec<u8> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).expect("read RESP line");
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            return line;
        }
    }
}

pub fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut frame = Vec::new();
    let mut kind = [0; 1];
    stream.read_exact(&mut kind).expect("read RESP type");
    frame.push(kind[0]);
    let header = read_line(stream);
    frame.extend_from_slice(&header);

    match kind[0] {
        b'+' | b'-' | b':' => (),
        b'$' => {
            let length: isize = std::str::from_utf8(&header[..header.len() - 2])
                .expect("RESP bulk length")
                .parse()
                .expect("numeric RESP bulk length");
            if length >= 0 {
                let mut payload = vec![0; length as usize + 2];
                stream.read_exact(&mut payload).expect("read RESP bulk payload");
                frame.extend(payload);
            }
        }
        b'*' => {
            let count: isize = std::str::from_utf8(&header[..header.len() - 2])
                .expect("RESP array length")
                .parse()
                .expect("numeric RESP array length");
            for _ in 0..count.max(0) {
                frame.extend(read_frame(stream));
            }
        }
        other => panic!("unsupported RESP type {other:?}"),
    }
    frame
}

pub fn command(stream: &mut TcpStream, parts: &[&str]) -> String {
    stream
        .write_all(&encode_command(parts))
        .expect("write RESP command");
    String::from_utf8(read_frame(stream)).expect("UTF-8 RESP response")
}

pub fn server_args(port: u16) -> Vec<String> {
    vec![
        "--bind".into(),
        "127.0.0.1".into(),
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        std::env::temp_dir()
            .join(format!("redis-like-cli-test-{port}"))
            .to_string_lossy()
            .into_owned(),
    ]
}

#[allow(dead_code)]
pub fn sleep_ms(milliseconds: u64) {
    sleep(Duration::from_millis(milliseconds));
}

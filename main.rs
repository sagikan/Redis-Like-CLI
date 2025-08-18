#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = vec![0; 1024];
                loop {
                    let read_count = stream.read(&mut buf).unwrap();
                    if read_count == 0 {
                        break;
                    }
                    
                    stream.write_all(b"+PONG\r\n").unwrap();
                }
                
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

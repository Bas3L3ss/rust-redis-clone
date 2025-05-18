#![allow(unused_imports)]
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut writer = stream;

                    loop {
                        let mut line = String::new();
                        let bytes_read = reader.read_line(&mut line).unwrap();
                        if bytes_read == 0 {
                            break; // client disconnected
                        }

                        // You can add more intelligent checks here later
                        writer.write_all(b"+PONG\r\n").unwrap();
                        writer.flush().unwrap();
                    }
                });
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
    }
}

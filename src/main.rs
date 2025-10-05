use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use codecrafters_redis::rdb::start_up::start_up;
use codecrafters_redis::structs::connection::Connection;
use codecrafters_redis::structs::global::RedisGlobal;
use codecrafters_redis::structs::request::Request;
use codecrafters_redis::structs::runner::Runner;
use codecrafters_redis::types::{DbConfigType, DbType, RedisGlobalType};
use codecrafters_redis::utils::num_bytes;
use std::sync::{Arc, Mutex};

fn main() {
    println!("Logs from your program will appear here!");
    let start = std::time::Instant::now();

    let global_state = Arc::new(Mutex::new(RedisGlobal::init(std::env::args())));

    let port = {
        let global = global_state.lock().unwrap();
        global.port.clone()
    };

    let bind_addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(&bind_addr).expect(&format!("Failed to bind to {bind_addr}"));
    println!("Listening on {bind_addr}");

    let db = Arc::new(Mutex::new(HashMap::new()));
    let db_config = Arc::new(Mutex::new(HashMap::new()));

    start_up(
        Arc::clone(&db),
        Arc::clone(&db_config),
        Arc::clone(&global_state),
    );
    spawn_cleanup_thread(Arc::clone(&db), Arc::clone(&db_config));
    spawn_client_thread(
        Arc::clone(&db),
        Arc::clone(&db_config),
        Arc::clone(&global_state),
    );

    let duration = start.elapsed();
    eprintln!("initialization took {:?}", duration);

    listen_for_clients(listener, db, db_config, global_state);
}

pub fn spawn_client_thread(db: DbType, db_config: DbConfigType, global_state: RedisGlobalType) {
    let is_master = {
        let global_guard = global_state.lock().unwrap();
        global_guard.is_master()
    };

    if is_master {
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                let ack_message = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                let bytes = num_bytes(ack_message);

                let mut global_guard = global_state.lock().unwrap();

                for (slave_port, replica_arc) in global_guard.replica_states.iter_mut() {
                    if let Ok(mut replica) = replica_arc.lock() {
                        // Send the ACK message to the slave
                        if let Err(e) = replica.stream.write_all(ack_message.as_bytes()) {
                            eprintln!("Failed to send ACK to slave {}: {:?}", slave_port, e);
                            continue;
                        }
                        if let Err(e) = replica.stream.flush() {
                            eprintln!("Failed to flush ACK to slave {}: {:?}", slave_port, e);
                            continue;
                        }

                        let mut buf = [0u8; 1024];
                        replica
                            .stream
                            .set_read_timeout(Some(Duration::from_millis(100)))
                            .ok();
                        match replica.stream.read(&mut buf) {
                            Ok(n) if n > 0 => {
                                let resp = String::from_utf8_lossy(&buf[..n]);
                                // Check if the response is REPLCONF ACK 0
                                if resp.contains("REPLCONF") && resp.contains("ACK") {
                                    println!(
                                        "Slave {} replied with: {}",
                                        slave_port,
                                        resp.trim_end()
                                    );
                                } else {
                                    println!(
                                        "Slave {} sent unexpected reply: {}",
                                        slave_port,
                                        resp.trim_end()
                                    );
                                }
                            }
                            Ok(_) => {} // No data
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                            Err(e) => {
                                eprintln!(
                                    "Error reading response from slave {}: {:?}",
                                    slave_port, e
                                );
                            }
                        }
                        // Remove the timeout for future use
                        replica.stream.set_read_timeout(None).ok();
                    }
                }
                global_guard.offset_replica_sync += bytes;
            }
        });
    } else {
        thread::spawn(move || {
            let master_stream_arc = {
                let global_guard = global_state.lock().unwrap();
                match &global_guard.master_stream {
                    Some(stream_arc) => Arc::clone(stream_arc),
                    None => {
                        eprintln!("No master stream found; aborting replication thread");
                        return;
                    }
                }
            };

            let mut connection_info = Connection::default();
            let mut local_offset = 0;
            loop {
                let mut buffer = [0u8; 1024];

                let mut stream_guard = master_stream_arc.lock().unwrap();
                let bytes_read = {
                    match stream_guard.read(&mut buffer) {
                        Ok(0) => {
                            eprintln!("Master closed connection");
                            break;
                        }
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Read error from master: {e}");
                            break;
                        }
                    }
                };

                let request = Request::new_from_buffer(&buffer[..bytes_read]);

                let mut runner = Runner::new(request.args);

                runner.run(
                    &mut stream_guard,
                    &db,
                    &db_config,
                    &global_state,
                    &mut connection_info,
                    &local_offset,
                );

                local_offset += bytes_read;
            }

            eprintln!("Replication thread exiting; consider retrying sync with master");
        });
    }
}

fn spawn_cleanup_thread(db: DbType, db_config: DbConfigType) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));

        let mut db = db.lock().unwrap();
        let mut config = db_config.lock().unwrap();

        let expired_keys: Vec<String> = config
            .iter()
            .filter_map(|(key, cfg)| {
                if cfg.is_expired() {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in expired_keys {
            db.remove(&key);
            config.remove(&key);
            println!("Expired key removed: {}", key);
        }
    });
}

fn listen_for_clients(
    listener: TcpListener,
    db: DbType,
    db_config: DbConfigType,
    global_state: RedisGlobalType,
) {
    for stream in listener.incoming() {
        let db = Arc::clone(&db);
        let db_config = Arc::clone(&db_config);
        let global_state = Arc::clone(&global_state);
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_connection(stream, db, db_config, global_state);
                });
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    db: DbType,
    db_config: DbConfigType,
    global_state: RedisGlobalType,
) {
    let mut connection_info = Connection::default();
    loop {
        let mut buffer = [0u8; 1024];
        let bytes_read = match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                eprintln!("read error: {e}");
                break;
            }
        };

        let request = Request::new_from_buffer(&buffer[..bytes_read]);
        let mut runner = Runner::new(request.args);
        runner.run(
            &mut stream,
            &db,
            &db_config,
            &global_state,
            &mut connection_info,
            &0,
        );
    }
}

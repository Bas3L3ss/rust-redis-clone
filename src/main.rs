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
use codecrafters_redis::utils::write_array;
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
    spawn_replica_handler_thread(
        Arc::clone(&db),
        Arc::clone(&db_config),
        Arc::clone(&global_state),
    );

    let duration = start.elapsed();
    eprintln!("initialization took {:?}", duration);

    listen_for_clients(listener, db, db_config, global_state);
}

pub fn spawn_replica_handler_thread(
    db: DbType,
    db_config: DbConfigType,
    global_state: RedisGlobalType,
) {
    let is_master = {
        let global_guard = global_state.lock().unwrap();
        global_guard.is_master()
    };

    if is_master {
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(1));
            println!("### Start sending heartbeat");
            let mut global_guard = global_state.lock().unwrap();
            let master_offset = global_guard.offset_replica_sync as i64;

            for (slave_port, replica) in global_guard.replica_states.iter_mut() {
                let mut stream_guard = match replica.stream.lock() {
                    Ok(guard) => guard,
                    Err(_) => {
                        eprintln!("Failed to lock stream for replica {}", slave_port);
                        continue;
                    }
                };

                println!("Start sending heartbeat to replica with port: {slave_port}");
                write_array(
                    &mut *stream_guard,
                    &[Some("REPLCONF"), Some("GETACK"), Some("*")],
                );

                let mut buf = [0u8; 1024];
                match stream_guard.read(&mut buf) {
                    Ok(n) if n > 0 => {
                        let mut offset = 0;
                        while offset < n {
                            match Request::try_parse(&buf[offset..n]) {
                                Some((req, consumed)) => {
                                    if req.args.len() >= 3
                                        && req.args[0].eq_ignore_ascii_case("REPLCONF")
                                        && req.args[1].eq_ignore_ascii_case("ACK")
                                    {
                                        if let Ok(replica_offset) = req.args[2].parse::<i64>() {
                                            let diff = master_offset - replica_offset;
                                            if diff != 0 {
                                                eprintln!(
                                                    "replica is behind the master by {}",
                                                    diff
                                                );
                                            }
                                            replica.local_offset = replica_offset as usize;
                                        }
                                    }
                                    offset += consumed;
                                }
                                None => {
                                    break;
                                }
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => {
                        eprintln!("Error reading response from slave {}: {:?}", slave_port, e);
                    }
                }
            }
            global_guard.offset_replica_sync += 37;
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
            let mut read_buffer: Vec<u8> = Vec::new();

            loop {
                let mut temp = [0u8; 1024];
                let mut stream_guard = master_stream_arc.lock().unwrap();
                let bytes_read = match stream_guard.read(&mut temp) {
                    Ok(0) => {
                        eprintln!("Master closed connection");
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Read error from master: {e}");
                        break;
                    }
                };

                read_buffer.extend_from_slice(&temp[..bytes_read]);

                // Try to parse as many complete requests as possible
                while let Some((request, consumed)) = Request::try_parse(&read_buffer) {
                    local_offset += consumed; // ✅ advance offset only for what we parsed

                    let mut runner = Runner::new(request.args);
                    runner.run(
                        &mut stream_guard,
                        &db,
                        &db_config,
                        &global_state,
                        &mut connection_info,
                        &local_offset,
                        true,
                    );

                    // Remove the bytes we consumed from the buffer
                    read_buffer.drain(..consumed);
                }
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
    let mut local_offset = 0;
    let mut read_buffer: Vec<u8> = Vec::new();

    loop {
        if connection_info.is_slave_established {
            break;
        }
        let mut temp = [0u8; 1024];
        let bytes_read = match stream.read(&mut temp) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) => {
                eprintln!("read error from api handler: {e}");
                break;
            }
        };

        read_buffer.extend_from_slice(&temp[..bytes_read]);

        // Try to parse as many complete requests as possible
        while let Some((request, consumed)) = Request::try_parse(&read_buffer) {
            local_offset += consumed;

            let mut runner = Runner::new(request.args);
            runner.run(
                &mut stream,
                &db,
                &db_config,
                &global_state,
                &mut connection_info,
                &local_offset,
                false,
            );

            // Remove the bytes we consumed from the buffer
            read_buffer.drain(..consumed);
        }
    }
}

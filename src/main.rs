use std::collections::HashMap;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use codecrafters_redis::rdb::start_up::start_up;
use codecrafters_redis::structs::connection::Connection;
use codecrafters_redis::structs::global::RedisGlobal;
use codecrafters_redis::structs::request::Request;
use codecrafters_redis::structs::runner::Runner;
use codecrafters_redis::types::{DbConfigType, DbType, RedisGlobalType};
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

fn spawn_client_thread(db: DbType, db_config: DbConfigType, global_state: RedisGlobalType) {
    thread::spawn(move || {
        let mut global_guard = global_state.lock().unwrap();
        if global_guard.is_master() {
            return;
        }
        let master_stream = match global_guard.master_stream.take() {
            Some(stream) => stream,
            None => return,
        };
        drop(global_guard);

        handle_connection(master_stream, db, db_config, global_state);
    });
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
        );
    }
}

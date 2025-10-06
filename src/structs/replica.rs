use std::{
    io::Write,
    net::TcpStream,
    sync::{mpsc, Arc, Mutex},
    thread,
};

#[derive(Debug)]
pub struct ReplicaState {
    pub sender: mpsc::Sender<String>,
    pub stream: Arc<Mutex<TcpStream>>,
    pub local_offset: usize,
}

impl ReplicaState {
    pub fn new(stream: Arc<Mutex<TcpStream>>, sender: mpsc::Sender<String>) -> Self {
        ReplicaState {
            stream,
            sender,
            local_offset: 0,
        }
    }
}

pub fn add_replica(
    guard: &mut std::sync::MutexGuard<'_, crate::structs::global::RedisGlobal>,
    stream: TcpStream,
    replica_port: &str,
) {
    let (tx, rx) = mpsc::channel::<String>();

    let stream_arc = Arc::new(Mutex::new(stream));
    let stream_for_thread = Arc::clone(&stream_arc);

    spawn_replica_stream_sender(stream_for_thread, rx);

    guard
        .replica_states
        .insert(replica_port.to_string(), ReplicaState::new(stream_arc, tx));
}

fn spawn_replica_stream_sender(stream: Arc<Mutex<TcpStream>>, receiver: mpsc::Receiver<String>) {
    thread::spawn(move || {
        while let Ok(msg) = receiver.recv() {
            let mut stream_guard = match stream.lock() {
                Ok(guard) => guard,
                Err(_) => {
                    eprintln!("Failed to lock stream for replica");
                    break;
                }
            };
            if let Err(e) = stream_guard.write_all(msg.as_bytes()) {
                eprintln!("Failed to write to replica: {:?}", e);
                break;
            }
        }
    });
}

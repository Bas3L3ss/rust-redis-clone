use std::net::TcpStream;

#[derive(Debug)]
pub struct ReplicaState {
    pub stream: TcpStream,
}

impl ReplicaState {
    pub fn new(stream: TcpStream) -> Self {
        ReplicaState { stream }
    }
}

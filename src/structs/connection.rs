use crate::structs::transaction::Transaction;

pub struct Connection {
    pub slave_port: Option<String>,
    pub is_slave_established: bool,
    pub transaction: Transaction,
    pub channels_connected: usize,
}

impl Default for Connection {
    fn default() -> Self {
        Connection {
            slave_port: None,
            is_slave_established: false,
            transaction: Transaction::new(),
            channels_connected: 0,
        }
    }
}

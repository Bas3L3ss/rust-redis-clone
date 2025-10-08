use crate::structs::transaction::Transaction;

pub struct Connection {
    pub slave_port: Option<String>,
    pub is_slave_established: bool,
    pub transaction: Transaction,
}

impl Default for Connection {
    fn default() -> Self {
        Connection {
            slave_port: None,
            is_slave_established: false,
            transaction: Transaction::new(),
        }
    }
}

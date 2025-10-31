use std::{collections::HashMap, sync::mpsc::Receiver};

use crate::structs::transaction::Transaction;

pub struct Connection {
    pub slave_port: Option<String>,
    pub is_slave_established: bool,
    pub transaction: Transaction,
    pub subscribed_channels: HashMap<String, Receiver<String>>,
}

impl Default for Connection {
    fn default() -> Self {
        Connection {
            slave_port: None,
            is_slave_established: false,
            transaction: Transaction::new(),
            subscribed_channels: HashMap::new(),
        }
    }
}

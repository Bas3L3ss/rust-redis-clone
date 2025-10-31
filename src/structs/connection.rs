use rand::{distr::Alphanumeric, rng, Rng};
use std::{collections::HashMap, sync::mpsc::Receiver};

use crate::structs::transaction::Transaction;

pub struct Connection {
    pub id: String,
    pub slave_port: Option<String>,
    pub is_slave_established: bool,
    pub transaction: Transaction,
    pub subscribed_channels: HashMap<String, Receiver<String>>,
}

impl Default for Connection {
    fn default() -> Self {
        // Generate a random alphanumeric id of length 16 for each connection
        let id: String = rng()
            .sample_iter(&Alphanumeric)
            .take(16)
            .map(char::from)
            .collect();

        Connection {
            id,
            slave_port: None,
            is_slave_established: false,
            transaction: Transaction::new(),
            subscribed_channels: HashMap::new(),
        }
    }
}

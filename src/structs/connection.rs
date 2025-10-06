pub struct Connection {
    pub slave_port: Option<String>,
    pub is_slave_established: bool,
}

impl Default for Connection {
    fn default() -> Self {
        Connection {
            slave_port: None,
            is_slave_established: false,
        }
    }
}

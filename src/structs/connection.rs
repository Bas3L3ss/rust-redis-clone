pub struct Connection {
    pub slave_port: Option<String>,
}

impl Default for Connection {
    fn default() -> Self {
        Connection { slave_port: None }
    }
}

pub struct Transaction {
    pub is_txing: bool,
    pub tasks: Vec<String>,
    pub job_done_at: usize,
    pub response: Vec<String>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            is_txing: false,
            tasks: Vec::new(),
            job_done_at: 0,
            response: Vec::new(),
        }
    }
}

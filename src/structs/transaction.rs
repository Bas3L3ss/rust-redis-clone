pub struct Transaction {
    pub is_txing: bool,
    pub tasks: Vec<String>,
    pub job_done_at: Option<usize>,
    pub response: Vec<Option<String>>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            is_txing: false,
            tasks: Vec::new(),
            job_done_at: None,
            response: Vec::new(),
        }
    }
}

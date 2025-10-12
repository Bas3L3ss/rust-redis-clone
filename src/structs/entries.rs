pub struct Stream {
    pub entries: Vec<Entries>,
}

pub struct Entries {
    pub id: String,
}
impl Stream {
    pub fn new() -> Self {
        Stream { entries: vec![] }
    }

    pub fn add_entries(&mut self, id: String, kv: Vec<(String, String)>) {
        self.entries.push(Entries { id: id });
    }
}

impl ToString for Stream {
    fn to_string(&self) -> String {
        let entries_str: Vec<String> = self.entries.iter().map(|entry| entry.to_string()).collect();
        format!("[{}]", entries_str.join(", "))
    }
}

impl ToString for Entries {
    fn to_string(&self) -> String {
        format!("{}", self.id)
    }
}

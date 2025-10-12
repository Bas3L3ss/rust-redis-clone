pub struct Stream {
    pub entries: Vec<Entries>,
}

pub struct Entries {
    pub sequence_number: u64,
    pub milisec: u64,
}
impl Stream {
    pub fn new() -> Self {
        Stream { entries: vec![] }
    }

    pub fn add_entries(&mut self, id: String, kv: Vec<(String, String)>) -> bool {
        let mili_sequence_vec: Vec<&str> = id.split('-').collect();

        if mili_sequence_vec.len() != 2 {
            return false;
        }

        let curr_ms = mili_sequence_vec[0].parse::<u64>();
        let curr_seq = mili_sequence_vec[1].parse::<u64>();

        if curr_ms.is_err() || curr_seq.is_err() {
            return false;
        }
        let (curr_ms, curr_seq) = (curr_ms.unwrap(), curr_seq.unwrap());

        if self.entries.is_empty() {
            // Initial entry, check rules
            if curr_ms == 0 && curr_seq == 0 {
                return false;
            }
            if curr_ms == 0 && curr_seq < 1 {
                return false;
            }
        } else {
            let last_entry = self.entries.last().unwrap();
            let last_ms = last_entry.milisec;
            let last_seq = last_entry.sequence_number;

            if curr_ms < last_ms {
                return false;
            }
            if curr_ms == last_ms && curr_seq <= last_seq {
                return false;
            }
        }

        self.entries.push(Entries {
            sequence_number: curr_seq,
            milisec: curr_ms,
        });
        true
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
        format!("{}-{}", self.milisec, self.sequence_number)
    }
}

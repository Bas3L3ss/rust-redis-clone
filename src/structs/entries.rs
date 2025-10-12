use crate::enums::add_stream_entries_result::StreamResult;

pub struct Stream {
    pub entries: Vec<Entries>,
}

pub struct Entries {
    pub milisec: u64,
    pub sequence_number: u64,
}
impl Stream {
    pub fn new() -> Self {
        Stream { entries: vec![] }
    }

    pub fn add_entries(&mut self, id: String, kv: Vec<(String, String)>) -> StreamResult {
        let mili_sequence_vec: Vec<&str> = id.split('-').collect();

        if mili_sequence_vec.len() != 2 {
            return StreamResult::Err("The ID specified in XADD is not valid".to_string());
        }

        if mili_sequence_vec[1] == "*" {
            let curr_ms = mili_sequence_vec[0].parse::<u64>();

            if curr_ms.is_err() {
                return StreamResult::Err("The ID specified in XADD is not valid".to_string());
            }
            let curr_ms = curr_ms.unwrap();
            let mut curr_seq = 1;

            if !self.entries.is_empty() {
                let last_entry = self.entries.last().unwrap();
                let last_ms = last_entry.milisec;
                curr_seq = curr_seq + last_entry.sequence_number;

                if curr_ms < last_ms {
                    return StreamResult::Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                );
                }
            }

            self.entries.push(Entries {
                milisec: curr_ms,
                sequence_number: curr_seq,
            });

            StreamResult::Some(format!("{curr_ms}-{curr_seq}"))
        } else {
            let curr_ms = mili_sequence_vec[0].parse::<u64>();
            let curr_seq = mili_sequence_vec[1].parse::<u64>();

            if curr_ms.is_err() || curr_seq.is_err() {
                return StreamResult::Err("The ID specified in XADD is not valid".to_string());
            }
            let (curr_ms, curr_seq) = (curr_ms.unwrap(), curr_seq.unwrap());

            if curr_ms == 0 && curr_seq == 0 {
                return StreamResult::Err(
                    "The ID specified in XADD must be greater than 0-0".to_string(),
                );
            }
            if curr_ms == 0 && curr_seq < 1 {
                return StreamResult::Err(
                    "The ID specified in XADD must be greater than 0-0".to_string(),
                );
            }
            if !self.entries.is_empty() {
                let last_entry = self.entries.last().unwrap();
                let last_ms = last_entry.milisec;
                let last_seq = last_entry.sequence_number;

                if curr_ms < last_ms {
                    return StreamResult::Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                );
                }
                if curr_ms == last_ms && curr_seq <= last_seq {
                    return StreamResult::Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                );
                }
            }
            self.entries.push(Entries {
                milisec: curr_ms,
                sequence_number: curr_seq,
            });

            StreamResult::Some(id)
        }
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

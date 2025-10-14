use crate::enums::add_stream_entries_result::StreamResult;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct Stream {
    pub entries: Vec<Entry>,
}

#[derive(Debug)]
pub struct Entry {
    pub milisec: u64,
    pub sequence_number: u64,
    pub key_val: Vec<(String, String)>,
}
impl Stream {
    pub fn new() -> Self {
        Stream { entries: vec![] }
    }

    pub fn range(&self, start: (u64, u64), end: (u64, u64)) -> Vec<&Entry> {
        let start_idx = self
            .entries
            .binary_search_by(|e| (e.milisec, e.sequence_number).cmp(&start))
            .unwrap_or_else(|x| x);

        let end_idx = self
            .entries
            .binary_search_by(|e| (e.milisec, e.sequence_number).cmp(&end))
            .unwrap_or_else(|x| x);

        self.entries[start_idx..=end_idx].iter().collect()
    }

    pub fn range_start(&self, start: (u64, u64)) -> Vec<&Entry> {
        let start_idx = self
            .entries
            .binary_search_by(|e| (e.milisec, e.sequence_number).cmp(&start))
            .unwrap_or_else(|x| x);

        self.entries[start_idx..].iter().collect()
    }

    pub fn add_entries(&mut self, id: String, key_val: Vec<(String, String)>) -> StreamResult {
        if id == "*" {
            let curr_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let mut curr_seq = 0;
            if let Some(last_entry) = self.entries.last() {
                if last_entry.milisec == curr_ms {
                    curr_seq = last_entry.sequence_number + 1;
                }
            }

            self.entries.push(Entry {
                milisec: curr_ms,
                sequence_number: curr_seq,
                key_val,
            });

            return StreamResult::Some(format!("{curr_ms}-{curr_seq}"));
        }

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
                if curr_ms == last_ms {
                    curr_seq = curr_seq + last_entry.sequence_number;
                } else {
                    curr_seq = 0;
                }

                if curr_ms < last_ms {
                    return StreamResult::Err(
                    "The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_string(),
                );
                }
            }

            self.entries.push(Entry {
                milisec: curr_ms,
                sequence_number: curr_seq,
                key_val,
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
            self.entries.push(Entry {
                milisec: curr_ms,
                sequence_number: curr_seq,
                key_val,
            });

            StreamResult::Some(id)
        }
    }

    pub fn last_entry_id(&self) -> Option<(u64, u64)> {
        if let Some(entry) = self.entries.last() {
            Some((entry.milisec, entry.sequence_number))
        } else {
            Some((0, 0))
        }
    }
}

impl ToString for Stream {
    fn to_string(&self) -> String {
        let entries_str: Vec<String> = self.entries.iter().map(|entry| entry.to_string()).collect();
        format!("[{}]", entries_str.join(", "))
    }
}

impl ToString for Entry {
    fn to_string(&self) -> String {
        let mut keyvals = Vec::new();
        for (k, v) in &self.key_val {
            keyvals.push(k.clone());
            keyvals.push(v.clone());
        }
        format!(
            "{}-{} [{}]",
            self.milisec,
            self.sequence_number,
            keyvals.join(", ")
        )
    }
}

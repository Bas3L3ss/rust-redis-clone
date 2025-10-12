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

    pub fn add_entries(&mut self, id: String, key_val: Vec<(String, String)>) -> StreamResult {
        const ERR_INVALID_ID: &str = "The ID specified in XADD is not valid";
        const ERR_NOT_GREATER: &str =
            "The ID specified in XADD is equal or smaller than the target stream top item";
        const ERR_MUST_GT_00: &str = "The ID specified in XADD must be greater than 0-0";

        let id_is_star = id == "*";
        let mut _ms = 0u64;
        let mut seq = 0u64;
        let mut gen_seq = false;

        if id_is_star {
            _ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            if let Some(last) = self.entries.last() {
                seq = if last.milisec == _ms {
                    last.sequence_number + 1
                } else {
                    0
                };
            }
            self.entries.push(Entry {
                milisec: _ms,
                sequence_number: seq,
                key_val,
            });
            return StreamResult::Some(format!("{_ms}-{seq}"));
        }

        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return StreamResult::Err(ERR_INVALID_ID.to_string());
        }

        let ms_parsed = parts[0].parse::<u64>();
        if ms_parsed.is_err() {
            return StreamResult::Err(ERR_INVALID_ID.to_string());
        }
        _ms = ms_parsed.unwrap();

        if parts[1] == "*" {
            gen_seq = true;
        } else {
            let seq_parsed = parts[1].parse::<u64>();
            if seq_parsed.is_err() {
                return StreamResult::Err(ERR_INVALID_ID.to_string());
            }
            seq = seq_parsed.unwrap();
        }

        if !gen_seq {
            if _ms == 0 && seq == 0 {
                return StreamResult::Err(ERR_MUST_GT_00.to_string());
            }
            if _ms == 0 && seq < 1 {
                return StreamResult::Err(ERR_MUST_GT_00.to_string());
            }
        }

        if let Some(last) = self.entries.last() {
            if _ms < last.milisec {
                return StreamResult::Err(ERR_NOT_GREATER.to_string());
            }
            if _ms == last.milisec {
                if gen_seq {
                    seq = last.sequence_number + 1;
                }
                if !gen_seq && seq <= last.sequence_number {
                    return StreamResult::Err(ERR_NOT_GREATER.to_string());
                }
            } else if gen_seq {
                seq = 0;
            }
        } else if gen_seq {
            seq = 0;
        }

        self.entries.push(Entry {
            milisec: _ms,
            sequence_number: seq,
            key_val,
        });

        if gen_seq {
            StreamResult::Some(format!("{_ms}-{seq}"))
        } else {
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

impl ToString for Entry {
    fn to_string(&self) -> String {
        format!("{}-{}", self.milisec, self.sequence_number)
    }
}

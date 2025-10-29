use std::collections::HashMap;

use crate::structs::skiplist::{self, SkipList};

pub struct ZSet {
    dict: HashMap<String, f64>,
    skiplist: SkipList,
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            dict: HashMap::new(),
            skiplist: SkipList::new(),
        }
    }

    pub fn zadd(&mut self, score: f64, member: String) -> i64 {
        if self.skiplist.add((score, member), &mut self.dict) {
            1
        } else {
            0
        }
    }

    pub fn zscore(&self, member: &String) -> Option<&f64> {
        self.dict.get(member)
    }

    pub fn zrank(&self, member: &String) -> Option<u64> {
        if let Some(score) = self.dict.get(member) {
            self.skiplist.rank(score, member)
        } else {
            None
        }
    }

    pub fn zrange(&self, start: i64, end: i64) -> Vec<(f64, String)> {
        self.skiplist.range(start, end)
    }

    pub fn zcard(&self) -> usize {
        self.dict.len()
    }

    pub fn zrem(&mut self, member: &String) -> u32 {
        if let Some(score) = self.dict.get(member) {
            if self.skiplist.remove_entry(score, member) {
                self.dict.remove(member);
                1
            } else {
                0
            }
        } else {
            0
        }
    }
}

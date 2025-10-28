use std::collections::HashMap;

use crate::structs::skiplist::SkipList;

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

    pub fn zadd(&mut self, score: f64, member: String) {
        self.skiplist.add((score, member), &mut self.dict);
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    /// Check if ZSet is
    pub fn is_empty(&self) -> bool {
        self.dict.is_empty()
    }
}

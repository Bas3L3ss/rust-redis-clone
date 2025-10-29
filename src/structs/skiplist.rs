use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const MAX_LEVEL: usize = 32;

type NodeType = Arc<RwLock<Node>>;

fn is_head() -> bool {
    use rand::Rng;
    let mut rng = rand::rng();
    rng.random_bool(0.5)
}

#[derive(Debug)]
pub struct Node {
    pub member: String,
    is_special: bool,
    score: f64,
    forwards: Vec<Option<NodeType>>,
}

impl Node {
    fn new(entry: (f64, String)) -> NodeType {
        return Arc::new(RwLock::new(Self {
            member: entry.1,
            score: entry.0,
            is_special: false,
            forwards: vec![None; MAX_LEVEL],
        }));
    }
    fn new_dummy() -> NodeType {
        return Arc::new(RwLock::new(Self {
            member: String::new(),
            score: 0.0,
            forwards: vec![None; MAX_LEVEL],
            is_special: true,
        }));
    }
}
#[derive(Debug)]
pub struct SkipList {
    head: NodeType,
    level: usize,
}

fn cmp(a_score: f64, a_member: &str, b_score: f64, b_member: &str) -> Ordering {
    match a_score.partial_cmp(&b_score).unwrap() {
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
        Ordering::Equal => a_member.cmp(b_member),
    }
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            head: Node::new_dummy(),
            level: 0,
        }
    }

    pub fn add(&mut self, entry: (f64, String), member_dict: &mut HashMap<String, f64>) -> bool {
        let (score, member) = entry.clone();
        let mut is_new = true;

        if let Some(s) = member_dict.get(&member) {
            self.remove_entry(&s, &member);
            is_new = false;
        }

        member_dict.insert(member.clone(), score);

        let mut update: Vec<NodeType> = vec![Arc::clone(&self.head); MAX_LEVEL];
        let mut cur = Arc::clone(&self.head);

        for top_to_bottom_lvl in (0..=self.level).rev() {
            loop {
                let next_opt = cur.read().unwrap().forwards[top_to_bottom_lvl]
                    .as_ref()
                    .map(Arc::clone);

                match next_opt {
                    Some(next) => {
                        let next_ref = next.read().unwrap();
                        match cmp(next_ref.score, &next_ref.member, score, &member) {
                            Ordering::Less => {
                                cur = Arc::clone(&next);
                            }
                            _ => break,
                        }
                    }
                    None => break,
                }
            }
            update[top_to_bottom_lvl] = Arc::clone(&cur);
        }

        let highest_level = SkipList::rand_number();
        if highest_level > self.level {
            for i in self.level + 1..=highest_level {
                update[i] = Arc::clone(&self.head);
            }
            self.level = highest_level;
        }

        let new_node = Node::new(entry);

        for bottom_to_top_lvl in 0..=highest_level {
            let next_opt = update[bottom_to_top_lvl].read().unwrap().forwards[bottom_to_top_lvl]
                .as_ref()
                .map(Arc::clone);
            new_node.write().unwrap().forwards[bottom_to_top_lvl] = next_opt;
            update[bottom_to_top_lvl].write().unwrap().forwards[bottom_to_top_lvl] =
                Some(Arc::clone(&new_node));
        }
        is_new
    }

    fn rand_number() -> usize {
        let mut lvl = 0;

        while is_head() && lvl < MAX_LEVEL {
            lvl += 1;
        }

        lvl
    }

    pub fn search_entry(&self, score: &f64, member: &str) -> Option<NodeType> {
        let mut cur = Arc::clone(&self.head);

        for lvl in (0..=self.level).rev() {
            loop {
                let next_opt = cur.read().unwrap().forwards[lvl].as_ref().map(Arc::clone);
                match next_opt {
                    Some(next) => {
                        let nb = next.read().unwrap();
                        match cmp(nb.score, &nb.member, *score, member) {
                            Ordering::Less => cur = Arc::clone(&next),
                            Ordering::Equal => return Some(Arc::clone(&next)),
                            Ordering::Greater => break,
                        }
                    }
                    None => break,
                }
            }
        }

        let next_opt = cur.read().unwrap().forwards[0].as_ref().map(Arc::clone);
        if let Some(next) = next_opt {
            let nb = next.read().unwrap();
            if nb.score == *score && nb.member == member {
                return Some(Arc::clone(&next));
            }
        }

        None
    }

    pub fn remove_entry(&mut self, score: &f64, member: &str) -> bool {
        let mut update: Vec<Option<NodeType>> = vec![None; MAX_LEVEL];
        let mut cur = Arc::clone(&self.head);
        let mut is_removed = false;

        for top_to_bottom_lvl in (0..=self.level).rev() {
            let mut is_found = false;
            loop {
                let next_opt = cur.read().unwrap().forwards[top_to_bottom_lvl]
                    .as_ref()
                    .map(Arc::clone);
                match next_opt {
                    Some(next) => {
                        let next_ref = next.read().unwrap();
                        match cmp(next_ref.score, &next_ref.member, *score, member) {
                            Ordering::Less => cur = Arc::clone(&next),
                            Ordering::Equal => {
                                is_found = true;
                                break;
                            }
                            _ => break,
                        }
                    }
                    None => break,
                }
            }

            if is_found {
                update[top_to_bottom_lvl] = Some(Arc::clone(&cur));
            }
        }

        for bottom_to_top_lvl in 0..=self.level {
            if let Some(ref update_node) = update[bottom_to_top_lvl] {
                is_removed = true;
                let delete_node = update_node.read().unwrap().forwards[bottom_to_top_lvl]
                    .as_ref()
                    .map(Arc::clone)
                    .expect("This shouldn't be possible");

                let next_node = {
                    if let Some(ref next) = delete_node.read().unwrap().forwards[bottom_to_top_lvl]
                    {
                        Some(Arc::clone(next))
                    } else {
                        None
                    }
                };

                update_node.write().unwrap().forwards[bottom_to_top_lvl] = next_node;

                if bottom_to_top_lvl == self.level
                    && self.head.read().unwrap().forwards[bottom_to_top_lvl].is_none()
                {
                    self.level -= 1
                }
            }
        }

        is_removed
    }

    pub fn range(&self, start: i64, end: i64) -> Vec<(f64, String)> {
        let mut result = Vec::new();
        let mut cur = Arc::clone(&self.head);

        let mut total_len = 0;
        {
            // First, count total elements
            let mut node = Arc::clone(&self.head);
            loop {
                let next_opt = {
                    let node_ref = node.read().unwrap();
                    node_ref.forwards[0].as_ref().map(Arc::clone)
                };
                match next_opt {
                    Some(next) => {
                        total_len += 1;
                        node = next;
                    }
                    None => break,
                }
            }
        }
        let start = if start < 0 {
            (total_len as i64 + start).max(0)
        } else {
            start.min(total_len as i64)
        } as usize;
        let mut end = if end < 0 {
            (total_len as i64 + end).max(0)
        } else {
            end
        } as usize;
        if end >= total_len {
            end = if total_len == 0 { 0 } else { total_len - 1 };
        }
        if start > end || start >= total_len {
            return result;
        }

        // Move to start node
        let mut idx = 0;
        loop {
            let next_opt = {
                let cur_ref = cur.read().unwrap();
                cur_ref.forwards[0].as_ref().map(Arc::clone)
            };
            match next_opt {
                Some(next) => {
                    if idx >= start {
                        // collect until end
                        let nref = next.read().unwrap();
                        result.push((nref.score, nref.member.clone()));
                        if idx == end {
                            break;
                        }
                    }
                    cur = next;
                    idx += 1;
                }
                None => {
                    break;
                }
            }
        }
        result
    }

    pub fn rank(&self, score: &f64, member: &str) -> Option<u64> {
        let mut rank = 0;
        let mut cur = Arc::clone(&self.head);

        loop {
            let next_opt = {
                let cur_ref = cur.read().unwrap();
                cur_ref.forwards[0].as_ref().map(Arc::clone)
            };

            match next_opt {
                Some(next) => {
                    let next_ref = next.read().unwrap();
                    if next_ref.score == *score && next_ref.member == member {
                        return Some(rank);
                    }
                    cur = Arc::clone(&next);
                    rank += 1;
                }
                None => return None,
            }
        }
    }
}

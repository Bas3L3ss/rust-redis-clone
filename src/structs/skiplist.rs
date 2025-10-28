use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

const MAX_LEVEL: usize = 32;

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
    forwards: Vec<Option<Rc<RefCell<Node>>>>,
}

impl Node {
    fn new(entry: (f64, String)) -> Rc<RefCell<Node>> {
        return Rc::new(RefCell::new(Self {
            member: entry.1,
            score: entry.0,
            is_special: false,
            forwards: vec![None; MAX_LEVEL],
        }));
    }
    fn new_dummy() -> Rc<RefCell<Node>> {
        return Rc::new(RefCell::new(Self {
            member: String::new(),
            score: 0.0,
            forwards: vec![None; MAX_LEVEL],
            is_special: true,
        }));
    }
}
#[derive(Debug)]
pub struct SkipList {
    head: Rc<RefCell<Node>>,
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

    pub fn add(&mut self, entry: (f64, String), member_dict: &mut HashMap<String, f64>) {
        let (score, member) = entry.clone();

        if let Some(s) = member_dict.get(&member) {
            self.remove_entry(&s, &member);
        }

        member_dict.insert(member.clone(), score);

        let mut update: Vec<Rc<RefCell<Node>>> = vec![Rc::clone(&self.head); MAX_LEVEL];
        let mut cur = Rc::clone(&self.head);

        for top_to_bottom_lvl in (0..=self.level).rev() {
            loop {
                let next_opt = cur.borrow().forwards[top_to_bottom_lvl]
                    .as_ref()
                    .map(Rc::clone);

                match next_opt {
                    Some(next) => {
                        let next_ref = next.borrow();
                        match cmp(next_ref.score, &next_ref.member, score, &member) {
                            Ordering::Less => {
                                cur = Rc::clone(&next);
                            }
                            _ => break,
                        }
                    }
                    None => break,
                }
            }
            update[top_to_bottom_lvl] = Rc::clone(&cur);
        }

        let highest_level = SkipList::rand_number();
        if highest_level > self.level {
            for i in self.level + 1..=highest_level {
                update[i] = Rc::clone(&self.head);
            }
            self.level = highest_level;
        }

        let new_node = Node::new(entry);

        for bottom_to_top_lvl in 0..=highest_level {
            let next_opt = update[bottom_to_top_lvl].borrow().forwards[bottom_to_top_lvl]
                .as_ref()
                .map(Rc::clone);
            new_node.borrow_mut().forwards[bottom_to_top_lvl] = next_opt;
            update[bottom_to_top_lvl].borrow_mut().forwards[bottom_to_top_lvl] =
                Some(Rc::clone(&new_node));
        }
    }

    fn rand_number() -> usize {
        let mut lvl = 0;

        while is_head() && lvl < MAX_LEVEL {
            lvl += 1;
        }

        lvl
    }

    pub fn print_all(&self) {
        println!("Skip List:");
        for lvl in (0..=self.level).rev() {
            print!("LEVEL {lvl}: [ ");
            let mut cur = Rc::clone(&self.head);
            while let Some(node) = {
                let cur_ref = cur.borrow();
                cur_ref.forwards[lvl].as_ref().map(Rc::clone)
            } {
                print!("{}, ", node.borrow().member);
                cur = node;
            }
            println!("]");
        }
    }

    pub fn search_entry(&self, score: &f64, member: &str) -> Option<Rc<RefCell<Node>>> {
        let mut cur = Rc::clone(&self.head);

        for lvl in (0..=self.level).rev() {
            loop {
                let next_opt = cur.borrow().forwards[lvl].as_ref().map(Rc::clone);
                match next_opt {
                    Some(next) => {
                        let nb = next.borrow();
                        match cmp(nb.score, &nb.member, *score, member) {
                            Ordering::Less => cur = Rc::clone(&next),
                            Ordering::Equal => return Some(Rc::clone(&next)),
                            Ordering::Greater => break,
                        }
                    }
                    None => break,
                }
            }
        }

        let next_opt = cur.borrow().forwards[0].as_ref().map(Rc::clone);
        if let Some(next) = next_opt {
            let nb = next.borrow();
            if nb.score == *score && nb.member == member {
                return Some(Rc::clone(&next));
            }
        }

        None
    }

    pub fn remove_entry(&mut self, score: &f64, member: &str) -> bool {
        let mut update: Vec<Option<Rc<RefCell<Node>>>> = vec![None; MAX_LEVEL];
        let mut cur = Rc::clone(&self.head);
        let mut is_removed = false;

        for top_to_bottom_lvl in (0..=self.level).rev() {
            let mut is_found = false;
            loop {
                let next_opt = cur.borrow().forwards[top_to_bottom_lvl]
                    .as_ref()
                    .map(Rc::clone);
                match next_opt {
                    Some(next) => {
                        let next_ref = next.borrow();
                        match cmp(next_ref.score, &next_ref.member, *score, member) {
                            Ordering::Less => cur = Rc::clone(&next),
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
                update[top_to_bottom_lvl] = Some(Rc::clone(&cur));
            }
        }

        for bottom_to_top_lvl in 0..=self.level {
            if let Some(ref update_node) = update[bottom_to_top_lvl] {
                is_removed = true;
                let delete_node = update_node.borrow().forwards[bottom_to_top_lvl]
                    .as_ref()
                    .map(Rc::clone)
                    .expect("This shouldn't be possible");

                let next_node = {
                    if let Some(ref next) = delete_node.borrow().forwards[bottom_to_top_lvl] {
                        Some(Rc::clone(next))
                    } else {
                        None
                    }
                };

                update_node.borrow_mut().forwards[bottom_to_top_lvl] = next_node;

                if bottom_to_top_lvl == self.level
                    && self.head.borrow().forwards[bottom_to_top_lvl].is_none()
                {
                    self.level -= 1
                }
            }
        }

        is_removed
    }

    pub fn range(&self, start: f64, end: f64) -> Vec<Rc<RefCell<Node>>> {
        // TODO: handle duplicates
        if end < start {
            panic!("Invalid range");
        }

        let mut cur = Rc::clone(&self.head);

        for lvl in (0..=self.level).rev() {
            loop {
                let next_opt = cur.borrow().forwards[lvl].as_ref().map(Rc::clone);
                match next_opt {
                    Some(next) if next.borrow().score < start => cur = next,
                    _ => break,
                }
            }
        }

        let mut cur_opt = cur.borrow().forwards[0].as_ref().map(Rc::clone);
        let mut node_vec = vec![];

        while let Some(node) = cur_opt {
            let score = node.borrow().score;
            if score > end {
                break;
            }
            if !node.borrow().is_special {
                node_vec.push(Rc::clone(&node));
            }
            cur_opt = node.borrow().forwards[0].as_ref().map(Rc::clone);
        }

        node_vec
    }
}

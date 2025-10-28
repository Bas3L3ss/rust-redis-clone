use std::collections::HashMap;

use crate::structs::{stream::Stream, zset::ZSet};

pub enum ValueType {
    String(String),
    Stream(Stream),
    List(Vec<String>),
    ZSet(ZSet),
    Set(Vec<ValueType>),
    Hash(HashMap<String, ValueType>),
    VectorSet(Vec<Vec<f32>>), // For future AI/vector search support
}

impl ValueType {
    pub fn type_name(&self) -> &'static str {
        match self {
            ValueType::String(_) => "string",
            ValueType::List(_) => "list",
            ValueType::Set(_) => "set",
            ValueType::ZSet(_) => "zset",
            ValueType::Hash(_) => "hash",
            ValueType::Stream(_) => "stream",
            ValueType::VectorSet(_) => "vectorset",
        }
    }
}

impl ToString for ValueType {
    fn to_string(&self) -> String {
        match self {
            ValueType::String(s) => s.clone(),
            ValueType::List(list) => {
                let items: Vec<String> = list.iter().map(|v| v.to_string()).collect();
                format!("[{}]", items.join(", "))
            }
            ValueType::Set(set) => {
                let items: Vec<String> = set.iter().map(|v| v.to_string()).collect();
                format!("{{{}}}", items.join(", "))
            }
            ValueType::ZSet(zset) => {
                // TODO: FINISH TOSTRING FOR THIS
                format!("")
            }
            ValueType::Hash(hash) => {
                let items: Vec<String> = hash
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, v.to_string()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
            ValueType::Stream(stream) => stream.to_string(),
            ValueType::VectorSet(vectors) => {
                let items: Vec<String> = vectors
                    .iter()
                    .map(|vec| {
                        let nums: Vec<String> = vec.iter().map(|f| f.to_string()).collect();
                        format!("[{}]", nums.join(", "))
                    })
                    .collect();
                format!("[{}]", items.join(", "))
            }
        }
    }
}

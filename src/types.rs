use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    enums::val_type::ValueType,
    structs::{config::Config, global::RedisGlobal},
};

pub type DbType = Arc<Mutex<HashMap<String, ValueType>>>;
pub type RedisGlobalType = Arc<Mutex<RedisGlobal>>;
pub type DbConfigType = Arc<Mutex<HashMap<String, Config>>>;

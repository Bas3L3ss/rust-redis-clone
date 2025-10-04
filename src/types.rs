use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::structs::{config::Config, global::RedisGlobal};

pub type DbType = Arc<Mutex<HashMap<String, String>>>;
pub type RedisGlobalType = Arc<Mutex<RedisGlobal>>;
pub type DbConfigType = Arc<Mutex<HashMap<String, Config>>>;

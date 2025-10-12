use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    enums::{transaction_result::TransactionResult, val_type::ValueType},
    structs::{config::Config, connection::Connection, transaction::Transaction},
    types::{DbConfigType, DbType, RedisGlobalType},
    utils::{is_matched, propagate_slaves},
};

pub struct TransactionRunner<'a> {
    transaction: &'a mut Transaction,
}

impl<'a> TransactionRunner<'a> {
    pub fn new(connection: &'a mut Connection) -> Self {
        TransactionRunner {
            transaction: &mut connection.transaction,
        }
    }

    pub fn execute_transactions(
        &mut self,
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
    ) {
        let tasks = self.transaction.tasks.clone();
        for (idx, task) in tasks.iter().enumerate() {
            let args: Vec<String> = task.split_whitespace().map(|s| s.to_string()).collect();
            let res = self.exec(db, db_config, global_state, args);

            let re = match res {
                TransactionResult::Some(s) => s,
                TransactionResult::Err(err) => err,
            };

            self.transaction.response.push(Some(re));

            self.transaction.job_done_at = Some(idx);
        }
    }

    pub fn exec(
        &mut self,
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        args: Vec<String>,
    ) -> TransactionResult {
        if args.is_empty() {
            return self.none();
        }

        let command = args[0].to_ascii_lowercase();
        let args = &args[1..];

        eprintln!("Transaction execute command: {:?}", command);

        match command.as_str() {
            "ping" => self.handle_ping(),
            "echo" => self.handle_echo(args),
            "set" => self.handle_set(args, db, db_config, global_state),
            "get" => self.handle_get(args, db, db_config),
            "del" => self.handle_del(args, db, db_config, global_state),
            "incr" => self.handle_incr(args, db, db_config, global_state),
            "config" => self.handle_config(args, global_state),
            "keys" => self.handle_keys(args, db, db_config),
            "info" => self.handle_info(args, db, db_config, global_state),

            "command" | "docs" => {
                return self.string(&"Ok".to_string());
            }

            _ => {
                return self.none();
            }
        }
    }

    fn handle_info(
        &self,
        _args: &[String],
        _db: &DbType,
        _db_config: &DbConfigType,
        global_state: &RedisGlobalType,
    ) -> TransactionResult {
        let global = global_state.lock().unwrap();
        let role = if global.is_master() {
            "master"
        } else {
            "slave"
        };

        let mut info = format!("role:{}", role);

        if role == "master" {
            info.push_str(&format!("\nmaster_replid:{}", global.master_replid));
            info.push_str(&format!(
                "\nmaster_repl_offset:{}",
                global.master_repl_offset
            ));
        }

        self.bulk_string(&info)
    }

    fn handle_keys(
        &self,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
    ) -> TransactionResult {
        if args.len() == 1 {
            let mut db_config = db_config.lock().unwrap();
            let mut db = db.lock().unwrap();

            let expired_keys: Vec<String> = db_config
                .iter()
                .filter_map(|(key, config)| {
                    if !is_matched(&args[0], key) {
                        return None;
                    }

                    if config.is_expired() {
                        Some(key.clone())
                    } else {
                        None
                    }
                })
                .collect();

            for key in &expired_keys {
                db_config.remove(key.as_str());
                db.remove(key.as_str());
            }

            let valid_keys: Vec<String> = db_config
                .keys()
                .filter(|key| is_matched(&args[0], key))
                .cloned()
                .collect();

            self.array(valid_keys)
        } else {
            self.array(vec![])
        }
    }

    fn handle_ping(&self) -> TransactionResult {
        return self.string(&"PONG".to_string());
    }

    fn handle_echo(&self, args: &[String]) -> TransactionResult {
        if let Some(msg) = args.get(0) {
            return self.string(msg);
        } else {
            return self.string(&format!(""));
        }
    }

    fn handle_config(&self, args: &[String], global_state: &RedisGlobalType) -> TransactionResult {
        if args.len() >= 2 && args[0].to_ascii_lowercase() == "get" {
            let config_key = args[1].to_ascii_lowercase();

            match config_key.as_str() {
                "dir" => {
                    let global = global_state.lock().unwrap();
                    self.array(vec!["dir".to_string(), global.dir_path.clone()])
                }
                "dbfilename" => {
                    let global = global_state.lock().unwrap();
                    self.array(vec!["dbfilename".to_string(), global.dbfilename.clone()])
                }
                _ => self.array(vec![String::new()]),
            }
        } else {
            return self.err("invalid CONFIG argument");
        }
    }

    fn handle_get(
        &self,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
    ) -> TransactionResult {
        if args.len() < 1 {
            return self.err("invalid GET argument");
        }

        let key = &args[0];

        let mut config_map = db_config.lock().unwrap();
        let expired = if let Some(config) = config_map.get(key) {
            config.is_expired()
        } else {
            false
        };
        if expired {
            config_map.remove(key);
            let mut map = db.lock().unwrap();
            map.remove(key);
            return self.none();
        } else {
            drop(config_map);

            let map = db.lock().unwrap();
            if let Some(val) = map.get(key) {
                return self.string(&val.to_string());
            } else {
                return self.none();
            }
        }
    }

    fn handle_set(
        &self,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
    ) -> TransactionResult {
        if args.len() < 2 {
            return self.err("invalid SET argument");
        }

        let key = args[0].clone();
        let value = args[1].clone();

        let mut config: Config = Default::default();

        let mut idx = 2;
        let mut ex_arg: Option<String> = None;
        let mut px_arg: Option<String> = None;

        while idx < args.len() {
            let opt = args[idx].to_ascii_lowercase();
            match opt.as_str() {
                "ex" => {
                    if let Some(sec_str) = args.get(idx + 1) {
                        if let Ok(secs) = sec_str.parse::<u64>() {
                            let now_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            let expire_at = now_ms + (secs as u64) * 1000;
                            config.expire_at = Some(expire_at);
                            ex_arg = Some(sec_str.clone());
                        } else {
                            return self.err("invalid EX argument");
                        }
                        idx += 2;
                    } else {
                        return self.err("invalid EX argument");
                    }
                }
                "px" => {
                    if let Some(ms_str) = args.get(idx + 1) {
                        if let Ok(ms) = ms_str.parse::<u64>() {
                            let now_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64;
                            let expire_at = now_ms + (ms as u64);
                            config.expire_at = Some(expire_at);
                            px_arg = Some(ms_str.clone());
                        } else {
                            return self.err("invalid EX argument");
                        }
                        idx += 2;
                    } else {
                        return self.err("invalid EX argument");
                    }
                }
                _ => {
                    break;
                }
            }
        }

        {
            let mut map = db.lock().unwrap();
            map.insert(key.clone(), ValueType::String(value.clone()));
        }
        {
            let mut config_map = db_config.lock().unwrap();
            config_map.insert(key.clone(), config);
        }

        // Propagate to slaves, with correct SET form
        let propagation = if let Some(ex) = ex_arg {
            format!(
                "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nEX\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value,
                ex.len(),
                ex
            )
        } else if let Some(px) = px_arg {
            format!(
                "*5\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n$2\r\nPX\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value,
                px.len(),
                px
            )
        } else {
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
        };
        propagate_slaves(global_state, &propagation);

        return self.string(&"OK".to_string());
    }

    fn handle_del(
        &self,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
    ) -> TransactionResult {
        if args.is_empty() {
            return self.err("invalid DEL argument");
        }

        let key = &args[0];
        let mut removed = 0;
        {
            let mut map = db.lock().unwrap();
            let mut config_map = db_config.lock().unwrap();
            if map.remove(key).is_some() {
                removed += 1;
            }
            config_map.remove(key);
        }
        propagate_slaves(
            global_state,
            &format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key),
        );

        return self.integer(&removed.to_string());
    }

    fn handle_incr(
        &self,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
    ) -> TransactionResult {
        use crate::enums::val_type::ValueType;

        if args.is_empty() {
            return self.err("invalid INCR argument");
        }

        let key = &args[0];
        let mut _result_value = 0;

        {
            let mut map = db.lock().unwrap();
            let mut config_map = db_config.lock().unwrap();

            if !config_map.contains_key(key) || !map.contains_key(key) {
                map.insert(key.clone(), ValueType::String("1".to_string()));
                config_map.insert(key.clone(), Default::default());
                _result_value = 1;
            } else {
                if let Some(cfg) = config_map.get(key) {
                    if cfg.is_expired() {
                        map.remove(key);
                        config_map.remove(key);
                        return self.err(&format!("key {key} is expired"));
                    }
                }
                let value = map.get(key).unwrap();
                let parsed = match value {
                    ValueType::String(s) => s.parse::<i64>(),
                    _ => {
                        return self.err("value is not an integer or out of range");
                    }
                };
                let new_value = match parsed {
                    Ok(val) => val + 1,
                    Err(_) => {
                        return self.err("value is not an integer or out of range");
                    }
                };
                map.insert(key.clone(), ValueType::String(new_value.to_string()));
                _result_value = new_value;
            }
        }

        propagate_slaves(
            global_state,
            &format!("*2\r\n$3\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key),
        );

        self.integer(&_result_value.to_string())
    }

    fn err(&self, message: &str) -> TransactionResult {
        TransactionResult::Some(format!("-ERR {}\r\n", message))
    }

    fn string(&self, message: &String) -> TransactionResult {
        // RESP Simple String: +message\r\n
        TransactionResult::Some(format!("+{}\r\n", message))
    }

    fn bulk_string(&self, message: &String) -> TransactionResult {
        if message.is_empty() {
            TransactionResult::Some("$-1\r\n".to_string())
        } else {
            TransactionResult::Some(format!("${}\r\n{}\r\n", message.len(), message))
        }
    }

    fn array(&self, messages: Vec<String>) -> TransactionResult {
        let mut resp = format!("*{}\r\n", messages.len());
        for msg in messages {
            resp.push_str(&format!("${}\r\n{}\r\n", msg.len(), msg));
        }
        TransactionResult::Some(resp)
    }

    fn none(&self) -> TransactionResult {
        // RESP Null Bulk String
        TransactionResult::Some("$-1\r\n".to_string())
    }

    fn integer(&self, message: &String) -> TransactionResult {
        // RESP Integer: :n\r\n
        match message.parse::<i64>() {
            Ok(n) => TransactionResult::Some(format!(":{}\r\n", n)),
            Err(_) => TransactionResult::Err("ERR value is not an integer".to_string()),
        }
    }
}

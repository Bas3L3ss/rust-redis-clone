use crate::enums::val_type::ValueType;
use crate::structs::config::Config;
use crate::structs::connection::Connection;
use crate::structs::replica::add_replica;
use crate::structs::transaction_runner::TransactionRunner;
use crate::types::{DbConfigType, DbType, RedisGlobalType};
use crate::utils::{
    is_matched, propagate_slaves, write_array, write_bulk_string, write_error, write_integer,
    write_null_bulk_string, write_redis_file, write_resp_array, write_simple_string,
};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub struct Runner {
    pub args: Vec<String>,
    pub cur_step: usize,
}

impl Runner {
    pub fn new(args: Vec<String>) -> Self {
        Runner { args, cur_step: 0 }
    }

    pub fn run(
        &mut self,
        stream: &mut TcpStream,
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        connection: &mut Connection,
        local_offset: &usize,
        is_propagation: bool,
    ) {
        while self.cur_step < self.args.len() {
            self.step(
                stream,
                db,
                db_config,
                global_state,
                connection,
                local_offset,
                is_propagation,
            );
            self.cur_step += 1;
        }
    }

    pub fn step(
        &mut self,
        stream: &mut TcpStream,
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        connection: &mut Connection,
        local_offset: &usize,
        is_propagation: bool,
    ) {
        if self.args.is_empty() {
            write_error(stream, "empty command");
            self.cur_step += 1;
            return;
        }

        let command = self.args[self.cur_step].to_ascii_lowercase();
        let args = &self.args[self.cur_step + 1..];

        eprintln!("Received command: {:?}", command);

        match command.as_str() {
            "ping" => {
                self.handle_ping(stream, connection);
            }
            "echo" => {
                self.cur_step += self.handle_echo(stream, args, connection);
            }
            "set" => {
                self.cur_step += self.handle_set(
                    stream,
                    args,
                    db,
                    db_config,
                    global_state,
                    &is_propagation,
                    connection,
                );
            }
            "get" => {
                self.cur_step += self.handle_get(stream, args, db, db_config, connection);
            }
            "del" => {
                self.cur_step += self.handle_del(
                    stream,
                    args,
                    db,
                    db_config,
                    global_state,
                    &is_propagation,
                    connection,
                );
            }
            "incr" => {
                self.cur_step += self.handle_incr(
                    stream,
                    args,
                    db,
                    db_config,
                    global_state,
                    &is_propagation,
                    connection,
                );
            }
            "config" => {
                self.cur_step += self.handle_config(stream, args, global_state, connection);
            }
            "keys" => {
                self.cur_step += self.handle_keys(stream, args, db, db_config, connection);
            }
            "info" => {
                self.handle_info(stream, args, db, db_config, global_state, connection);
            }
            "replconf" => {
                self.cur_step +=
                    self.handle_replconf(stream, args, global_state, connection, local_offset);
            }
            "psync" => {
                self.cur_step += self.handle_psync(stream, args, global_state, connection);
            }
            "wait" => {
                self.cur_step += self.handle_wait(stream, args, global_state, connection);
            }
            "multi" => {
                self.handle_multi(stream, connection);
            }
            "discard" => {
                self.handle_discard(stream, connection);
            }

            "exec" => {
                self.handle_exec(stream, db, db_config, global_state, connection);
            }

            "type" => {
                self.cur_step += self.handle_type(stream, args, db, db_config, connection);
            }

            "command" | "docs" => {
                if connection.transaction.is_txing {
                    write_simple_string(stream, "QUEUED");
                }
                write_simple_string(stream, "OK");
            }

            _ => {
                write_error(stream, "unknown command");
            }
        }
    }

    fn handle_type(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        connection: &mut Connection,
    ) -> usize {
        if args.len() < 1 {
            write_error(stream, "wrong number of arguments for 'TYPE'");
            return 0;
        }

        let key = &args[0];

        if connection.transaction.is_txing {
            // Queue the full command with key
            connection.transaction.tasks.push(format!("type {}", key));
            write_simple_string(stream, "QUEUED");
            return 1;
        }

        // Check for expiration
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
            write_simple_string(stream, "none");
            return 1;
        }
        drop(config_map);

        let map = db.lock().unwrap();
        if let Some(val) = map.get(key) {
            write_simple_string(stream, val.type_name());
        } else {
            write_simple_string(stream, "none");
        }
        1
    }

    fn handle_discard(&self, stream: &mut TcpStream, connection: &mut Connection) {
        if !connection.transaction.is_txing {
            write_error(stream, "DISCARD without MULTI");
            return;
        }
        connection.transaction.is_txing = false;
        connection.transaction.tasks.clear();
        connection.transaction.response.clear();
        connection.transaction.job_done_at = None;
        write_simple_string(stream, "OK");
    }

    fn handle_multi(&self, stream: &mut TcpStream, connection: &mut Connection) {
        if connection.transaction.is_txing {
            write_error(stream, "Transaction has already started");
        }

        connection.transaction.is_txing = true;
        connection.transaction.tasks.clear();
        connection.transaction.response.clear();
        connection.transaction.job_done_at = None;
        write_simple_string(stream, "OK");
    }

    fn handle_exec(
        &self,
        stream: &mut TcpStream,
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        connection: &mut Connection,
    ) {
        if !connection.transaction.is_txing {
            write_error(stream, "EXEC without MULTI");
            return;
        }

        let mut runner = TransactionRunner::new(connection);
        runner.execute_transactions(db, db_config, global_state);

        write_resp_array(stream, &connection.transaction.response);
        connection.transaction.is_txing = false;
    }

    pub fn handle_wait(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        global_state: &RedisGlobalType,
        _connection: &mut Connection,
    ) -> usize {
        if args.len() < 2 {
            write_error(stream, "wrong number of arguments for 'WAIT'");
            return 0;
        }

        let numreplicas = match args[0].parse::<usize>() {
            Ok(n) => n,
            Err(_) => {
                write_error(stream, "ERR invalid number of replicas");
                return 0;
            }
        };

        let timeout_ms = match args[1].parse::<u64>() {
            Ok(t) => t,
            Err(_) => {
                write_error(stream, "ERR invalid timeout");
                return 0;
            }
        };

        let connected_replicas = {
            let global = global_state.lock().unwrap();
            global.replica_states.len()
        };

        let satisfied = if connected_replicas >= numreplicas {
            numreplicas
        } else {
            connected_replicas
        };

        let deadline = Instant::now() + Duration::from_millis(timeout_ms);

        let offset = {
            let guard = global_state.lock().unwrap();
            guard.offset_replica_sync
        };

        write_integer(stream, satisfied as i64);

        loop {
            let acks = {
                let guard = global_state.lock().unwrap();
                guard
                    .replica_states
                    .values()
                    .filter(|replica| replica.local_offset == offset)
                    .count()
            };
            if acks >= satisfied {
                return 2;
            }

            if Instant::now() >= deadline {
                return 2;
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn handle_psync(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        global_state: &RedisGlobalType,
        connection: &mut Connection,
    ) -> usize {
        let mut global = global_state.lock().unwrap();
        if args.len() >= 2 {
            write_simple_string(
                stream,
                &format!(
                    "FULLRESYNC {} {}",
                    global.master_replid, global.master_repl_offset
                ),
            );

            let stream_clone = stream.try_clone().unwrap();
            if let Some(ref slave_port) = connection.slave_port {
                add_replica(&mut global, stream_clone, slave_port);
                write_redis_file(
                    stream,
                    &format!("{}/{}", global.dir_path, global.dbfilename),
                );
                connection.is_slave_established = true;
            }
            return 2;
        }
        0
    }

    pub fn handle_replconf(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        global_state: &RedisGlobalType,
        connection: &mut Connection,
        local_offset: &usize,
    ) -> usize {
        if args.len() >= 2 {
            let subcmd = args[0].to_ascii_lowercase();
            match subcmd.as_str() {
                "listening-port" => {
                    write_simple_string(stream, "OK");
                    let connection_port = &args[1];

                    connection.slave_port = Some(connection_port.clone());
                    return 2;
                }
                "capa" => {
                    let mut idx = 1;
                    let mut caps = Vec::new();
                    while idx < args.len() {
                        let cap = args[idx].to_ascii_lowercase();

                        if cap != "psync2" {
                            break;
                        }

                        caps.push(cap.clone());

                        idx += 1;
                    }

                    if !caps.is_empty() {
                        write_simple_string(stream, "OK");
                        if let Some(ref slave_port) = connection.slave_port {
                            let mut global = global_state.lock().unwrap();
                            global.set_slave_caps(slave_port.clone(), caps.clone());
                        } else {
                            panic!("slave_port is not set before REPLCONF capa");
                        }
                        return 1 + caps.len();
                    }
                    return 1;
                }

                "getack" => {
                    write_array(
                        stream,
                        &[
                            Some("REPLCONF"),
                            Some("ACK"),
                            Some(&local_offset.to_string()),
                        ],
                    );
                    return 2;
                }
                _ => return 0,
            }
        }
        write_error(stream, "syntax error");
        0
    }

    fn handle_info(
        &self,
        stream: &mut TcpStream,
        _args: &[String],
        _db: &DbType,
        _db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        connection: &mut Connection,
    ) {
        // If in transaction, queue the command and return
        if connection.transaction.is_txing {
            connection.transaction.tasks.push("info".to_string());
            write_simple_string(stream, "QUEUED");
            return;
        }

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

        write_bulk_string(stream, &info);
    }

    fn handle_keys(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        connection: &mut Connection,
    ) -> usize {
        if connection.transaction.is_txing {
            if args.len() >= 1 {
                connection
                    .transaction
                    .tasks
                    .push(format!("keys {}", args[0]));
            } else {
                connection.transaction.tasks.push("keys".to_string());
            }
            write_simple_string(stream, "QUEUED");
            if args.len() == 1 {
                1
            } else {
                0
            }
        } else if args.len() == 1 {
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

            let valid_keys: Vec<Option<&str>> = db_config
                .keys()
                .filter(|key| is_matched(&args[0], key))
                .map(|key| Some(key.as_str()))
                .collect();

            write_array(stream, &valid_keys);
            1
        } else {
            write_array::<&str>(stream, &[]);
            0
        }
    }

    fn handle_ping(&self, stream: &mut TcpStream, connection: &mut Connection) {
        if connection.transaction.is_txing {
            connection.transaction.tasks.push(String::from("Ping"));
            write_simple_string(stream, "QUEUED");

            return;
        }
        write_simple_string(stream, "PONG");
    }

    fn handle_echo(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        connection: &mut Connection,
    ) -> usize {
        if let Some(msg) = args.get(0) {
            if connection.transaction.is_txing {
                connection.transaction.tasks.push(format!("echo {msg}"));
                write_simple_string(stream, "QUEUED");
                return 1;
            }
            write_simple_string(stream, msg);
            1
        } else {
            if connection.transaction.is_txing {
                connection.transaction.tasks.push(format!("echo"));
                write_simple_string(stream, "QUEUED");
                return 0;
            }
            write_simple_string(stream, "");
            0
        }
    }

    fn handle_config(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        global_state: &RedisGlobalType,
        connection: &mut Connection,
    ) -> usize {
        if args.len() >= 2 && args[0].to_ascii_lowercase() == "get" {
            let mut consumed = 1;
            let config_key = args[1].to_ascii_lowercase();

            if connection.transaction.is_txing {
                connection
                    .transaction
                    .tasks
                    .push(format!("config get {}", args[1]));
                write_simple_string(stream, "QUEUED");
                consumed += 1;
                return consumed;
            }

            match config_key.as_str() {
                "dir" => {
                    let global = global_state.lock().unwrap();
                    write_array(stream, &[Some("dir"), Some(&global.dir_path)]);
                    consumed += 1;
                }
                "dbfilename" => {
                    let global = global_state.lock().unwrap();
                    write_array(stream, &[Some("dbfilename"), Some(&global.dbfilename)]);
                    consumed += 1;
                }
                _ => {
                    write_array::<&str>(stream, &[]);
                }
            }
            consumed
        } else {
            write_error(stream, "invalid config argument");
            0
        }
    }

    fn handle_get(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        connection: &mut Connection,
    ) -> usize {
        if args.len() < 1 {
            write_error(stream, "wrong number of arguments for 'GET'");
            return 0;
        }
        if connection.transaction.is_txing {
            connection
                .transaction
                .tasks
                .push(format!("get {}", args[0]));
            write_simple_string(stream, "QUEUED");
            return 1;
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
            write_null_bulk_string(stream);
        } else {
            drop(config_map);

            let map = db.lock().unwrap();
            if let Some(val) = map.get(key) {
                write_bulk_string(stream, &val.to_string());
            } else {
                write_null_bulk_string(stream);
            }
        }
        1
    }

    fn handle_set(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        connection: &mut Connection,
    ) -> usize {
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };
        if args.len() < 2 {
            if is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'SET'");
            }
            return 0;
        }

        if connection.transaction.is_txing {
            let mut consumed = 2;
            let mut idx = 2;
            while idx < args.len() {
                let opt = args[idx].to_ascii_lowercase();
                match opt.as_str() {
                    "ex" | "px" => {
                        if idx + 1 < args.len() {
                            consumed += 2;
                            idx += 2;
                        } else {
                            write_error(stream, "invalid EX argument");
                            return consumed;
                        }
                    }
                    _ => break,
                }
            }
            let mut task = String::from("set");
            for i in 0..consumed {
                if let Some(arg) = args.get(i) {
                    task.push(' ');
                    task.push_str(arg);
                }
            }
            connection.transaction.tasks.push(task);
            write_simple_string(stream, "QUEUED");
            return consumed;
        }

        let mut consumed = 0;

        let key = args[0].clone();
        let value = args[1].clone();
        consumed += 2;

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
                            if !is_slave_and_propagation {
                                write_error(stream, "invalid EX argument");
                            }
                            return 0;
                        }
                        idx += 2;
                    } else {
                        if !is_slave_and_propagation {
                            write_error(stream, "missing EX argument");
                        }
                        return 0;
                    }
                    consumed += 2;
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
                            if !is_slave_and_propagation {
                                write_error(stream, "invalid PX argument");
                            }
                            return 0;
                        }
                        idx += 2;
                    } else {
                        if !is_slave_and_propagation {
                            write_error(stream, "missing PX argument");
                        }
                        return 0;
                    }
                    consumed += 2;
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

        if !is_slave_and_propagation {
            write_simple_string(stream, "OK");
        }
        consumed
    }

    fn handle_del(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        connection: &mut Connection,
    ) -> usize {
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };
        if args.is_empty() {
            if is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'DEL'");
            }
            return 0;
        }

        if connection.transaction.is_txing {
            connection
                .transaction
                .tasks
                .push(format!("del {}", args[0]));
            write_simple_string(stream, "QUEUED");
            return 1;
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
        if !is_slave_and_propagation {
            write_integer(stream, removed);
        }
        propagate_slaves(
            global_state,
            &format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key),
        );
        1
    }

    fn handle_incr(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        connection: &mut Connection,
    ) -> usize {
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };

        if args.is_empty() {
            if is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'INCR'");
            }
            return 0;
        }

        if connection.transaction.is_txing {
            connection
                .transaction
                .tasks
                .push(format!("incr {}", args[0]));
            write_simple_string(stream, "QUEUED");
            return 1;
        }

        let key = &args[0];
        let mut result_value = 0;

        {
            let mut map = db.lock().unwrap();
            let mut config_map = db_config.lock().unwrap();

            if !config_map.contains_key(key) || !map.contains_key(key) {
                map.insert(key.clone(), ValueType::String("1".to_string()));
                config_map.insert(key.clone(), Default::default());
                result_value = 1;
            } else {
                if let Some(cfg) = config_map.get(key) {
                    if cfg.is_expired() {
                        map.remove(key);
                        config_map.remove(key);
                        write_error(stream, &format!("key {key} is expired"));
                        return 1;
                    }
                }
                let value = map.get(key).unwrap();
                let parsed = match value {
                    ValueType::String(s) => s.parse::<i64>(),
                    _ => {
                        write_error(stream, "value is not an integer or out of range");
                        return 1;
                    }
                };
                let new_value = match parsed {
                    Ok(val) => val + 1,
                    Err(_) => {
                        write_error(stream, "value is not an integer or out of range");
                        return 1;
                    }
                };
                map.insert(key.clone(), ValueType::String(new_value.to_string()));
                result_value = new_value;
            }
        }
        if !is_slave_and_propagation {
            write_integer(stream, result_value);
        }
        propagate_slaves(
            global_state,
            &format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key),
        );
        1
    }
}

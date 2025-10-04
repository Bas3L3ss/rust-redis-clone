use crate::structs::config::Config;
use crate::structs::connection::Connection;
use crate::types::{DbConfigType, DbType, RedisGlobalType};
use crate::utils::{
    is_matched, write_array, write_bulk_string, write_error, write_integer, write_null_bulk_string,
    write_redis_file, write_simple_string,
};
use std::io::Write;
use std::net::TcpStream;
use std::time::{SystemTime, UNIX_EPOCH};

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
    ) {
        while self.cur_step < self.args.len() {
            self.step(stream, db, db_config, global_state, connection);
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
    ) {
        if self.args.is_empty() {
            write_error(stream, "empty command");
            self.cur_step += 1;
            return;
        }

        let command = self.args[self.cur_step].to_ascii_uppercase();
        let args = &self.args[self.cur_step + 1..];

        eprintln!("Received command: {:?}", command);

        match command.as_str() {
            "PING" => {
                self.handle_ping(stream);
            }
            "ECHO" => {
                self.cur_step += self.handle_echo(stream, args);
            }
            "SET" => {
                self.cur_step += self.handle_set(stream, args, db, db_config, global_state);
            }
            "GET" => {
                self.cur_step += self.handle_get(stream, args, db, db_config);
            }
            "DEL" => {
                self.cur_step += self.handle_del(stream, args, db, db_config, global_state);
            }
            "CONFIG" => {
                self.cur_step += self.handle_config(stream, args, global_state);
            }
            "KEYS" => {
                self.cur_step += self.handle_keys(stream, args, db, db_config);
            }
            "INFO" => {
                self.handle_info(stream, args, db, db_config, global_state);
            }
            "REPLCONF" => {
                self.cur_step += self.handle_replconf(stream, args, global_state, connection);
            }
            "PSYNC" => {
                self.cur_step += self.handle_psync(stream, args, global_state, connection);
            }
            _ => {
                write_error(stream, "unknown command");
            }
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

            write_redis_file(
                stream,
                &format!("{}/{}", global.dir_path, global.dbfilename),
            );

            let stream_clone = stream.try_clone().unwrap();
            if let Some(ref slave_port) = connection.slave_port {
                global.set_slave_streams(slave_port.clone(), stream_clone);
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
    ) {
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
    ) -> usize {
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

    fn handle_ping(&self, stream: &mut TcpStream) {
        write_simple_string(stream, "PONG");
    }

    fn handle_echo(&self, stream: &mut TcpStream, args: &[String]) -> usize {
        if let Some(msg) = args.get(0) {
            write_simple_string(stream, msg);
            1
        } else {
            write_simple_string(stream, "");
            0
        }
    }

    fn handle_config(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        global_state: &RedisGlobalType,
    ) -> usize {
        if args.len() >= 2 && args[0].to_ascii_uppercase() == "GET" {
            let mut consumed = 1;
            let global = global_state.lock().unwrap();
            match args[1].to_ascii_lowercase().as_str() {
                "dir" => {
                    write_array(stream, &[Some("dir"), Some(&global.dir_path)]);
                    consumed += 1;
                }
                "dbfilename" => {
                    write_array(stream, &[Some("dbfilename"), Some(&global.dbfilename)]);
                    consumed += 1;
                }
                _ => {
                    write_array::<&str>(stream, &[]);
                }
            }
            consumed
        } else {
            write_array::<&str>(stream, &[]);
            0
        }
    }

    fn handle_get(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
    ) -> usize {
        if args.len() < 1 {
            write_error(stream, "wrong number of arguments for 'GET'");
            return 0;
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
                write_bulk_string(stream, val);
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
    ) -> usize {
        if args.len() < 2 {
            write_error(stream, "wrong number of arguments for 'SET'");
            return 0;
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
            let opt = args[idx].to_ascii_uppercase();
            match opt.as_str() {
                "EX" => {
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
                            write_error(stream, "invalid EX argument");
                            return 0;
                        }
                        idx += 2;
                    } else {
                        write_error(stream, "missing EX argument");
                        return 0;
                    }
                    consumed += 2;
                }
                "PX" => {
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
                            write_error(stream, "invalid PX argument");
                            return 0;
                        }
                        idx += 2;
                    } else {
                        write_error(stream, "missing PX argument");
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
            map.insert(key.clone(), value.clone());
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

        self.propogate_slaves(global_state, &propagation);

        write_simple_string(stream, "OK");
        consumed
    }

    fn handle_del(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        db_config: &DbConfigType,
        global_state: &RedisGlobalType,
    ) -> usize {
        if args.is_empty() {
            write_error(stream, "wrong number of arguments for 'DEL'");
            return 0;
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
            self.propogate_slaves(
                global_state,
                &format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key),
            );
        }
        write_integer(stream, removed);
        1
    }

    fn propogate_slaves(&self, global_state: &RedisGlobalType, message: &str) {
        let mut global_guard = global_state.lock().unwrap();
        for stream_arc in global_guard.slave_streams.values_mut() {
            if let Ok(mut stream) = stream_arc.lock() {
                if let Err(e) = stream.write_all(message.as_bytes()) {
                    eprintln!("Failed to propagate to slave: {:?}", e);
                }
            }
        }
    }
}

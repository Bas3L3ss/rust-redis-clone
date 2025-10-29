use crate::enums::add_stream_entries_result::StreamResult;
use crate::enums::val_type::ValueType;
use crate::geo::{decode, encode, validate_latitude, validate_longitude};
use crate::structs::config::Config;
use crate::structs::connection::Connection;
use crate::structs::replica::add_replica;
use crate::structs::stream::Stream;
use crate::structs::transaction_runner::TransactionRunner;
use crate::structs::xread_config::XreadConfig;
use crate::structs::zset::ZSet;
use crate::types::{DbConfigType, DbType, RedisGlobalType};
use crate::utils::{
    is_matched, parse_range, propagate_slaves, write_array, write_bulk_string, write_error,
    write_integer, write_null_array, write_null_bulk_string, write_redis_file, write_resp_array,
    write_simple_string,
};
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::thread::sleep;
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
            "xadd" => {
                self.cur_step +=
                    self.handle_xadd(stream, args, db, global_state, &is_propagation, connection);
            }
            "xrange" => {
                self.cur_step += self.handle_xrange(stream, args, db, connection);
            }
            "xread" => {
                self.cur_step += self.handle_xread(stream, args, db, connection);
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

            "rpush" => {
                self.cur_step +=
                    self.handle_rpush(stream, args, db, global_state, &is_propagation, connection);
            }

            "lpush" => {
                self.cur_step +=
                    self.handle_lpush(stream, args, db, global_state, &is_propagation, connection);
            }

            "lpop" => {
                self.cur_step +=
                    self.handle_lpop(stream, args, db, global_state, &is_propagation, connection);
            }

            "zadd" => {
                self.cur_step +=
                    self.handle_zadd(stream, args, db, global_state, &is_propagation, connection);
            }
            "zrem" => {
                self.cur_step +=
                    self.handle_zrem(stream, args, db, global_state, &is_propagation, connection);
            }

            "zscore" => {
                self.cur_step += self.handle_zscore(stream, args, db, connection);
            }

            "zrank" => {
                self.cur_step += self.handle_zrank(stream, args, db, connection);
            }

            "zrange" => {
                self.cur_step += self.handle_zrange(stream, args, db, connection);
            }

            "zcard" => {
                self.cur_step += self.handle_zcard(stream, args, db, connection);
            }

            "blpop" => {
                self.cur_step +=
                    self.handle_blpop(stream, args, db, global_state, &is_propagation, connection);
            }

            "llen" => {
                self.cur_step += self.handle_llen(stream, args, db, connection);
            }

            "lrange" => {
                self.cur_step += self.handle_lrange(stream, args, db, connection);
            }

            "command" | "docs" => {
                if connection.transaction.is_txing {
                    write_simple_string(stream, "QUEUED");
                }
                write_simple_string(stream, "OK");
            }

            "geoadd" => {
                self.cur_step +=
                    self.handle_geoadd(stream, args, db, global_state, &is_propagation, connection);
            }

            "geopos" => {
                self.cur_step += self.handle_geopos(stream, args, db, connection);
            }

            _ => {
                write_error(stream, "unknown command");
            }
        }
    }

    fn handle_zadd(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };

        if args.len() < 3 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'ZADD'");
            }
            return 3;
        }

        let zset_key = &args[0];
        let score = match args[1].parse::<f64>() {
            Ok(score) => score,
            Err(_) => {
                if !is_slave_and_propagation {
                    write_error(stream, "invalid score for 'ZADD': must be a number");
                }
                return 3;
            }
        };
        let member = &args[2];
        let mut _added_number = 1;
        {
            let mut map = db.lock().unwrap();
            let zset_opt = map.get_mut(zset_key);

            if let Some(ValueType::ZSet(zset)) = zset_opt {
                _added_number = zset.zadd(score, member.clone());
            } else {
                let mut new_zset = ZSet::new();
                _added_number = new_zset.zadd(score, member.clone());
                map.insert(zset_key.clone(), ValueType::ZSet(new_zset));
            }
        }

        if !is_slave_and_propagation {
            write_integer(stream, _added_number);
            let propagation = format!("ZADD {} {} {}", zset_key, score, member);
            propagate_slaves(global_state, &propagation);
        }

        3
    }

    fn handle_geoadd(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };

        if args.len() < 4 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'GEOADD'");
            }
            return 0;
        }

        let zset_key = &args[0];
        let longitude = match args[1].parse::<f64>() {
            Ok(long) if validate_longitude(long) => long,
            _ => {
                if !is_slave_and_propagation {
                    write_error(
                        stream,
                        "invalid score for 'GEOADD': must be a valid longitude (-180..180)",
                    );
                }
                return 4;
            }
        };
        let latitude = match args[2].parse::<f64>() {
            Ok(lat) if validate_latitude(lat) => lat,
            _ => {
                if !is_slave_and_propagation {
                    write_error(stream, "invalid score for 'GEOADD': must be a valid latitude (-85.05112878..85.05112878)");
                }
                return 4;
            }
        };

        let score = encode(latitude, longitude);

        let member = &args[3];
        let mut _added_number = 1;
        {
            let mut map = db.lock().unwrap();
            let zset_opt = map.get_mut(zset_key);

            if let Some(ValueType::ZSet(zset)) = zset_opt {
                _added_number = zset.zadd(score as f64, member.clone());
            } else {
                let mut new_zset = ZSet::new();
                _added_number = new_zset.zadd(score as f64, member.clone());
                map.insert(zset_key.clone(), ValueType::ZSet(new_zset));
            }
        }

        if !is_slave_and_propagation {
            write_integer(stream, _added_number);
            let propagation = format!("ZADD {} {} {}", zset_key, 0.0, member);
            propagate_slaves(global_state, &propagation);
        }

        4
    }

    fn handle_zrem(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };

        if args.len() < 2 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'ZREM'");
            }
            return 0;
        }

        let zset_key = &args[0];
        let member = &args[1];

        let mut _removed_number = 1;
        {
            let mut map = db.lock().unwrap();
            let zset_opt = map.get_mut(zset_key);

            if let Some(ValueType::ZSet(zset)) = zset_opt {
                _removed_number = zset.zrem(member);
            } else {
                _removed_number = 0;
            }
        }

        if !is_slave_and_propagation {
            write_integer(stream, _removed_number as i64);
            let propagation = format!("ZREM {} {}", zset_key, member);
            propagate_slaves(global_state, &propagation);
        }

        3
    }

    fn handle_blpop(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };

        if args.len() < 2 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'BLPOP'");
            }
            return 0;
        }

        let list_key = &args[0];
        let timeout = match args[1].parse::<f64>() {
            Ok(t) if t >= 0.0 => t,
            _ => {
                write_error(
                    stream,
                    "invalid arguments for BLPOP: timeout must be a non-negative number",
                );
                return 2;
            }
        };
        let timeout = timeout;

        let start_time = Instant::now();
        loop {
            {
                let mut map = db.lock().unwrap();
                if let Some(val) = map.get_mut(list_key) {
                    if let ValueType::List(ref mut redis_list) = val {
                        if !redis_list.is_empty() {
                            let popped = redis_list.remove(0);
                            if !is_slave_and_propagation {
                                write_array(
                                    stream,
                                    &[Some(list_key.as_str()), Some(popped.as_str())],
                                );
                                propagate_slaves(global_state, &format!("LPOP {}", list_key));
                                return 2;
                            }
                        }
                    } else {
                        if !is_slave_and_propagation {
                            write_error(
                                stream,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                        }

                        return 2;
                    }
                }
            }

            if timeout > 0.0 {
                let elapsed = start_time.elapsed();
                if elapsed.as_secs_f64() >= timeout {
                    let _ = stream.write_all(b"*-1\r\n");
                    return 2;
                }
                sleep(Duration::from_millis(10));
                continue;
            } else {
                sleep(Duration::from_millis(10));
                continue;
            }
        }
    }

    fn handle_lpop(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };

        if args.len() < 1 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'LPOP'");
            }
            return 0;
        }

        let list_key = &args[0];
        let mut count = 1;
        let mut has_count = false;
        if args.len() >= 2 {
            match args[1].parse::<usize>() {
                Ok(val) if val > 0 => {
                    count = val;
                    has_count = true;
                }
                _ => {
                    if !is_slave_and_propagation {
                        write_error(stream, "ERR value is not an integer or out of range");
                    }
                    return 0;
                }
            }
        }
        let consumed = if has_count { 2 } else { 1 };

        let mut map = db.lock().unwrap();
        if let Some(val) = map.get_mut(list_key) {
            if let ValueType::List(ref mut redis_list) = val {
                if !redis_list.is_empty() {
                    let remove_count = count.min(redis_list.len());
                    let mut removed_elems = Vec::with_capacity(remove_count);
                    for _ in 0..remove_count {
                        removed_elems.push(redis_list.remove(0));
                    }
                    if !is_slave_and_propagation {
                        if count == 1 {
                            if !removed_elems.is_empty() {
                                write_bulk_string(stream, &removed_elems[0]);
                            } else {
                                write_null_bulk_string(stream);
                            }
                        } else {
                            let arr: Vec<Option<&str>> =
                                removed_elems.iter().map(|s| Some(s.as_str())).collect();
                            write_array(stream, &arr);
                        }
                    }
                    if redis_list.is_empty() {
                        map.remove(list_key);
                    }
                    if !is_slave_and_propagation {
                        let propagation = if args.len() >= 2 {
                            format!("LPOP {} {}", list_key, count)
                        } else {
                            format!("LPOP {}", list_key)
                        };
                        propagate_slaves(global_state, &propagation);
                    }
                    return consumed;
                } else {
                    if !is_slave_and_propagation {
                        if count == 1 {
                            write_null_bulk_string(stream);
                        } else {
                            write_array::<&str>(stream, &[]);
                        }
                    }
                    if !is_slave_and_propagation {
                        let propagation = if args.len() >= 2 {
                            format!("LPOP {} {}", list_key, count)
                        } else {
                            format!("LPOP {}", list_key)
                        };
                        propagate_slaves(global_state, &propagation);
                    }
                    return consumed;
                }
            } else {
                if !is_slave_and_propagation {
                    write_error(
                        stream,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
                return consumed;
            }
        }

        if !is_slave_and_propagation {
            if count == 1 {
                write_null_bulk_string(stream);
            } else {
                write_array::<&str>(stream, &[]);
            }
            let propagation = if args.len() >= 2 {
                format!("LPOP {} {}", list_key, count)
            } else {
                format!("LPOP {}", list_key)
            };
            propagate_slaves(global_state, &propagation);
        }
        consumed
    }

    fn handle_llen(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        if args.len() < 1 {
            write_error(stream, "wrong number of arguments for 'LLEN'");
            return 0;
        }
        let list_key = &args[0];

        let map = db.lock().unwrap();
        if let Some(val) = map.get(list_key) {
            if let ValueType::List(ref redis_list) = val {
                write_integer(stream, redis_list.len() as i64);
            } else {
                write_integer(stream, 0);
            }
        } else {
            write_integer(stream, 0);
        }
        1
    }
    fn handle_zrank(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        if args.len() < 2 {
            write_error(stream, "wrong number of arguments for 'ZRANK'");
            return 0;
        }
        let zset_key = &args[0];
        let member = &args[1];

        let map = db.lock().unwrap();
        if let Some(ValueType::ZSet(zset)) = map.get(zset_key) {
            if let Some(rank) = zset.zrank(member) {
                write_integer(stream, rank as i64);
            } else {
                write_null_bulk_string(stream);
            }
        } else {
            write_null_bulk_string(stream);
        }
        2
    }

    fn handle_zrange(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        if args.len() < 3 {
            write_error(stream, "wrong number of arguments for 'ZRANGE'");
            return 0;
        }
        let zset_key = &args[0];
        let start = match args[1].parse::<i64>() {
            Ok(v) => v,
            Err(_) => {
                write_error(stream, "ERR value is not an integer or out of range");
                return 0;
            }
        };
        let end = match args[2].parse::<i64>() {
            Ok(v) => v,
            Err(_) => {
                write_error(stream, "ERR value is not an integer or out of range");
                return 0;
            }
        };

        let map = db.lock().unwrap();
        if let Some(ValueType::ZSet(zset)) = map.get(zset_key) {
            let sorted_members: Vec<Option<String>> = zset
                .zrange(start, end)
                .into_iter()
                .map(|item| Some(item.1))
                .collect();
            write_array(stream, &sorted_members);
        } else {
            write_array::<&str>(stream, &[]);
        }
        3
    }

    fn handle_zcard(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        if args.len() < 1 {
            write_error(stream, "wrong number of arguments for 'ZCARD'");
            return 0;
        }
        let zset_key = &args[0];

        let map = db.lock().unwrap();

        if let Some(ValueType::ZSet(zset)) = map.get(zset_key) {
            write_integer(stream, zset.zcard() as i64);
        } else {
            write_integer(stream, 0);
        }
        1
    }

    fn handle_geopos(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction
        if args.len() < 2 {
            write_error(stream, "wrong number of arguments for 'GEOPOS'");
            return 0;
        }
        let zset_key = &args[0];
        let places = &args[1..];

        let map = db.lock().unwrap();

        if let Some(ValueType::ZSet(zset)) = map.get(zset_key) {
            // Form RESP array of size = places.len()
            let _ = stream.write_all(format!("*{}\r\n", places.len()).as_bytes());
            for place in places {
                if let Some(score) = zset.zscore(place) {
                    let (lat, long) = decode(score.clone() as u64);
                    let _ = stream.write_all(b"*2\r\n");
                    write_bulk_string(stream, &long.to_string());
                    write_bulk_string(stream, &lat.to_string());
                } else {
                    write_null_array(stream);
                }
            }
        } else {
            let _ = stream.write_all(format!("*{}\r\n", places.len()).as_bytes());
            for _ in places {
                write_null_array(stream);
            }
        }
        args.len()
    }

    fn handle_zscore(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        if args.len() < 2 {
            write_error(stream, "wrong number of arguments for 'ZSCORE'");
            return 0;
        }
        let zset_key = &args[0];
        let member = &args[1];

        let map = db.lock().unwrap();

        if let Some(ValueType::ZSet(zset)) = map.get(zset_key) {
            if let Some(score) = zset.zscore(member) {
                write_bulk_string(stream, &score.to_string());
            }
        } else {
            write_null_bulk_string(stream);
        }
        2
    }

    fn handle_lrange(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        if args.len() < 3 {
            write_error(stream, "wrong number of arguments for 'LRANGE'");
            return 0;
        }
        let stream_key = &args[0];

        let map = db.lock().unwrap();
        let redis_list = match map.get(stream_key) {
            Some(val) => {
                if let ValueType::List(ref redis_list) = val {
                    redis_list
                } else {
                    write_error(
                        stream,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return 3;
                }
            }
            None => {
                write_array::<&str>(stream, &[]);
                return 3;
            }
        };

        let (start_i64, end_i64) = match (args[1].parse::<i64>(), args[2].parse::<i64>()) {
            (Ok(s), Ok(e)) => (s, e),
            _ => {
                write_error(
                    stream,
                    "invalid arguments for LRANGE: start and end must be integers",
                );
                return 3;
            }
        };

        let list_len = redis_list.len() as i64;

        // Calculate start index
        let mut start = if start_i64 < 0 {
            list_len + start_i64
        } else {
            start_i64
        };
        if start < 0 {
            start = 0;
        }
        let start = start as usize;

        // Calculate end index (inclusive)
        let mut end = if end_i64 < 0 {
            list_len + end_i64
        } else {
            end_i64
        };
        if end < 0 {
            end = 0;
        }
        let end = end as usize;

        if start >= redis_list.len() || end < start {
            write_array::<&str>(stream, &[]);
        } else {
            // Redis LRANGE is inclusive of end
            let upper = if end + 1 > redis_list.len() {
                redis_list.len()
            } else {
                end + 1
            };
            let result: Vec<Option<&str>> = redis_list[start..upper]
                .iter()
                .map(|s| Some(s.as_str()))
                .collect();
            write_array(stream, &result);
        }
        3
    }

    fn handle_rpush(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction runner and enqueuing
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };
        if args.len() < 2 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'RPUSH'");
            }
            return 0;
        }

        let list_key = &args[0];
        let mut consumed = 1;
        let mut val_vec: Vec<String> = vec![];
        for idx in 1..args.len() {
            val_vec.push(args[idx].clone());
            consumed += 1;
        }
        let mut len = val_vec.len();

        {
            let mut map = db.lock().unwrap();
            if let Some(val_ref) = map.get_mut(list_key) {
                if let ValueType::List(ref mut redis_list) = val_ref {
                    for val in &val_vec {
                        redis_list.push(val.clone());
                    }
                    len = redis_list.len();
                } else {
                    map.insert(list_key.clone(), ValueType::List(val_vec.clone()));
                }
            } else {
                map.insert(list_key.clone(), ValueType::List(val_vec.clone()));
            }
        }

        if !is_slave_and_propagation {
            write_integer(stream, len as i64);
            let propagation = format!("RPUSH {} {}", list_key, val_vec.join(" "));
            propagate_slaves(global_state, &propagation);
        }
        consumed
    }

    fn handle_lpush(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction runner and enqueuing
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };
        if args.len() < 2 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'LPUSH'");
            }
            return 0;
        }

        let list_key = &args[0];
        let mut consumed = 1;
        let mut val_vec: Vec<String> = vec![];
        for idx in 1..args.len() {
            val_vec.push(args[idx].clone());
            consumed += 1;
        }
        let mut len = val_vec.len();

        {
            let mut map = db.lock().unwrap();
            if let Some(val_ref) = map.get_mut(list_key) {
                if let ValueType::List(ref mut redis_list) = val_ref {
                    for val in &val_vec {
                        redis_list.insert(0, val.clone());
                    }
                    len = redis_list.len();
                } else {
                    val_vec.reverse();
                    map.insert(list_key.clone(), ValueType::List(val_vec.clone()));
                }
            } else {
                val_vec.reverse();
                map.insert(list_key.clone(), ValueType::List(val_vec.clone()));
            }
        }

        if !is_slave_and_propagation {
            write_integer(stream, len as i64);
            let propagation = format!("RPUSH {} {}", list_key, val_vec.join(" "));
            propagate_slaves(global_state, &propagation);
        }
        consumed
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

    fn handle_xread(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        let (mut xread_config, consumed, err) = XreadConfig::from_args(&args);
        if let Some(e) = err {
            write_error(stream, &e);
            return consumed;
        }

        if let Some(block) = xread_config.block {
            let start_time = Instant::now();
            let block_duration = Duration::from_millis(block as u64);

            let latest_snapshot = {
                let db_guard = db.lock().unwrap();
                let map: HashMap<_, _> = xread_config
                    .streams
                    .iter()
                    .filter_map(|(key, _)| {
                        db_guard.get(key).and_then(|val| {
                            if let ValueType::Stream(redis_stream) = val {
                                Some((key.clone(), redis_stream.entries.len()))
                            } else {
                                Some((key.clone(), 0))
                            }
                        })
                    })
                    .collect();

                map
            };

            loop {
                let mut found_entries = false;

                for (key, range) in &mut xread_config.streams {
                    let db_guard = db.lock().unwrap();
                    if let Some(ValueType::Stream(redis_stream)) = db_guard.get(key) {
                        if range == "$" {
                            if let Some(latest_num) = latest_snapshot.get(key) {
                                if let Some(ValueType::Stream(redis_stream)) = db_guard.get(key) {
                                    if redis_stream.entries.len() > *latest_num {
                                        found_entries = true;
                                        break;
                                    }
                                }
                            }
                        } else {
                            let range_opt = parse_range(range, None);

                            if let Some(start_range) = range_opt {
                                let entries = redis_stream.range_start(start_range, true);
                                if !entries.is_empty() {
                                    found_entries = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if found_entries {
                    break;
                }

                if block_duration != Duration::from_millis(0)
                    && start_time.elapsed() >= block_duration
                {
                    let _ = stream.write_all(b"*-1\r\n");
                    return consumed;
                }

                sleep(Duration::from_millis(10));
            }
        }

        if let Some(_count) = xread_config.count {}

        if xread_config.streams.is_empty() {
            write_error(stream, "no streams specified for XREAD");
            return consumed;
        }

        let _ = stream.write_all(format!("*{}\r\n", xread_config.streams.len()).as_bytes());

        for (key, range) in xread_config.streams {
            let db_guard = db.lock().unwrap();
            if let Some(ValueType::Stream(redis_stream)) = db_guard.get(&key) {
                let range_opt = parse_range(&range, redis_stream.last_entry_id());

                if range_opt.is_none() {
                    write_error(stream, "not valid id");
                    continue;
                }

                let start_range = range_opt.unwrap();
                let entries = redis_stream.range_start(start_range, range != "$");

                let _ = stream.write_all(b"*2\r\n");
                let _ = stream.write_all(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());

                let _ = stream.write_all(format!("*{}\r\n", entries.len()).as_bytes());

                for entry in entries {
                    let entry_id = format!("{}-{}", entry.milisec, entry.sequence_number);

                    let _ = stream.write_all(b"*2\r\n");
                    let _ = stream
                        .write_all(format!("${}\r\n{}\r\n", entry_id.len(), entry_id).as_bytes());

                    let _ =
                        stream.write_all(format!("*{}\r\n", entry.key_val.len() * 2).as_bytes());

                    for (field, value) in &entry.key_val {
                        let _ = stream
                            .write_all(format!("${}\r\n{}\r\n", field.len(), field).as_bytes());
                        let _ = stream
                            .write_all(format!("${}\r\n{}\r\n", value.len(), value).as_bytes());
                    }
                }
            } else {
                write_error(stream, "stream not found or not of type 'stream'");
            }
        }

        consumed
    }

    fn handle_xrange(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        _connection: &mut Connection,
    ) -> usize {
        if args.len() < 3 {
            write_error(stream, "wrong number of arguments for 'XRANGE'");
            return 0;
        };
        let stream_key = &args[0];

        let mut _stream_obj: Option<&Stream> = None;

        let map = db.lock().unwrap();
        if let Some(val) = map.get(stream_key) {
            if let ValueType::Stream(ref stream) = val {
                _stream_obj = Some(stream);
            } else {
                write_error(
                    stream,
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                );
                return 3;
            }
        } else {
            write_null_bulk_string(stream);
            return 3;
        };

        if let Some(redis_stream) = _stream_obj {
            let (start, end) = (
                parse_range(&args[1], None),
                parse_range(&args[2], redis_stream.last_entry_id()),
            );
            if start.is_none() || end.is_none() {
                write_error(
                    stream,
                    "invalid arguments for XRANGE: start and end must be integers",
                );
                return 3;
            }

            let (start, end) = (start.unwrap(), end.unwrap());

            let range = redis_stream.range(start, end);

            let _ = stream.write_all(format!("*{}\r\n", range.len()).as_bytes());
            for entry in range {
                let id = format!("{}-{}", entry.milisec, entry.sequence_number);

                let mut fields = Vec::with_capacity(entry.key_val.len());
                for (k, v) in &entry.key_val {
                    fields.push(k.clone());
                    fields.push(v.clone());
                }

                let mut resp = String::new();
                resp.push_str("*2\r\n");

                resp.push_str(&format!("${}\r\n{}\r\n", id.len(), id));

                // Second element: key-value array
                resp.push_str(&format!("*{}\r\n", entry.key_val.len() * 2));
                for (k, v) in &entry.key_val {
                    resp.push_str(&format!("${}\r\n{}\r\n", k.len(), k));
                    resp.push_str(&format!("${}\r\n{}\r\n", v.len(), v));
                }

                let _ = stream.write_all(resp.as_bytes());
            }
        }
        3
    }

    fn handle_xadd(
        &self,
        stream: &mut TcpStream,
        args: &[String],
        db: &DbType,
        global_state: &RedisGlobalType,
        is_propagation: &bool,
        _connection: &mut Connection,
    ) -> usize {
        // TODO: transaction runner and enqueuing
        let is_slave_and_propagation = {
            let global = global_state.lock().unwrap();
            !global.is_master() && *is_propagation
        };
        if args.len() < 4 {
            if !is_slave_and_propagation {
                write_error(stream, "wrong number of arguments for 'XADD'");
            }
            return 0;
        }

        let stream_key = &args[0];
        let mut id = args[1].clone();
        let mut kv = Vec::new();
        let mut idx = 2;
        while idx + 1 < args.len() {
            let key = args[idx].clone();
            let value = args[idx + 1].clone();
            kv.push((key, value));
            idx += 2;
        }
        {
            let mut map = db.lock().unwrap();

            let add_result = if let Some(existing) = map.get_mut(stream_key) {
                if let ValueType::Stream(ref mut stream_obj) = existing {
                    stream_obj.add_entries(id.clone(), kv.clone())
                } else {
                    let mut s = Stream::new();
                    let ok = s.add_entries(id.clone(), kv.clone());
                    map.insert(stream_key.clone(), ValueType::Stream(s));
                    ok
                }
            } else {
                let mut s = Stream::new();
                let ok = s.add_entries(id.clone(), kv.clone());
                map.insert(stream_key.clone(), ValueType::Stream(s));
                ok
            };

            match add_result {
                StreamResult::Err(err) => {
                    if !is_slave_and_propagation {
                        write_error(stream, &err);
                    }
                    return idx;
                }
                StreamResult::Some(new_id) => id = new_id,
            }
        }
        if !is_slave_and_propagation {
            write_bulk_string(stream, &id);
            let mut propagation = format!("XADD {}", id);
            for (k, v) in &kv {
                propagation.push(' ');
                propagation.push_str(k);
                propagation.push(' ');
                propagation.push_str(v);
            }
            propagate_slaves(global_state, &propagation);
        }
        idx
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
            if !is_slave_and_propagation {
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
            if !is_slave_and_propagation {
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
            if !is_slave_and_propagation {
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
                _result_value = new_value;
            }
        }
        if !is_slave_and_propagation {
            write_integer(stream, _result_value);
        }
        propagate_slaves(
            global_state,
            &format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key),
        );
        1
    }
}

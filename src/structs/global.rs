use std::{
    collections::HashMap,
    env::Args,
    net::TcpStream,
    sync::{Arc, Mutex},
};

use crate::utils::sync_with_master;

#[derive(Debug)]
pub struct RedisGlobal {
    pub port: String,
    pub master_address: Option<(String, String)>,
    pub master_stream: Option<Arc<Mutex<TcpStream>>>,
    pub slave_caps: HashMap<String, Vec<String>>,
    pub slave_streams: HashMap<String, Arc<Mutex<TcpStream>>>,
    pub master_replid: String,
    pub master_repl_offset: usize,
    pub dir_path: String,
    pub dbfilename: String,
}

impl RedisGlobal {
    pub fn set_port(&mut self, port: String) {
        self.port = port;
    }

    pub fn set_slave_caps(&mut self, slave_port: String, caps: Vec<String>) {
        self.slave_caps.insert(slave_port, caps);
    }
    pub fn set_slave_streams(&mut self, slave_port: String, stream: TcpStream) {
        self.slave_streams
            .insert(slave_port, Arc::new(Mutex::new(stream)));
    }

    pub fn set_master(&mut self, master: Option<(String, String)>) {
        self.master_address = master;
    }

    pub fn is_master(&self) -> bool {
        let is_master = !(self.master_address.is_some() && self.master_stream.is_some());
        is_master
    }

    pub fn init(mut args: Args) -> Self {
        let mut port = "6379".to_string();
        let mut master_address: Option<(String, String)> = None;
        let master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        let master_repl_offset = 0;
        let mut dir_path = String::from("/var/tmp/redis");
        let mut dbfilename = String::from("dump.rdb");
        let mut master_stream = None;

        args.next(); // skip program name

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    if let Some(val) = args.next() {
                        port = val;
                    }
                }
                "--dir" => {
                    if let Some(val) = args.next() {
                        dir_path = val.to_string();
                    } else {
                        eprintln!("Error: --dir requires a value");
                    }
                }
                "--dbfilename" => {
                    if let Some(val) = args.next() {
                        dbfilename = val.to_string();
                    } else {
                        eprintln!("Error: --dbfilename requires a value");
                    }
                }

                "--replicaof" => {
                    if let Some(host_port) = args.next() {
                        let mut parts = host_port.splitn(2, ' ');
                        if let (Some(host), Some(port_str)) = (parts.next(), parts.next()) {
                            let stream = Some(Arc::new(Mutex::new(sync_with_master(
                                host,
                                port_str,
                                &port,
                                &dbfilename,
                            ))));
                            master_stream = stream;
                            master_address = Some((host.to_string(), port_str.to_string()));
                        }
                    }
                }
                _ => {}
            }
        }

        RedisGlobal {
            port,
            master_address,
            slave_caps: HashMap::new(),
            slave_streams: HashMap::new(),
            master_repl_offset,
            master_stream,
            master_replid: master_replid.to_string(),
            dbfilename,
            dir_path,
        }
    }
}

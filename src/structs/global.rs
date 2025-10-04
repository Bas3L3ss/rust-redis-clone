use std::{collections::HashMap, env::Args, net::TcpStream};

use crate::utils::sync_with_master;

#[derive(Debug)]
pub struct RedisGlobal {
    pub port: String,
    pub master: Option<(String, String)>,
    pub master_stream: Option<TcpStream>,
    pub slave_caps: HashMap<String, Vec<String>>,
    pub slave_streams: HashMap<String, TcpStream>,
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
        self.slave_streams.insert(slave_port, stream);
    }

    pub fn set_master(&mut self, master: Option<(String, String)>) {
        self.master = master;
    }

    pub fn is_master(&self) -> bool {
        if self.master.is_some() || self.master_stream.is_some() {
            return false;
        }

        true
    }

    pub fn init(mut args: Args) -> Self {
        let mut port = "6379".to_string();
        let mut master: Option<(String, String)> = None;
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
                "--dir" => dir_path = args.next().unwrap().to_string(),
                "--dbfilename" => dbfilename = args.next().unwrap().to_string(),

                "--replicaof" => {
                    if let Some(host_port) = args.next() {
                        let mut parts = host_port.splitn(2, ' ');
                        if let (Some(host), Some(port_str)) = (parts.next(), parts.next()) {
                            // let s = Some(sync_with_master(host, port_str, &port, &dbfilename));
                            // master_stream = s;
                            master = Some((host.to_string(), port_str.to_string()));
                        }
                    }
                }
                _ => {}
            }
        }

        RedisGlobal {
            port,
            master,
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

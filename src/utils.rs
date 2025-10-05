use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;

use crate::types::RedisGlobalType;

pub fn write_simple_string(stream: &mut TcpStream, msg: &str) {
    print!("RESP IS SIMPLE STRING");

    let _ = stream.write_all(format!("+{}\r\n", msg).as_bytes());
}

pub fn write_error(stream: &mut TcpStream, msg: &str) {
    let _ = stream.write_all(format!("-ERR {}\r\n", msg).as_bytes());
}

pub fn write_bulk_string(stream: &mut TcpStream, msg: &str) {
    print!("RESP IS BULK STRING");

    let resp = format!("${}\r\n{}\r\n", msg.len(), msg);
    let _ = stream.write_all(resp.as_bytes());
}

pub fn write_null_bulk_string(stream: &mut TcpStream) {
    let _ = stream.write_all(b"$-1\r\n");
}

pub fn write_integer(stream: &mut TcpStream, val: i64) {
    let resp = format!(":{}\r\n", val);
    let _ = stream.write_all(resp.as_bytes());
}

pub fn write_redis_file(stream: &mut TcpStream, file_name: &str) {
    const EMPTY_RDB: &[u8] = &[
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69,
        0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65,
        0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69,
        0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d,
        0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61,
        0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
    ];

    let mut file = match File::open(file_name) {
        Ok(f) => f,
        Err(_) => {
            // If the file does not exist, use the embedded empty RDB content.
            let file_len = EMPTY_RDB.len();
            let mut resp = format!("${}\r\n", file_len).into_bytes();
            resp.extend_from_slice(EMPTY_RDB);
            let _ = stream.write_all(&resp);
            return;
        }
    };

    let file_len = match file.metadata() {
        Ok(meta) => meta.len(),
        Err(_) => {
            let _ = stream.write_all(b"-ERR could not stat file\r\n");
            return;
        }
    };

    let mut content = Vec::with_capacity(file_len as usize);
    if let Err(_) = file.read_to_end(&mut content) {
        let _ = stream.write_all(b"-ERR could not read file\r\n");
        return;
    }
    let mut resp = format!("${}\r\n", file_len).into_bytes();
    resp.extend_from_slice(&content);
    let _ = stream.write_all(&resp);
}

pub fn write_array<T: AsRef<str>>(stream: &mut TcpStream, items: &[Option<T>]) {
    print!("RESP IS ARRAY");
    let _ = stream.write_all(format!("*{}\r\n", items.len()).as_bytes());
    for item in items {
        match item {
            Some(val) => {
                let s = val.as_ref();
                let _ = stream.write_all(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
            }
            None => {
                let _ = stream.write_all(b"$-1\r\n");
            }
        }
    }
}

pub fn is_matched(pattern: &str, word: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }
    if pattern == "*" {
        return true;
    }
    if let Some(idx) = pattern.find('*') {
        let (prefix, suffix) = pattern.split_at(idx);
        let suffix = &suffix[1..]; // skip the '*'
        if prefix.is_empty() {
            // Pattern starts with '*'
            return word.ends_with(suffix);
        } else if suffix.is_empty() {
            // Pattern ends with '*'
            return word.starts_with(prefix);
        } else {
            // Pattern is like "foo*bar"
            return word.starts_with(prefix)
                && word.ends_with(suffix)
                && word.len() >= prefix.len() + suffix.len();
        }
    }
    pattern == word
}

pub fn parse_len(bytes: &[u8]) -> (usize, usize) {
    let first_byte = bytes[0];
    let msb2 = (first_byte & 0b1100_0000) >> 6;

    match msb2 {
        0b00 => {
            // 6 bit length
            let len = (first_byte & 0b0011_1111) as usize;
            (len, 1)
        }
        0b01 => {
            // 14 bit length
            let second_byte = bytes[1];
            let len = (((first_byte & 0b0011_1111) as usize) << 8) | (second_byte as usize);
            (len, 2)
        }
        0b10 => {
            // 32 bit length
            let len = u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize;
            (len, 5)
        }
        _ => {
            panic!("Invalid length encoding in parse_len");
        }
    }
}

pub fn parse_string(bytes: &[u8]) -> (String, usize) {
    let first_byte = bytes[0];
    let msb2 = (first_byte & 0b1100_0000) >> 6;

    match msb2 {
        0b00 | 0b01 | 0b10 => {
            let (len, offset) = parse_len(bytes);
            let s = String::from_utf8_lossy(&bytes[offset..offset + len]).to_string();
            (s, offset + len)
        }
        0b11 => {
            let format = first_byte & 0b0011_1111;
            match format {
                0 => {
                    let int_val = bytes[1] as i8;
                    (int_val.to_string(), 2)
                }
                1 => {
                    let int_val = i16::from_be_bytes([bytes[1], bytes[2]]);
                    (int_val.to_string(), 3)
                }
                2 => {
                    let int_val = i32::from_be_bytes(bytes[1..5].try_into().unwrap());
                    (int_val.to_string(), 5)
                }
                _ => {
                    panic!("Unknown special string encoding: {}", format);
                }
            }
        }
        _ => unreachable!(),
    }
}

pub fn parse_expiry(bytes: &[u8]) -> Option<(u64, bool, usize)> {
    match bytes[0] {
        0xFD => {
            let ts = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as u64;
            Some((ts * 1000, false, 5))
        }
        0xFC => {
            let ts = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
            Some((ts, true, 9))
        }
        _ => None,
    }
}

pub fn parse_key_value(bytes: &[u8]) -> (String, usize, u8) {
    let value_type = bytes[0];
    let (key, key_used) = parse_string(&bytes[1..]);
    (key, key_used + 1, value_type)
}

pub fn parse_value_by_type(value_type: u8, bytes: &[u8]) -> (String, usize) {
    match value_type {
        0x00 => parse_string(bytes), // String
        // Add more types as needed (e.g., list, set, etc.)
        _ => panic!("Unsupported value type: {:#x}", value_type),
    }
}

pub fn sync_with_master(
    host: &str,
    port_str: &str,
    listening_port: &String,
    dbfilename: &String,
) -> TcpStream {
    let mut stream = TcpStream::connect(format!("{}:{}", host, port_str)).unwrap();

    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
    stream.write_all(ping_cmd).unwrap();
    stream.flush().unwrap();
    println!("hohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohohoho");

    {
        let mut resp = [0u8; 1024];
        let n = stream.read(&mut resp).unwrap();
        if n == 0 {
            panic!("No response from master after PING");
        }
    }

    let port_str = listening_port.to_string();
    let listening_port_len = port_str.len().to_string();
    let replconf_listen = format!(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
        listening_port_len, port_str
    );
    stream.write_all(replconf_listen.as_bytes()).unwrap();
    stream.flush().unwrap();

    {
        let mut resp = [0u8; 1024];
        let n = stream.read(&mut resp).unwrap();
        if n == 0 {
            panic!("No response from master after REPLCONF listening-port");
        }
    }

    let replconf_capa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    stream.write_all(replconf_capa.as_bytes()).unwrap();
    stream.flush().unwrap();

    {
        let mut resp = [0u8; 1024];
        let n = stream.read(&mut resp).unwrap();
        if n == 0 {
            panic!("No response from master after REPLCONF capa");
        }
    }

    let psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    stream.write_all(psync_cmd.as_bytes()).unwrap();
    stream.flush().unwrap();

    loop {
        let mut header = Vec::new();
        while header.len() < 2 || !header.ends_with(b"\r\n") {
            let mut byte = [0u8; 1];
            let n = stream.read(&mut byte).unwrap();
            if n == 0 {
                break;
            }
            header.push(byte[0]);
        }

        if header.starts_with(b"$") {
            // Parse the length
            let header_str = String::from_utf8_lossy(&header);
            if let Some(idx) = header_str.find("\r\n") {
                let len_str = &header_str[1..idx];
                if let Ok(file_len) = len_str.parse::<usize>() {
                    // Read the binary contents of the file
                    let mut file_contents = vec![0u8; file_len];
                    let mut read_total = 0;
                    while read_total < file_len {
                        let n = stream.read(&mut file_contents[read_total..]).unwrap();
                        if n == 0 {
                            break;
                        }
                        read_total += n;
                    }
                    write_to_file(&dbfilename, file_contents).unwrap();
                    break;
                }
            }
        }
    }
    stream
}

pub fn write_to_file(filename: &str, contents: Vec<u8>) -> std::io::Result<()> {
    let mut file = File::create(filename)?;
    file.write_all(&contents)?;
    eprintln!("file {filename} has been saved!");
    Ok(())
}

pub fn propagate_slaves(global_state: &RedisGlobalType, message: &str) {
    let msg = message.to_string();

    let mut global_guard = global_state.lock().unwrap();
    global_guard.offset_replica_sync += num_bytes(&msg);

    for replica in global_guard.replica_states.values() {
        // Send message to replica’s channel
        if let Err(e) = replica.sender.send(msg.clone()) {
            eprintln!("Failed to queue message for replica: {:?}", e);
        }
    }
}

pub fn offset_difference(master_offset: usize, replica_offset: usize) -> usize {
    master_offset - replica_offset
}

pub fn num_bytes(s: &str) -> usize {
    s.as_bytes().len()
}

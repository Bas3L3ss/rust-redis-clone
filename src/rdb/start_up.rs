use memmap2::Mmap;
use std::fs::File;

use crate::{
    enums::val_type::ValueType,
    rdb::structs::header_metadata::HeaderMetadata,
    structs::config::Config,
    types::{DbConfigType, DbType, RedisGlobalType},
    utils::{parse_expiry, parse_key_value, parse_len, parse_value_by_type},
};

pub fn start_up(db: DbType, db_config: DbConfigType, global_state: RedisGlobalType) {
    let global = global_state.lock().unwrap();
    let db_path = format!("{}/{}", global.dir_path, global.dbfilename);
    let file = match File::open(&db_path) {
        Ok(f) => f,
        Err(_) => return,
    };

    // Memory-map the file for efficient access
    let file_map: Mmap = unsafe { Mmap::map(&file).expect("Failed to mmap dump.rdb") };

    // Parse the header metadata and get the initial offset
    let (_header_metadata, mut offset) = HeaderMetadata::from_bytes(&file_map[..]);

    if file_map.get(offset) == Some(&0xFF) {
        return;
    }

    loop {
        // Database indicator
        match file_map.get(offset) {
            Some(&0xFE) => offset += 1,
            _ => panic!("Expected database indicator (0xFE)"),
        }

        // Database number (skip)
        offset += 1;

        // Resizedb field indicator
        match file_map.get(offset) {
            Some(&0xFB) => offset += 1,
            _ => panic!("Expected resizedb field indicator (0xFB)"),
        }

        // Hash table size
        let (ht_size, used1) = parse_len(&file_map[offset..]);
        offset += used1;

        // Hash table expires size (skip)
        let (_ht_expires_size, used2) = parse_len(&file_map[offset..]);
        offset += used2;

        let mut local_offset = offset;

        for _ in 0..ht_size {
            let (expiry, is_millis, exp_used) = match parse_expiry(&file_map[local_offset..]) {
                Some((exp, millis, used)) => (Some(exp), millis, used),
                None => (None, false, 0),
            };
            local_offset += exp_used;

            // Parse key and value
            let (key, key_used, value_type) = parse_key_value(&file_map[local_offset..]);
            local_offset += key_used;

            let (value, value_used) = parse_value_by_type(value_type, &file_map[local_offset..]);
            local_offset += value_used;

            // Insert into DB
            {
                let mut db_guard = db.lock().unwrap();
                db_guard.insert(key.clone(), ValueType::String(value));
            }

            // Insert config (expiry)
            let mut config = Config::default();
            if let Some(expiry_ts) = expiry {
                let expire_at = if is_millis {
                    expiry_ts
                } else {
                    // Convert seconds to milliseconds
                    expiry_ts * 1000
                };
                config.expire_at = Some(expire_at);
            }
            {
                let mut config_guard = db_config.lock().unwrap();
                config_guard.insert(key, config);
            }
        }

        offset = local_offset;

        if file_map.get(offset) == Some(&0xFF) {
            break;
        }
    }
}

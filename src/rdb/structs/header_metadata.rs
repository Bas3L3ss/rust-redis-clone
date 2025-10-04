use std::collections::HashMap;

use crate::utils::parse_string;

#[derive(Debug)]
pub struct HeaderMetadata {
    pub magic_string: String,
    pub version_number_string: String,
    pub metadata_map: HashMap<String, String>,
}

impl HeaderMetadata {
    pub fn from_bytes(bytes: &[u8]) -> (Self, usize) {
        let magic_string = String::from_utf8_lossy(&bytes[0..5]).to_string();
        let version_number_string = String::from_utf8_lossy(&bytes[5..9]).to_string();
        let mut metadata_map = HashMap::new();

        let mut idx = 9;

        while bytes[idx] != 0xFE && bytes[idx] != 0xFF {
            if bytes[idx] == 0xFA {
                idx += 1
            }

            let (key, offset) = parse_string(&bytes[idx..]);
            idx += offset;
            let (value, offset) = parse_string(&bytes[idx..]);
            idx += offset;
            metadata_map.insert(key, value);
        }

        (
            HeaderMetadata {
                magic_string,
                version_number_string,
                metadata_map,
            },
            idx,
        )
    }
}

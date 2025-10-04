#[derive(Debug)]
pub struct Request {
    pub args: Vec<String>,
}

impl Request {
    pub fn new_from_buffer(buffer: &[u8]) -> Self {
        let buffer_string = String::from_utf8_lossy(buffer);
        let cleaned = buffer_string.trim_matches(char::from(0));

        let mut args = Vec::new();
        let mut lines = cleaned.split("\r\n");

        while let Some(token) = lines.next() {
            if token.starts_with('*') {
                continue; // array length
            }
            if token.starts_with('$') {
                if let Some(str) = lines.next() {
                    args.push(str.to_string());
                }
            }
        }

        Request { args }
    }
}

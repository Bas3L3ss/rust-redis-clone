#[derive(Debug)]
pub struct Request {
    pub args: Vec<String>,
}

impl Request {
    pub fn try_parse(buffer: &[u8]) -> Option<(Self, usize)> {
        let buffer_string = String::from_utf8_lossy(buffer);

        let mut lines = buffer_string.split("\r\n").peekable();

        let array_header = lines.next()?;
        if !array_header.starts_with('*') {
            return None;
        }
        let num_args: usize = array_header[1..].parse().ok()?;

        let mut args = Vec::with_capacity(num_args);
        let mut consumed_bytes = array_header.len() + 2; // +2 for \r\n

        for _ in 0..num_args {
            let len_line = lines.next()?;
            consumed_bytes += len_line.len() + 2;

            if !len_line.starts_with('$') {
                return None;
            }
            let len: usize = len_line[1..].parse().ok()?;

            let value = lines.next()?;
            consumed_bytes += value.len() + 2;

            if value.len() < len {
                return None;
            }

            args.push(value.to_string());
        }

        Some((Request { args }, consumed_bytes))
    }
}

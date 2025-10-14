#[derive(Debug)]
pub struct XreadConfig {
    pub count: Option<usize>,
    pub block: Option<usize>,
    pub streams: Vec<(String, String)>,
}

impl XreadConfig {
    pub fn from_args(args: &[String]) -> (Self, usize, Option<String>) {
        let mut count = None;
        let mut block = None;
        let mut streams: Vec<(String, String)> = Vec::new();

        let mut i = 0;
        let mut err: Option<String> = None;
        let mut found_streams = false;

        while i < args.len() {
            let arg = args[i].to_ascii_lowercase();
            match arg.as_str() {
                "count" => {
                    if i + 1 >= args.len() {
                        err = Some("COUNT requires an argument".to_string());
                        break;
                    }
                    match args[i + 1].parse::<usize>() {
                        Ok(n) => count = Some(n),
                        Err(_) => {
                            err = Some("COUNT must be an integer".to_string());
                            break;
                        }
                    }
                    i += 2;
                }
                "block" => {
                    if i + 1 >= args.len() {
                        err = Some("BLOCK requires an argument".to_string());
                        break;
                    }
                    match args[i + 1].parse::<usize>() {
                        Ok(n) => block = Some(n),
                        Err(_) => {
                            err = Some("BLOCK must be an integer".to_string());
                            break;
                        }
                    }
                    i += 2;
                }
                "streams" => {
                    found_streams = true;
                    i += 1;

                    let remaining = args.len() - i;
                    if remaining == 0 || remaining % 2 != 0 {
                        err = Some("Missing ids for keys".to_string());
                        break;
                    }

                    let mid = i + remaining / 2;
                    for j in 0..(remaining / 2) {
                        let key = args[i + j].clone();
                        let id = args[mid + j].clone();
                        streams.push((key, id));
                    }

                    i += remaining;
                    break;
                }
                _ => {
                    err = Some(format!("Unknown or misplaced argument: {}", args[i]));
                    break;
                }
            }
        }

        if !found_streams {
            err = Some("Missing STREAMS argument".to_string());
        }

        let consumed = if let Some(_e) = &err { i.max(1) } else { i };

        (
            XreadConfig {
                count,
                block,
                streams,
            },
            consumed,
            err,
        )
    }
}

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
                    let mut stream_keys: Vec<String> = Vec::new();
                    while i < args.len()
                        && !["count", "block"].contains(&args[i].to_ascii_lowercase().as_str())
                    {
                        if args[i].to_ascii_lowercase() == "streams" {
                            break;
                        }

                        stream_keys.push(args[i].clone());
                        i += 1;
                    }
                    let ids_start = i;
                    let ids_num = stream_keys.len();
                    if ids_num == 0 || args.len() < ids_start + ids_num {
                        err = Some(
                            "STREAMS must be followed by keys and their corresponding IDs"
                                .to_string(),
                        );
                        break;
                    }
                    for (ix, key) in stream_keys.into_iter().enumerate() {
                        let id_idx = ids_start + ix;
                        if id_idx >= args.len() {
                            err = Some("Missing ID for stream".to_string());
                            break;
                        }
                        let id = args[id_idx].clone();
                        streams.push((key, id));
                    }
                    i = ids_start + ids_num;
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

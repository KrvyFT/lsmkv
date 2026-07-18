use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::env;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[path = "../protocol.rs"]
mod protocol;

use protocol::{Request, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("help:");
        eprintln!("  {} put <key> <value>", args[0]);
        eprintln!("  {} get <key>", args[0]);
        eprintln!("  {} del <key>", args[0]);
        std::process::exit(1);
    }

    let cmd = args[1].to_lowercase();
    let req = match cmd.as_str() {
        "put" => {
            if args.len() < 4 {
                eprintln!("error: put require key and value");
                std::process::exit(1);
            }
            Request::Put {
                key: args[2].as_bytes().to_vec(),
                value: args[3].as_bytes().to_vec(),
            }
        }
        "get" => {
            if args.len() < 3 {
                eprintln!("error: get require key");
                std::process::exit(1);
            }
            Request::Get {
                key: args[2].as_bytes().to_vec(),
            }
        }
        "del" => {
            if args.len() < 3 {
                eprintln!("error: del require key");
                std::process::exit(1);
            }
            Request::Delete {
                key: args[2].as_bytes().to_vec(),
            }
        }
        _ => {
            eprintln!("unknown command: {}", cmd);
            std::process::exit(1);
        }
    };

    let addr = "127.0.0.1:8080";
    let stream = match TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connect failed: {}. Ensure server is running.", e);
            std::process::exit(1);
        }
    };

    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    let req_bytes = bincode::serialize(&req)?;
    if let Err(e) = framed.send(Bytes::from(req_bytes)).await {
        eprintln!("send failed: {}", e);
        std::process::exit(1);
    }

    match framed.next().await {
        Some(Ok(resp_bytes)) => {
            let resp: Response = bincode::deserialize(&resp_bytes)?;
            match resp {
                Response::Ok(Some(v)) => println!(
                    "\"{}\"",
                    String::from_utf8(v).unwrap_or_else(|_| "<invalid utf-8>".to_string())
                ),
                Response::Ok(None) => {
                    if cmd == "put" {
                        println!("put {} finish", args[2]);
                    } else if cmd == "del" {
                        println!("del {} finish", args[2]);
                    } else {
                        println!("(nil)");
                    }
                }
                Response::Err(e) => {
                    eprintln!("(error) {}", e);
                    std::process::exit(1)
                }
            }
        }
        Some(Err(e)) => {
            eprintln!("receive failed: {}", e);
            std::process::exit(1);
        }
        None => {
            eprintln!("server disconnected");
            std::process::exit(1);
        }
    }

    Ok(())
}

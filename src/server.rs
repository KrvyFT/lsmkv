use std::sync::Arc;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::db_kernel::{DbKernel, WriteBatch, WriteOP};
use crate::error::Result;
use crate::protocol::{Request, Response};

type Db = Arc<Mutex<DbKernel>>;

/// The message sent from the TCP client handler to the Group Commit writer task.
pub struct WriteMessage {
    pub op: WriteOP,
    pub responder: oneshot::Sender<std::result::Result<(), String>>,
}

pub struct Server<'a> {
    addr: &'a str,
    db: Db,
}

impl<'a> Server<'a> {
    pub fn new(addr: &'a str, db: Db) -> Self {
        Self { addr, db }
    }

    pub async fn run(&'a self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        println!("LSM-Tree TCP Server listening on {}", self.addr);

        // 建立 Writer Channel，缓冲容量设为 10000
        let (write_tx, write_rx) = mpsc::channel::<WriteMessage>(10000);

        // 启动专属的后台 Writer Task 进行组提交 (Group Commit)
        let db_writer = Arc::clone(&self.db);
        tokio::spawn(async move {
            Self::writer_task(db_writer, write_rx).await;
        });

        loop {
            let (stream, _) = listener.accept().await?;
            let db_clone = Arc::clone(&self.db);
            let write_tx_clone = write_tx.clone();

            tokio::spawn(async move {
                let err = Self::handle_client(stream, db_clone, write_tx_clone).await;

                if let Err(e) = err {
                    eprintln!("Error handling client: {}", e);
                }
            });
        }
    }

    /// 后台组提交任务：接收队列里的请求并一把梭哈
    async fn writer_task(db: Db, mut rx: mpsc::Receiver<WriteMessage>) {
        while let Some(first_msg) = rx.recv().await {
            let mut ops = vec![first_msg.op];
            let mut responders = vec![first_msg.responder];

            // 尝试拿尽当前队列里所有积压的请求，最高打成 10000 个一批
            while let Ok(msg) = rx.try_recv() {
                ops.push(msg.op);
                responders.push(msg.responder);
                if ops.len() >= 10000 {
                    break;
                }
            }

            // 获取一次独占写锁
            let res = {
                let mut kernel = db.lock().await;
                let batch = WriteBatch { ops };
                if let Err(e) = kernel.write(&batch) {
                    Err(e.to_string())
                } else {
                    Ok(())
                }
            };

            // 统一回复所有等待的客户端
            for responder in responders {
                let _ = responder.send(res.clone());
            }
        }
    }

    pub async fn handle_client(
        stream: TcpStream, 
        db: Db, 
        write_tx: mpsc::Sender<WriteMessage>
    ) -> Result<()> {
        let mut frame = Framed::new(stream, LengthDelimitedCodec::new());

        while let Some(result) = frame.next().await {
            match result {
                Ok(bytes) => {
                    let req: Request = match bincode::deserialize(&bytes) {
                        Ok(r) => r,
                        Err(e) => {
                            let res = Response::Err(format!("Invalid request format: {}", e));
                            let res_bytes = bincode::serialize(&res)?;
                            frame.send(Bytes::from(res_bytes)).await?;
                            continue;
                        }
                    };

                    let response = match req {
                        Request::Get { key } => {
                            // 由于使用了 Mutex，读写依然互斥，但组提交大大减少了写锁的占用时间
                            let kernel = db.lock().await;
                            match kernel.get(&key) {
                                Ok(v) => Response::Ok(Some(v)),
                                Err(e) => Response::Err(e.to_string()),
                            }
                        }
                        Request::Put { key, value } => {
                            let (tx, rx) = oneshot::channel();
                            let msg = WriteMessage {
                                op: WriteOP::Put(key, value),
                                responder: tx,
                            };
                            if write_tx.send(msg).await.is_err() {
                                Response::Err("Server shutting down".to_string())
                            } else {
                                match rx.await {
                                    Ok(Ok(_)) => Response::Ok(None),
                                    Ok(Err(e)) => Response::Err(e),
                                    Err(_) => Response::Err("Writer task dropped".to_string()),
                                }
                            }
                        }
                        Request::Delete { key } => {
                            let (tx, rx) = oneshot::channel();
                            let msg = WriteMessage {
                                op: WriteOP::Delete(key),
                                responder: tx,
                            };
                            if write_tx.send(msg).await.is_err() {
                                Response::Err("Server shutting down".to_string())
                            } else {
                                match rx.await {
                                    Ok(Ok(_)) => Response::Ok(None),
                                    Ok(Err(e)) => Response::Err(e),
                                    Err(_) => Response::Err("Writer task dropped".to_string()),
                                }
                            }
                        }
                    };
                    
                    let res_bytes = bincode::serialize(&response)?;
                    frame.send(Bytes::from(res_bytes)).await?;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

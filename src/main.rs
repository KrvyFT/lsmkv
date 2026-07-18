use crate::flush::Flusher;
use crate::{db_kernel::DbKernel, server::Server};
use std::sync::{Arc, mpsc};

mod db_kernel;
mod error;
mod flush;
mod memtable;
mod model;
mod protocol;
mod server;
mod sstable;
mod wal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = "./data";

    let (flush_tx, task_rx) = mpsc::channel();
    let (result_tx, flush_rx) = mpsc::channel();

    let flusher = Flusher::new(result_tx, task_rx, dir, 4);
    flusher.spawn();

    let db = DbKernel::new(flush_tx, flush_rx, dir)?;
    
    // 恢复使用 Arc<Mutex> 包裹 DbKernel。
    // 因为 DbKernel 内部含有 mpsc::Receiver (不满足 Sync 特征)，无法使用 RwLock。
    // 但有了组提交(Group Commit)，Mutex 的抢占开销已经被降至最低！
    let shared_db = Arc::new(tokio::sync::Mutex::new(db));

    let server = Server::new("127.0.0.1:8080", shared_db);
    server.run().await?;

    Ok(())
}

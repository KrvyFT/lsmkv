use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crate::db_kernel::DbKernel;
use crate::flush::Flusher;

mod db_kernel;
mod error;
mod flush;
mod memtable;
mod model;
mod sstable;
mod wal;

fn main() {
    let dir = "./data_test";
    let _ = std::fs::remove_dir_all(dir); // 确保环境干净

    // table adn path
    let (flush_tx, task_rx) = mpsc::channel();
    // task adn new sstable
    let (result_tx, flush_rx) = mpsc::channel();

    // 启动后台 Flusher，池大小设置为 4
    let flusher = Flusher::new(result_tx, task_rx, dir, 4);
    flusher.spawn();

    // 启动 DbKernel
    let mut db = DbKernel::new(flush_tx, flush_rx, dir).unwrap();

    println!("--- Testing basic Put/Get ---");
    db.put(b"key1".to_vec(), b"value1".to_vec()).unwrap();
    assert_eq!(db.get(&b"key1".to_vec()).unwrap(), b"value1".to_vec());

    println!("--- Testing Delete ---");
    db.delete(b"key1".to_vec()).unwrap();
    assert!(db.get(&b"key1".to_vec()).is_err());

    println!("--- Testing large writes to trigger Flush (This will take a moment) ---");
    // 写足够多的数据，突破 4MB 触发至少一次 MemTable Flush
    for i in 0..50000 {
        let key = format!("key_{:06}", i).into_bytes();
        let value = vec![0u8; 100]; // 100 bytes
        db.put(key, value).unwrap();
    }

    println!("--- Waiting for background flush to complete ---");
    // 等待后台 lake 线程池完成刷盘
    thread::sleep(Duration::from_secs(2));

    // 触发一次写操作，借机调用 try_sync_flush_results 收取结果并挂载 SSTable
    db.put(b"trigger_sync".to_vec(), b"sync".to_vec()).unwrap();

    println!("--- Verifying read from SSTable ---");
    // 这些旧的 Key 现在应该已经落入 SSTable 了，测试是否能正确从底层读取
    let test_key1 = b"key_000000".to_vec();
    let val1 = db.get(&test_key1).unwrap();
    assert_eq!(val1.len(), 100);

    let test_key2 = b"key_049999".to_vec();
    let val2 = db.get(&test_key2).unwrap();
    assert_eq!(val2.len(), 100);

    println!(
        "E2E test completed successfully! Data successfully routed through MemTable -> ImmMemTable -> SSTable!"
    );
}

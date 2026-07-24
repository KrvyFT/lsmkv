# LSM-KV 🚀

LSM-KV is a high-performance, persistent Key-Value Store library written entirely in Rust. Its core is built on the **LSM-Tree (Log-Structured Merge-Tree)** architecture, specifically designed for extremely high write throughput and strong consistency.

## ✨ Core Features

- **Extreme Write Performance (Group Commit)**: Adopts an industry-standard "Group Commit" architecture. Thousands of concurrent write requests from background Tokio tasks are automatically grouped and merged, requiring only a single physical disk flush (`fdatasync`), completely breaking through disk I/O bottlenecks.
- **Reliable Data Persistence (WAL)**: All write operations are first appended to a Write-Ahead Log (WAL). Even if the application crashes unexpectedly, all data can be flawlessly recovered upon restart.
- **Non-blocking Background Flush**: When the in-memory data (MemTable) reaches the 4MB threshold, it automatically turns read-only and is handed over to a dedicated Tokio `spawn_blocking` task to asynchronously generate SSTable disk files, without blocking foreground requests.
- **Zero-copy Fast Reads (mmap)**: Reading from the underlying SSTable disk files directly uses memory mapping (`memmap2`), mapping file pages directly into system memory, leaving page cache scheduling entirely to the operating system.
- **Thread-safe Tokio Integration**: Fully async and `Send` + `Sync` ready. The `LsmKv` instance can be cheaply cloned (`Arc` internally) and shared across millions of Tokio tasks.

## 🏗️ Architecture Overview

```text
 Task 1 \                                                   /--> SSTable (.sst)
 Task 2 ----(put.await)---> LsmKv (Group Commit Writer) ---|
 Task 3 /                       |                           \--> SSTable (.sst)
                                v
                          [ MemTable ]  ----> [ Immutable MemTable ] (Background Flush)
                                |
                          [ WAL (.log) ] (Sequential Disk Write, Crash Safe)
```

### Core Modules

- `lib.rs`: The core brain of the LSM engine, providing the thread-safe `LsmKv` API.
- `wal.rs`: Write-Ahead Log ensuring durability using `tokio::fs`.
- `memtable.rs`: High-speed in-memory structure based on `BTreeMap`.
- `sstable.rs`: Underlying data persistence format, including data blocks, index blocks, and footer validation.
- `flush.rs`: Asynchronous disk flush scheduler utilizing `tokio::task::spawn_blocking`.

## 🚀 Quick Start

### 1. Add Dependency

Add `lsmkv` and `tokio` to your `Cargo.toml`:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
lsmkv = { path = "path/to/lsmkv" }
```

### 2. Usage Example

Due to intensive CPU serialization and system calls, it is **highly recommended to run in Release mode** for maximum performance:

```rust
use lsmkv::LsmKv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize and open the database (auto-recovers from WAL)
    let db = LsmKv::open("./data").await?;

    // 2. Write data (Group Commit enabled by default)
    db.put(b"hello".to_vec(), b"world".to_vec()).await?;

    // 3. Read data (Non-blocking, Lock-free read)
    if let Ok(value) = db.get(&b"hello".to_vec()) {
        println!("Value: {}", String::from_utf8_lossy(&value));
    }

    // 4. Delete data (Tombstone mechanism)
    db.delete(b"hello".to_vec()).await?;

    Ok(())
}
```

### 3. Highly Concurrent Writes

Because `LsmKv` is built with a sophisticated Actor model internally, you can effortlessly spawn thousands of Tokio tasks to bombard the database without explicitly locking anything:

```rust
let mut tasks = vec![];
for i in 0..10_000 {
    let db_clone = db.clone(); 
    let task = tokio::spawn(async move {
        db_clone.put(format!("key_{}", i).into_bytes(), vec![0; 100]).await.unwrap();
    });
    tasks.push(task);
}

for task in tasks {
    task.await.unwrap();
}
```

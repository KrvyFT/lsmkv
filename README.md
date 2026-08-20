# LSM-KV 🚀

LSM-KV is a high-performance, async-first Key-Value storage engine written entirely in **Rust**. It is built on the **Log-Structured Merge-Tree (LSM-Tree)** architecture.

Designed for extremely high-concurrency read and write scenarios, it strives to squeeze out maximum I/O performance on a single node through **Zero-cost Abstractions**, **Group Commit**, and **Zero-copy mmap reads**.

---

## ✨ Core Features

* **Extreme Write Performance (Group Commit)**: Adopts an industry-standard "Group Commit" architecture. Massive incoming write requests from background `tokio` tasks are automatically aggregated, requiring only a single physical disk flush (`fsync`) to complete the entire batch of transactions, effectively shattering disk I/O bottlenecks.
* **Zero-cost Abstraction**: Throughout the entire write lifecycle (from network/app layer ingress to building `LogRecord`s, and inserting into the `MemTable`'s BTreeMap), the system achieves **zero heap allocation copying**. By strictly transferring ownership of key-value pairs, it avoids catastrophic $O(N)$ performance degradation.
* **Reliable Data Persistence (WAL + Fail-Fast)**: All operations are appended to a Write-Ahead Log (WAL) before execution. If the underlying WAL encounters an I/O anomaly (e.g., disk full, permission denied), the system immediately triggers a circuit breaker (Panic/Fail-Fast) to prevent half-written states from corrupting memory data, guaranteeing 100% data consistency. Data perfectly recovers from the log upon restart.
* **Non-blocking Background Flush**: When the in-memory data pool (`MemTable`) reaches a configurable threshold, it is marked as read-only (Immutable) and handed over to an independent `tokio::task::spawn_blocking` thread pool to asynchronously construct SSTables, without blocking foreground requests at all.
* **Zero-copy Fast Reads (mmap)**: Reading persisted SSTable data leverages `memmap2` for memory mapping. Combined with a Sparse Index, Zstd block decompression, and binary search, page fault overhead is minimized.
* **Thread-safe Tokio Integration**: Built on a pure async Actor communication model. The `LsmKv` instance encapsulates fine-grained read-write locks and atomic reference counting (`Arc`), allowing it to be cheaply cloned and shared across tens of thousands of concurrent Tokio tasks.

---

## 🏗️ Architecture Overview

```text
 Client Tasks                                                Background Thread Pool
-------------                                                ----------------------
 Task 1 \                                                   /--> SSTable_1 (.sst) (Zstd Compressed)
 Task 2 ----(put.await)---> LsmKv (Group Commit) ----------|
 Task 3 /                       |                           \--> SSTable_2 (.sst)
                                v
                          [ MemTable ]  ----> [ Immutable MemTable ] -> (Flush Task)
                        (BTreeMap Node)
                                |
                          [ WAL (.log) ] (Sequential Disk Write, Fail-Fast Guaranteed)
```

---

## 🚀 Quick Start

### 1. Add Dependency

Add `lsmkv` and `tokio` to your `Cargo.toml`:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
lsmkv = { path = "path/to/lsmkv" }
```

### 2. Basic Usage & Highly Concurrent Writes

Since the database involves intensive CPU serialization, compression, and system calls, it is **strongly recommended to compile and run in Release mode** to experience its true performance:

```rust
use lsmkv::LsmKv;
use lsmkv::options::DbOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the database (Builder pattern recommended)
    let options = DbOptions::builder()
        .dir("./lsmkv_data")
        .build();
        
    let db = LsmKv::open(options).await?;

    // 2. Extremely high concurrent writes (Group Commit aggregates them automatically)
    let mut tasks = vec![];
    for i in 0..10_000 {
        let db_clone = db.clone(); 
        let task = tokio::spawn(async move {
            db_clone.put(format!("key_{}", i).into_bytes(), vec![0; 100]).await.unwrap();
        });
        tasks.push(task);
    }
    for task in tasks { task.await.unwrap(); }

    // 3. Read data (Lock-free concurrent reads)
    if let Ok(value) = db.get(&b"key_9999".to_vec()) {
        println!("Value length: {}", value.len());
    }

    // 4. Delete data (Tombstone mechanism)
    db.delete(b"key_9999".to_vec()).await?;

    Ok(())
}
```

---

## ⚙️ Advanced Tuning

LSM-KV provides a rich low-level hardware tuning interface via `DbOptionsBuilder`. You can customize configurations for different storage mediums (NVMe SSD or HDD) and memory constraints:

```rust
use lsmkv::options::DbOptions;
use lsmkv::sstable::sstable_builder::CompressionType;

let options = DbOptions::builder()
    .dir("/data/nvme_storage")
    // Max MemTable size: Larger size reduces write amplification but uses more RAM (Default: 4MB)
    .mem_table_max_size(16 * 1024 * 1024)
    // Group Commit trigger threshold: Max bytes per batch (Default: 1MB)
    .max_batch_bytes(2 * 1024 * 1024)
    // SSTable target block size: 4KB recommended for NVMe (Default: 4096)
    .sstable_block_size(4096)
    // Block compression algorithm: Zstd provides ultra-high compression ratios (Default: Zstd)
    .compression_type(CompressionType::Zstd)
    // Compression level (Default: 3)
    .compression_level(3)
    .build();
    
let db = LsmKv::open(options).await.unwrap();
```

---

## 📂 Core Modules Structure

* `lib.rs`: Exposes the thread-safe `LsmKv` API; manages the core read/write lifecycle and the Group Commit scheduler.
* `options.rs`: The `DbOptions` and `DbOptionsBuilder` configuration models.
* `wal.rs`: Write-Ahead Log ensuring disaster recovery safety via `tokio::fs` and strict `sync_all` guarantees.
* `memtable.rs`: High-speed memory layer. Uses `BTreeMap` to achieve optimal CPU Cache Locality compared to naive SkipLists.
* `sstable.rs`: Underlying persistence format. Handles block sharding, Zstd compression, Sparse Index extraction, and `mmap`-based zero-copy file mapping for reads.
* `flush.rs`: Independent, asynchronous disk flush task pool that never blocks the main thread.

---

## 🗺️ Roadmap

* [ ] Background Compaction Mechanism (Merge and reclaim expired data and Tombstones in SSTables).
* [ ] Add support for the ultra-fast `Snappy` compression algorithm.
* [ ] Mmap Read Cache (Block Cache) or LRU cache to further optimize read performance.
* [ ] MVCC (Multi-Version Concurrency Control) and Timestamp support.

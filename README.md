# LSM-KV 🚀

LSM-KV 是一个完全由 Rust 编写的高性能、持久化键值存储系统（Key-Value Store）。其底层基于 **LSM-Tree (Log-Structured Merge-Tree)** 架构构建，专为极高的写入吞吐量和强一致性设计。

## ✨ 核心特性

- **极致写入性能 (Group Commit)**：采用业界标准的“组提交”架构。上万个并发写请求会被后台 Tokio 协程自动打包合并，只需一次物理磁盘刷盘（`fdatasync`），彻底打破磁盘 I/O 瓶颈。
- **可靠的数据持久化 (WAL)**：所有的写操作会优先追加写入预写日志（Write-Ahead Log），即使服务器突然断电，也能在重启时毫发无损地恢复所有数据。
- **无阻塞后台刷盘**：内存数据（MemTable）触达 4MB 阈值后，会自动转为只读并移交至自定义的后台线程池 (`lake::ThreadPool`) 异步生成 SSTable 磁盘文件，完全不阻塞前台请求。
- **零拷贝极速读取 (mmap)**：对底层磁盘 SSTable 文件的读取直接使用了内存映射（`memmap2`），将文件页直接映射入系统内存，由操作系统全权负责缺页缓存调度。
- **轻量级 TCP 异步网络层**：基于 `Tokio` 和 `tokio-util` 实现了基于帧（Length-Delimited）的二进制协议服务器，支持极高并发的长连接。

## 🏗️ 架构概览

```text
 Client 1 \                                                   /--> SSTable (.sst)
 Client 2 --(TCP/Bincode)--> Server (Group Commit Writer) ---|
 Client 3 /                       |                           \--> SSTable (.sst)
                                  v
                            [ MemTable ]  ----> [ Immutable MemTable ] (后台落盘)
                                  |
                            [ WAL (.log) ] (顺序写磁盘，保证断电不丢)
```

### 核心模块

- `db_kernel.rs`：LSM 引擎的核心大脑，协调内存、磁盘与后台任务。
- `server.rs`：网络层，实现了读写锁分离和 **Group Commit（组提交）** 协程池。
- `wal.rs`：保证持久性的追加日志。
- `memtable.rs`：基于 `BTreeMap` 的极速内存结构。
- `sstable.rs`：底层数据持久化格式，包含数据区、索引区、Footer 校验。
- `flush.rs`：对接线程池的异步落盘调度器。
- `lake/`：项目中手写的底层线程池实现。

## 🚀 快速开始

### 1. 启动服务端

由于底层存在密集的 CPU 序列化和系统调用，**强烈建议在 Release 模式下运行**以获得最高性能：

```bash
cargo run --release --bin lsmkv
```

*服务端将默认在 `127.0.0.1:8080` 启动，并在当前目录的 `./data` 文件夹下存储数据。*

### 2. 使用 CLI 客户端交互

你可以使用自带的命令行工具与数据库进行交互：

```bash
# 写入数据
cargo run --release --bin cli put my_key "Hello LSM-Tree!"

# 读取数据
cargo run --release --bin cli get my_key

# 删除数据 (Tombstone 机制)
cargo run --release --bin cli del my_key
```

### 3. 运行极致并发压测 (Benchmark)

本项目自带了高强度的异步并发压测工具，可以在不撑爆系统进程池的前提下，瞬间向引擎倾泻数十万条数据：

```bash
cargo run --release --bin benchmark
```

*压测工具默认会建立上千个持久并发长连接，进行十万至百万级的并发写入测试，并实时打印 QPS（每秒吞吐量）报告。*

## 📜 协议说明

服务端采用 `LengthDelimitedCodec` 作为帧边界，内容使用 `Bincode` 进行高效的二进制序列化。如果你需要开发其他语言的客户端，只需发送 4 字节的长度 Header（小端序），后接 Bincode 序列化的 `Request` 结构体即可。

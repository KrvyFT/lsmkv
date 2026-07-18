# LSM-KV 数据库实现任务清单 (Todo List)

请严格按照以下步骤顺序进行开发。每完成一项，可以在前方打钩 `[x]`。

## 阶段一：项目骨架与序列化机制

- [x] **项目初始化**：执行 `cargo init`，并在 `Cargo.toml` 中引入 `thiserror`, `serde`, `bincode`, `memmap2`。
- [x] **基础定义与错误处理**：
  - 在 `src/error.rs` 定义统一的 `DbError` 和 `Result<T>` 枚举。
  - 在 `src/model.rs` 中使用 `bincode` 和 `serde` 定义 `LogRecord`（涵盖 Put 和 Delete）。

## 阶段二：WAL (预写日志) 的读写

- [x] **WAL 写入机制 (`WalWriter`)**：
  - 创建 `src/wal.rs`。
  - 实现基于 `[长度 (4字节)] + [bincode payload]` 的文件流式追加格式。
  - 务必暴露 `sync` 方法来调用 `File::sync_data`。
- [] **WAL 恢复机制 (`WalReader`)**：
  - 实现重启时的数据回放读取。
  - 处理尾部记录突然由于断电截断导致的 `UnexpectedEof`，吞掉错误并返回成功解析的记录。

## 阶段三：内存表与前台读写分离

- [x] **构建 `MemTable`**：
  - 在 `src/memtable.rs` 中包装 `BTreeMap<Key, Value>`。
  - 实现 `put`, `get`, `delete`，同时维护当前内存字节数的 `approx_size` 评估计数。
- [ ] **组合 `DbKernel`**：
  - 在 `src/db.rs` 初始化主体结构，将 `MemTable` 与 `WalWriter` 组合。
  - 跑通完整的前台 API：调用 `put` 时先串行写入日志，再注入内存表。

## 阶段四：SSTable 磁盘序列化与读取

- [x] **SSTable 构建 (`SsTableBuilder`)**：
  - 在 `src/sstable.rs` 中实现 `build`，遍历冻结的内存表 `Iter`。
  - 按顺序序列化 K-V（Data Block）并同时记录该条目的文件起始 Offset 至索引集合。
  - 结束时在文件尾部追写 Index Block（存放 BTreeMap 的序列化）与固定 16 字节的 Footer（Index Offset + 魔法魔数）。
- [ ] **SSTable 反序列化 (`SsTableReader`)**：
  - 编写读取方法：利用 `memmap2` 直接切片磁盘最后 16 字节读出 Footer 定位 Index Block 偏移。
  - 读取并反序列化 Index Block 为内存可用的查找结构。

## 阶段五：组件串联与异步 Flush 后台线程

- [ ] **后台线程 (`mpsc`)**：
  - 在 `DbKernel` 初始化时启动分离线程（`thread::spawn`），并通过 Channel 传递包含 `Arc<MemTable>` 的 `FlushTask` 变体。
- [ ] **容量拦截**：
  - 当检测到 MemTable 的大小超过设定阈值（如 4MB），发生替换。旧表送往后台通道进行 `SsTableBuilder::build`。
- [ ] **优雅关闭 (Graceful Shutdown)**：
  - 为 `DbKernel` 实现 `Drop`，向后台下发 `Shutdown` 毒丸信号并 `join`，确保停止前最后一部分滞留状态被完全落盘。

```text
lsmkv/
├── Cargo.toml            # 项目配置与依赖 (serde, bincode, memmap2, thiserror)
├── docs/                 # 您的文档目录
│   ├── lsm_kv_architecture.md
│   └── todolist.md
└── src/
    ├── lib.rs            # 根模块，负责 pub mod 导出各个子模块，对外暴露 API
    ├── error.rs          # 定义统一的 DbError 枚举和 Result<T> 别名
    ├── model.rs          # 纯数据定义：Key, Value, RecordType, LogRecord
    ├── wal.rs            # 预写日志的 IO 实现：WalWriter (追加写) 和 WalReader (故障回放)
    ├── memtable.rs       # 内存表封装：包装 BTreeMap，提供 put/get 并维护 approx_size
    ├── sstable.rs        # 磁盘表操作：SsTableBuilder (序列化落盘) 和 SsTableReader (mmap 零拷贝查询)
    └── db.rs             # 核心枢纽：定义 DbKernel，实现 put/get/delete/write，管理 mpsc 通道与后台 Flush 线程
```

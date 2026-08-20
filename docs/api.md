# LSMKV API Documentation

`lsmkv` 是一个高性能、线程安全、基于 LSM-Tree 架构的键值存储引擎。它提供了极简且强大的异步 API，专为高并发读写场景设计，同时支持高级配置以满足不同硬件环境（NVMe/SSD/HDD）下的性能调优需求。

---

## 核心接口 (Core API)

### `LsmKv`

数据库的核心交互对象。它是线程安全的（内部使用 `Arc` 与精细的读写锁），你可以安全地克隆并在多个 tokio 任务/线程中并发使用它。

#### `LsmKv::open`

```rust
pub async fn open(options: DbOptions) -> Result<LsmKv>
```

**描述**：打开或创建一个新的数据库实例。如果指定的目录不存在，则会自动创建。如果目录中已经存在 WAL 预写日志文件，它会自动进行崩溃恢复（Crash Recovery），将未落盘的数据重放至内存。

- **参数**：`options`: `DbOptions` - 数据库的全局配置。
- **返回**：`Result<LsmKv>` - 成功则返回可供操作的 `LsmKv` 实例，失败则返回 `DbError`。

#### `LsmKv::put`

```rust
pub async fn put(&self, key: Key, value: Value) -> Result<()>
```

**描述**：将指定的键值对写入数据库。
此方法是异步的，写入时会利用**组提交 (Group Commit)** 机制与其他并发写入进行聚合，直到数据安全持久化到 WAL（并完成 fsync）后才会返回。这确保了严格的持久性 (Durability)。

- **参数**：
  - `key`: `Vec<u8>` - 键
  - `value`: `Vec<u8>` - 值
- **返回**：成功返回 `Ok(())`，若发生底层存储故障（如磁盘写满）则抛出 `DbError::Corruption`。

#### `LsmKv::get`

```rust
pub fn get(&self, k: &Key) -> Result<Value>
```

**描述**：从数据库中读取指定键的值。
此方法是同步的。它会按顺序查找：Active MemTable -> Immutable MemTables -> SSTables，直到找到数据或遇到 Tombstone（逻辑删除标记）。

- **参数**：`k`: `&Vec<u8>` - 键的引用
- **返回**：成功返回 `Value`，若键不存在或被删除，返回 `DbError::NotFound`。

#### `LsmKv::delete`

```rust
pub async fn delete(&self, key: Key) -> Result<()>
```

**描述**：从数据库中删除指定键。
这不会立刻回收存储空间，而是通过追加写入一个**墓碑标记 (Tombstone)** 的方式来实现逻辑删除。后续的后台 Compaction 会真正回收数据空间。

- **参数**：`key`: `Vec<u8>` - 要删除的键

---

## 配置体系 (Configuration)

### `DbOptions`

全局配置选项。我们推荐使用 `DbOptions::builder()` 链式调用来构建此配置。

```rust
use lsmkv::options::DbOptions;
use lsmkv::sstable::sstable_builder::CompressionType;

let options = DbOptions::builder()
    .dir("./my_db_data")
    .mem_table_max_size(8 * 1024 * 1024)
    .max_batch_count(2000)
    .compression_type(CompressionType::Zstd)
    .build();
```

#### Builder 可用方法

| 方法名 | 类型 | 默认值 | 描述 |
| :--- | :--- | :--- | :--- |
| `dir(path)` | `impl Into<PathBuf>` | `"lsmkv_db"` | 数据库文件 (WAL / SSTable) 存放的目录路径。 |
| `mem_table_max_size(size)` | `usize` | `4MB` | 内存表的最大容量阈值。达到此阈值时会触发轮转并后台刷盘 (Flush)。调大可减少写放大，但会占用更多内存。 |
| `max_batch_bytes(bytes)` | `usize` | `1MB` | Group Commit 批处理写入的大小上限（字节数）。 |
| `max_batch_count(count)` | `usize` | `1000` | Group Commit 批处理写入的条数上限。 |
| `sstable_block_size(size)` | `usize` | `4096` | SSTable 的目标数据块大小 (Bytes)。建议对于 NVMe SSD 设为较小值 (4KB)，HDD 设为较大值。 |
| `compression_type(c_type)` | `CompressionType` | `Zstd` | SSTable 数据块的压缩算法。支持 `None`, `Snappy`(规划中) 和 `Zstd`。 |
| `compression_level(level)` | `i32` | `3` | 压缩等级。级别越高压缩率越高，但消耗更多 CPU。 |
| `max_write_queue_size(size)` | `usize` | `1024` | 写请求排队通道的容量。当并发请求超过该队列容量时，会产生反压阻塞 (Backpressure)。 |
| `flush_queue_size(size)` | `usize` | `100` | 等待后台刷盘的 MemTable 任务队列容量。 |

---

## 错误处理 (Error Handling)

所有的核心 API 均返回 `crate::error::Result<T>`，即 `Result<T, DbError>`。

### `DbError` Enum

```rust
pub enum DbError {
    /// 触发了底层的 std::io::Error。
    IO(std::io::Error),
    /// 序列化或反序列化失败。
    Serialize(bincode::Error),
    /// 数据损坏的致命错误。
    /// 触发原因可能是：WAL 日志截断、磁盘满、Magic Number 校验失败等。
    /// 此时强烈建议监控系统发出警报，进程应当 Fail-Fast。
    Corruption(String),
    /// 键未找到（通常在调用 `get` 时触发）。
    NotFound,
}
```

> [!TIP]
> 建议在实际使用中对 `DbError::Corruption` 进行最高等级的告警处理。由于系统已实现了全局只读/崩溃防御机制，当发生 `Corruption` 时，当前写入流可能会主动 `panic` 保护数据完整性。

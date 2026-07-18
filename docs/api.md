# LSM-Tree 引擎核心 API 文档

本文档详细描述了 `lsmkv` 引擎各个内部核心模块的职责和公共方法。

## 1. 核心控制器 `DbKernel` (`src/db_kernel.rs`)

`DbKernel` 是整个存储引擎的门面与调度中心。它负责管理 MemTable、持久化日志 (WAL) 以及后台异步刷盘的协调。

### 主要方法

- `pub fn new(flush_tx: Sender<FlushTask>, flush_rx: Receiver<Result<FlushResult>>, dir: &str) -> Result<Self>`
  创建或恢复数据库引擎。启动时会自动扫描工作目录下的 WAL 文件 (`.log`)，并回放日志以重建最后的活跃 `MemTable`。若发现多个未落盘的日志，会将旧数据封存为 `Immutable MemTable` 并发给后台调度。
- `pub fn put(&mut self, k: Key, v: Value) -> Result<()>`
  向引擎中插入或更新一条键值对。操作会首先被写入 WAL，然后再写入内存。
- `pub fn delete(&mut self, k: Key) -> Result<()>`
  对某个键执行删除操作。引擎会写入一个 Tombstone（墓碑）记录。
- `pub fn get(&self, k: &Key) -> Result<Value>`
  读取给定键的值。查找顺序遵循：活跃 MemTable -> 多个只读 Immutable MemTable（从新到老）-> 多个持久化 SSTable（从新到老）。

## 2. 内存表 `MemTable` (`src/memtable.rs`)

`MemTable` 是基于 `BTreeMap` 实现的内存层，所有写请求最终都先在它这里缓冲。

### 主要字段与常量

- `pub static MEM_TABLE_MAX_SIZE: usize = 4MB;`
  内存表的最大容量阈值，超过该限制将触发封存与刷盘。
- `pub id: u64`
  内存表的唯一序列号，通常与 WAL 文件的 ID 一一对应。

### 主要方法

- `pub fn put(&mut self, key: Key, value: Option<Value>)`
  写入数据，若传入 `None` 则表示插入墓碑（删除）。方法内部会自动估算并维护当前数据结构的 `approx_size`（占用字节数）。
- `pub fn get(&self, key: &Key) -> GetResult<&Value>`
  使用自定义的 `GetResult` 枚举返回查找结果，能清晰区分是找到了数据、找到了墓碑（已删除）还是不存在。

## 3. 预写日志 `WalWriter` (`src/wal.rs`)

保证系统具备 Crash Safe（崩溃恢复）能力的底层组件。在崩溃或意外重启时提供数据留存。

### 主要方法

- `pub fn rotate(&mut self, next_id: u64) -> Result<(), DbError>`
  滚动日志。将当前日志强制 `flush()` 并 `sync_all()` 落盘关闭，然后基于 `next_id` 开启一个新的 `.log` 文件。
- `pub fn append(&mut self, record: &LogRecord) -> Result<(), DbError>`
  以高性能 Bincode 序列化格式向日志文件追加写入一条记录。
- `pub fn read_all_records(path: &Path) -> Result<Vec<LogRecord>, DbError>`
  启动时使用的恢复方法，能够一次性读取特定日志文件内的所有操作，并在解析时容忍因系统崩溃导致的截断错误 (Truncated EOF)。

## 4. 后台刷盘调度器 `Flusher` (`src/flush.rs`)

脱离于主线程，专门用于处理 I/O 密集型任务的异步引擎。

### 主要方法

- `pub fn new(...) -> Self`
  构造调度器。依赖自定义的 `lake::ThreadPool`。
- `pub fn spawn(self) -> thread::JoinHandle<()>`
  在一个后台循环线程中启动监听。每当通过 `task_rx` 收到主引擎传来的 `FlushTask::Task(Arc<MemTable>)` 时，就会生成一个任务投递到线程池，执行向 `SSTable` 的序列化与磁盘写入操作，完成后再通过 `result_tx` 通知主引擎回收垃圾文件。

## 5. 排序字符串表 `SSTable` (`src/sstable.rs`)

落入本地磁盘的静态数据文件。

### `SSTableBuilder`

- `pub fn build(mut self, mem_iter: impl Iterator<Item = (Key, Option<Value>)>) -> Result<()>`
  接收一组有序的键值对（通常从 Immutable MemTable 获取），将它们序列化后顺序追加到文件。在末尾附加 `BTreeMap` 构建的偏移量索引，并记录 8 字节的 `Magic Number (0x8888)` 用于校验。

### `SSTable`

- `pub fn open(path: &str) -> Result<Self>`
  打开已有的 SSTable 文件，并立即将底层的索引 (Index) 加载至内存。
- `pub fn get(&self, key: &Key) -> GetResult<Value>`
  依托 `memmap2` (mmap) 实现的高性能零拷贝读取。首先在内存中查找索引获得偏移量，然后直接计算出磁盘块的长度并按需映射读取。

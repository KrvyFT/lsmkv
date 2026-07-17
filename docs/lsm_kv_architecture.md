# 轻量级 LSM-Tree Key-Value 数据库内核架构指南

## 1. 核心思想与妥协

我们要实现的是一个简化版的 LSM-Tree 存储引擎。LSM（Log-Structured Merge-Tree）的核心思想是：**将随机写转化为顺序追加写，以获取极高的写入吞吐量。**

为平衡实现难度与原理学习：

1. **舍弃底层指针手写**：不手撕 SkipList，采用标准库的 `BTreeMap` 替代。规避 Rust 中生命周期和裸指针操作的心智负担，从而聚焦在存储引擎的架构设计。
2. **混合序列化**：对于单个记录内部结构，使用 `bincode` 库快速序列化；但对于**整个磁盘文件的区块划分（块的长度、索引的定位）**，必须手动处理二进制偏移，这是深刻理解数据库底层布局的关键。

---

## 2. 核心模块与实现路径分解

以下是按开发顺序排列的模块详解。包含“目标”、“理论机制”以及“核心代码实现辅助参考”。

### 阶段一：基础通讯语言 —— 错误处理与记录定义

**1. 要干什么？**

- 统一整个数据库的错误类型，防止 IO 或格式异常导致的 Panic。
- 定义数据的基本存在形式（Put / Delete）。

**2. 怎么做到？**

- 引入 `thiserror` 定义 `DbError`。
- 定义 `LogRecord` 结构体，内部包含操作枚举、Key 和 Value。派生 `serde` 宏支持二进制序列化。

**3. 代码辅助参考**

```rust
use serde::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Data corruption: {0}")]
    Corruption(String),
}

pub type Result<T> = std::result::Result<T, DbError>;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Serialize, Deserialize, Debug)]
pub enum RecordType {
    Put,
    Delete,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogRecord {
    pub r_type: RecordType,
    pub key: Key,
    pub value: Value,
}
```

### 阶段二：生命线 —— WAL（预写日志）

**1. 要干什么？**
保证数据持久性（Durability）。进入内存前，数据必须先写入磁盘上的 WAL 文件，以防断电丢失。

**2. 怎么做到？**

- **写入端 (Writer)**：以 `append(true)` 追加模式打开文件。一条记录转为二进制后，**必须先写一个 4 字节的长度前缀 (Length-Prefixed)**，再写入真实数据。写完必须强制刷盘（`sync_data`）。
- **读取端 (Reader)**：启动时，按“先读 4 字节长度 -> 再读等长内容反序列化”的节奏循环读取。如果遇到截断（EOF），说明断电发生在写入中途，需优雅吞掉错误并停止读取。

**3. 代码辅助参考**

```rust
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, Read};

pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    pub fn new(path: &str) -> crate::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { writer: BufWriter::new(file) })
    }

    pub fn append(&mut self, record: &LogRecord) -> crate::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(record)?;
        // 1. 获取 Payload 长度
        let len = encoded.len() as u32;
        // 2. 先写 4 字节小端序长度
        self.writer.write_all(&len.to_le_bytes())?;
        // 3. 再写实际数据
        self.writer.write_all(&encoded)?;
        Ok(())
    }

    pub fn sync(&mut self) -> crate::Result<()> {
        self.writer.flush()?;
        // fdatasync，确保落入物理磁盘
        self.writer.get_ref().sync_data()?; 
        Ok(())
    }
}
```

### 阶段三：内存枢纽 —— MemTable

**1. 要干什么？**
充当写入缓冲，维持内存数据有序。写满后将其整体刷入磁盘形成 SSTable。

**2. 怎么做到？**

- 包装 `BTreeMap<Key, Value>` 提供有序存储。
- 重点维护 `approx_size`，每次 `put` 累加键值对大小，用于后续触发落盘动作。

**3. 代码辅助参考**

```rust
use std::collections::BTreeMap;

pub struct MemTable {
    map: BTreeMap<Key, Value>,
    /// 用于评估内存占用，决定何时触发刷盘
    pub approx_size: usize, 
}

impl MemTable {
    pub fn new() -> Self {
        Self { map: BTreeMap::new(), approx_size: 0 }
    }

    pub fn put(&mut self, key: Key, value: Value) {
        self.approx_size += key.len() + value.len();
        self.map.insert(key, value);
    }
    
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Value)> {
        self.map.iter()
    }
}
```

### 阶段四：不可变基石 —— SSTable 的生成

**1. 要干什么？**
将写满的 MemTable 落地为独立的磁盘文件。不仅要存数据，还要**在文件上建立索引，支持磁盘上的二分查找**，避免全表扫描。

**2. 怎么做到？（宏观文件布局）**

- **Data Blocks**：遍历 BTreeMap，将记录转二进制写入。同时**记录下该条记录在文件中的精确偏移量（Offset）**。
- **Index Block**：将刚才收集的 `Key -> Offset` 映射表完整序列化写入文件末尾。
- **Footer 定长尾部**：在文件最后写入固定 16 字节（8 字节记录 Index Block 的起始位置 + 8 字节魔法数字防伪）。

**3. 代码辅助参考**

```rust
use std::fs::File;
use std::io::{BufWriter, Write};
use std::collections::BTreeMap;

pub struct SsTableBuilder {
    writer: BufWriter<File>,
    index: BTreeMap<Key, u64>, // 记录每个 Key 在磁盘上的 Offset
    current_offset: u64,       // 当前写入游标
}

impl SsTableBuilder {
    pub fn build(mut self, memtable_iter: impl Iterator<Item=(Key, Value)>) -> crate::Result<()> {
        // 1. 写 Data Blocks
        for (k, v) in memtable_iter {
            let record = LogRecord { r_type: RecordType::Put, key: k.clone(), value: v };
            let encoded = bincode::serialize(&record)?;
            
            // 登记当前 Key 的精确磁盘位置
            self.index.insert(k, self.current_offset); 
            
            let len = encoded.len() as u32;
            self.writer.write_all(&len.to_le_bytes())?;
            self.writer.write_all(&encoded)?;
            
            // 更新游标 (4字节长度 + 实际数据长)
            self.current_offset += 4 + len as u64; 
        }
        
        // 2. 写 Index Block
        let index_offset = self.current_offset; // 记下索引块的起点
        let encoded_index = bincode::serialize(&self.index)?;
        self.writer.write_all(&encoded_index)?;
        
        // 3. 写 Footer (定长 16 字节)
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(&0x_8888_LSM_KV_u64.to_le_bytes())?; // Magic Number
        
        self.writer.flush()?;
        Ok(())
    }
}
```

### 阶段五：零拷贝检索 —— SSTable 的读取

**1. 要干什么？**
通过内存映射 (mmap) 极速定位索引，并在极少拷贝的情况下查出所需的数据块。

**2. 怎么做到？**

- 借助 `memmap2` 将磁盘文件映射为 `&[u8]` 切片。
- 直接切取最后 16 字节读取 Footer，提取 Index Offset。
- 跳到 Index Offset 反序列化出 `BTreeMap<Key, u64>` 的大纲索引。
- 用户查询时，先查内存索引拿到具体 Offset，再切取那一段 mmap 数据反序列化拿到 Value。

**3. 代码辅助参考**

```rust
use memmap2::Mmap;
use std::fs::File;

pub struct SsTable {
    mmap: Mmap,
    // 启动时解析出来驻留内存的目录索引
    index: BTreeMap<Key, u64>, 
}

impl SsTable {
    pub fn open(path: &str) -> crate::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        
        let len = mmap.len();
        // 1. 定位最后 16 字节的 Footer
        let footer = &mmap[len - 16..];
        
        // 2. 校验魔数
        let mut magic_bytes = [0u8; 8];
        magic_bytes.copy_from_slice(&footer[8..16]);
        if u64::from_le_bytes(magic_bytes) != 0x_8888_LSM_KV_u64 {
            return Err(crate::DbError::Corruption("Bad Magic".into()));
        }

        // 3. 提取 Index Offset
        let mut offset_bytes = [0u8; 8];
        offset_bytes.copy_from_slice(&footer[0..8]);
        let index_offset = u64::from_le_bytes(offset_bytes) as usize;

        // 4. 解析 Index Block
        let index_data = &mmap[index_offset..len - 16];
        let index: BTreeMap<Key, u64> = bincode::deserialize(index_data)?;

        Ok(Self { mmap, index })
    }
}
```

### 阶段六：中枢指挥与核心 API —— DbKernel

**1. 要干什么？**
组装 WAL、MemTable 和 SSTable，正式暴露出数据库的四大核心 API (`put`, `get`, `delete`, `write`)。同时，当内存表满载时，要实现无缝的前后台交接落盘，绝不能阻塞用户的并发请求。

**2. 怎么做到？**

- **写操作集群 (`put`, `delete`, `write`)**：
  - **批量写入 (`write`)**：为了保证原子性，支持传入一个批量操作队列（如 `WriteBatch`）。遍历队列追加到 WAL 中，最后统一执行一次 `sync_data` 落盘。接着，将它们逐个应用到 MemTable。
  - **单体操作 (`put`, `delete`)**：它们在底层只是**仅包含一条记录的 `write` 操作**。对于 `delete`，在 LSM 中并非去磁盘里寻找并物理删除，而是写入一条带有 `RecordType::Delete` 标记的特殊空记录，我们称之为**“墓碑 (Tombstone)”**。
  - **容量拦截**：每次内存表被修改后检查 `approx_size`，一旦超过阈值（如 4MB），通过 `std::mem::replace` 换出一张新表。老表用 `Arc` 包装后，通过 `mpsc` 通道发给后台 Flush 线程落盘。
- **读操作分层穿透 (`get`)**：
  - 用户的查询必须按严格的**时间倒序（从新到旧）**查找：
    1. 查当前的 **活跃 MemTable**。
    2. 没找到，查等待落盘的 **Immutable MemTable(s)**。
    3. 没找到，查磁盘上所有的 **SSTable(s)**（先查新的，再查老的）。
  - **墓碑机制**：在上述任何一层找到了该 Key，如果它的值是墓碑（空值或 Delete 标记），必须立刻停止往下找，并直接返回 `NotFound`。
- **优雅停机 (Graceful Shutdown)**：在 DB 释放（Drop）时，发送关闭毒丸，并 `join()` 等待后台将最后一张表的 SSTable 彻底建完。

**3. 代码辅助参考**

```rust
use std::sync::{Arc, mpsc};
use std::thread;

pub enum WriteOp {
    Put(Key, Value),
    Delete(Key),
}

pub struct WriteBatch {
    pub ops: Vec<WriteOp>,
}

pub enum FlushTask {
    Task(Arc<MemTable>), // 发送不可变老表供落盘
    Shutdown,            // 毒丸：退出信号
}

pub struct DbKernel {
    memtable: MemTable,
    wal: WalWriter,
    flush_tx: mpsc::Sender<FlushTask>,
}

impl DbKernel {
    /// 核心 API：批量写入保证原子性
    pub fn write(&mut self, batch: WriteBatch) -> crate::Result<()> {
        // 1. 将所有操作序列化并追加到 WAL，只执行 1 次 fsync
        for op in &batch.ops {
            let record = match op {
                WriteOp::Put(k, v) => LogRecord { r_type: RecordType::Put, key: k.clone(), value: v.clone() },
                WriteOp::Delete(k) => LogRecord { r_type: RecordType::Delete, key: k.clone(), value: vec![] }, // 墓碑
            };
            self.wal.append(&record)?;
        }
        self.wal.sync()?;
        
        // 2. 应用到 MemTable (此处假设 MemTable 用空 Vec 表示墓碑)
        for op in batch.ops {
            match op {
                WriteOp::Put(k, v) => self.memtable.put(k, v),
                WriteOp::Delete(k) => self.memtable.put(k, vec![]), 
            }
        }
        
        // 3. 拦截与后台转移
        if self.memtable.approx_size > 4 * 1024 * 1024 { 
            let old_memtable = std::mem::replace(&mut self.memtable, MemTable::new());
            self.flush_tx.send(FlushTask::Task(Arc::new(old_memtable))).unwrap();
        }
        Ok(())
    }

    /// 单条插入，底层复用 write
    pub fn put(&mut self, key: Key, value: Value) -> crate::Result<()> {
        self.write(WriteBatch { ops: vec![WriteOp::Put(key, value)] })
    }

    /// 删除操作转为写入墓碑
    pub fn delete(&mut self, key: Key) -> crate::Result<()> {
        self.write(WriteBatch { ops: vec![WriteOp::Delete(key)] })
    }
    
    /// 分层查询穿透
    pub fn get(&self, key: &Key) -> crate::Result<Value> {
        // 1. 查活跃 MemTable
        if let Some(val) = self.memtable.get(key) {
            // 遭遇墓碑，阻止向下查找
            if val.is_empty() { return Err(crate::DbError::NotFound); } 
            return Ok(val.clone());
        }
        
        // 2. 查 Immutable MemTables (需要有一个结构维护队列)
        // if let Some(val) = ...
        
        // 3. 查 SSTables (通过 SsTableReader，从新到旧遍历)
        // if let Some(val) = ...
        
        // 最底层都没找到
        Err(crate::DbError::NotFound)
    }
}
```

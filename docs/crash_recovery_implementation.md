# 崩溃恢复机制 (Crash Recovery) 实现指南

在 LSM-Tree 中，内存中的 `MemTable` 是易失的。如果数据库崩溃或正常重启，我们需要通过存储在硬盘上的 WAL (Write-Ahead Log) 重建崩溃前的内存状态。

以下是如何在 `DbKernel::new` 或新加的 `DbKernel::open` 中实现崩溃恢复逻辑的设计和步骤。

---

## 1. 核心恢复流程

当数据库启动时，它不应该只是单纯地 `MemTable::new(0)`，而必须先扫描数据目录：

1. **扫描并收集**：读取指定目录下的所有 `wal_xxxxxx.log` 文件。
2. **提取排序**：通过正则或字符串切片提取出文件中间的 `xxxxxx` 数字 ID，并将文件按 ID 从小到大排序。
3. **依次重放 (Replay)**：
   - 打开每个日志文件，逐条读取并反序列化 `LogRecord`。
   - 把这些记录重新写入（Put/Delete）到内存中的 `MemTable` 里。
4. **状态归位**：
   - **历史表**：除了编号最大的那个 WAL 文件外，其它的历史 WAL 文件对应的 `MemTable` 都应该放入 `imm_memtables`，并立刻发送给后台进行刷盘（因为它们本来就是要去刷盘的，只是中途崩溃了）。
   - **当前活跃表**：编号最大的那个 WAL，应该作为当前的 `self.memtable` 继续接收新的写请求。
5. **恢复 ID 计数**：设置 `DbKernel.next_file_id = 最新 WAL 的 ID`。

---

## 2. 详细的 Rust 代码实现方案

### 2.1 WAL 日志读取器 (WalReader)

现在的 `WalWriter` 只能写，不能读。你需要在 `src/wal.rs` 中实现日志读取逻辑。
由于我们在写入时是 `[长度 u32] + [bincode 数据]`，读取时也要逆向操作：

```rust
// src/wal.rs
use std::io::Read;

pub fn read_all_records(path: &Path) -> Result<Vec<LogRecord>, DbError> {
    let mut file = std::fs::File::open(path).map_err(|e| DbError::Corruption(e.to_string()))?;
    let mut records = Vec::new();

    loop {
        // 1. 读取 4 字节长度头
        let mut len_buf = [0u8; 4];
        if let Err(e) = file.read_exact(&mut len_buf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break; // 读到文件末尾了，正常退出
            }
            // 处理 WAL 尾部被截断的情况（崩溃时只写了一半）
            eprintln!("Warning: WAL file may be truncated: {}", e);
            break; 
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        // 2. 根据长度读取实际载荷
        let mut payload = vec![0u8; len];
        if let Err(e) = file.read_exact(&mut payload) {
            eprintln!("Warning: WAL payload truncated: {}", e);
            break; // 同样防范崩溃写入撕裂
        }

        // 3. 反序列化
        let record: LogRecord = bincode::deserialize(&payload)
            .map_err(|e| DbError::Corruption(e.to_string()))?;
            
        records.push(record);
    }

    Ok(records)
}
```

### 2.2 DbKernel 的恢复与初始化挂载

你需要重构 `DbKernel::new`，使其支持从目录中进行恢复：

```rust
// src/db_kernel.rs
impl DbKernel {
    pub fn new(dir: &str, flush_tx: mpsc::Sender<FlushTask>) -> Result<Self> {
        let dir_path = std::path::Path::new(dir);
        if !dir_path.exists() {
            std::fs::create_dir_all(dir_path).unwrap();
        }

        // 1. 扫描出所有的 wal 文件
        let mut wal_files = Vec::new();
        for entry in std::fs::read_dir(dir_path).unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name().into_string().unwrap();
            
            if file_name.starts_with("wal_") && file_name.ends_with(".log") {
                // 解析文件名 wal_000001.log 中的 000001
                let id_str = &file_name[4..10]; 
                if let Ok(id) = id_str.parse::<u64>() {
                    wal_files.push((id, entry.path()));
                }
            }
        }

        wal_files.sort_by_key(|k| k.0);

        let mut imm_memtables = Vec::new();
        let mut active_memtable = None;
        let mut next_file_id = 0;

        // 2. 遍历重放
        if wal_files.is_empty() {
            active_memtable = Some(MemTable::new(0));
        } else {
            let last_idx = wal_files.len() - 1;
            
            for (i, (id, path)) in wal_files.into_iter().enumerate() {
                let records = crate::wal::read_all_records(&path)?;
                let mut memtable = MemTable::new(id);
                
                // 将记录重放回 memtable
                for rec in records {
                    match rec.r_type {
                        RecordType::Put => memtable.put(rec.key, rec.value),
                        RecordType::Delete => { let _ = memtable.delete(&rec.key); }
                    }
                }

                if i == last_idx {
                    // 最新的那个作为活跃表
                    active_memtable = Some(memtable);
                    next_file_id = id;
                } else {
                    // 旧的表送入 imm 并触发 Flush
                    let imm = Arc::new(memtable);
                    imm_memtables.push(imm.clone());
                    flush_tx.send(FlushTask::Task(imm)).unwrap();
                }
            }
        }

        let wal = WalWriter::new(dir, next_file_id)?;

        Ok(Self {
            memtable: active_memtable.unwrap(),
            imm_memtables,
            wal,
            flush_tx,
            next_file_id,
            wal_dir: dir.to_string(),
        })
    }
}
```

---

## 3. 防御性设计要点 (Expert Advice)

1. **截断容忍 (Torn Writes Tolerance)**：如果在系统崩溃瞬间，系统只向 WAL 写了长度没写数据，或者数据只写了一半。这就导致最后一条数据是残缺的（Torn Write）。在 `read_all_records` 中，遇到此类 `UnexpectedEof`，我们直接 `break` 忽略它并视为合法结尾，而不是抛出 Panic 阻止数据库启动。这是一种标准且健壮的容错机制。
2. **幂等性 (Idempotency)**：如果在 Flush 到 SSTable 后系统崩溃了，但旧的 WAL 还来不及删除，重启时这部分旧 WAL 会被再次作为 `imm_memtables` 重放并进行再次 Flush。这也是可以接受的，因为 LSM 的写（合并）操作本身是幂等的。等我们在后续（阶段4）完成完整的“登记-删除”元数据事务后，就可以避免这种重复工作。

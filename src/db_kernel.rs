use std::{
    fs::create_dir_all,
    path::Path,
    sync::{Arc, mpsc},
};

use crate::{
    error::{DbError, Result},
    flush::{FlushResult, FlushTask},
    memtable::{MEM_TABLE_MAX_SIZE, MemTable},
    model::{GetResult, Key, LogRecord, RecordType, Value},
    sstable::sstable::SSTable,
    wal::WalWriter,
};

pub enum WriteOP {
    Put(Key, Value),
    Delete(Key),
}

/// A batch of write operations to be executed atomically.
pub struct WriteBatch {
    /// The operations in the batch
    pub ops: Vec<WriteOP>,
}

/// The core kernel of the LSM-Tree database.
/// Coordinates MemTable, WAL, Immutable MemTables, and SSTables.
pub struct DbKernel {
    memtable: MemTable,
    imm_memtables: Vec<Arc<MemTable>>,
    wal: WalWriter,
    next_file_id: u64,
    wal_dir: String,
    flush_tx: mpsc::Sender<FlushTask>,
    flush_rx: mpsc::Receiver<Result<FlushResult>>,
    sstables: Vec<SSTable>,
}

impl DbKernel {
    /// Creates a new `DbKernel` instance, recovering from the WAL log if it exists.
    pub fn new(
        flush_tx: mpsc::Sender<FlushTask>,
        flush_rx: mpsc::Receiver<Result<FlushResult>>,
        dir: &str,
    ) -> Result<Self> {
        let dir_path = Path::new(dir);
        if !dir_path.exists() {
            create_dir_all(dir_path).unwrap();
        }

        let mut wal_files = Vec::new();
        for entry in dir_path.read_dir().unwrap() {
            let entry = entry.unwrap();
            let file_name = entry.file_name().into_string().unwrap();

            if file_name.starts_with("log_") && file_name.ends_with(".log") {
                let id_str = &file_name[4..file_name.len() - 4];
                if let Ok(id) = id_str.parse::<u64>() {
                    wal_files.push((id, entry.path()));
                }
            }
        }

        wal_files.sort_by_key(|k| k.0);

        let mut imm_memtables = Vec::new();
        let mut active_memtable = None;
        let mut next_file_id = 0;

        if wal_files.is_empty() {
            active_memtable = Some(MemTable::new(0));
        } else {
            let last_idx = wal_files.len() - 1;
            for (i, (id, path)) in wal_files.iter().enumerate() {
                let records = WalWriter::read_all_records(path)?;
                let mut memtable = MemTable::new(*id);

                for rec in records {
                    match rec.r_type {
                        RecordType::Put => memtable.put(rec.key, rec.value),
                        RecordType::Delete => memtable.delete(&rec.key)?,
                    }
                }

                if i == last_idx {
                    active_memtable = Some(memtable);
                    next_file_id = *id;
                } else {
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
            flush_rx,
            sstables: Vec::new(),
        })
    }

    fn try_sync_flush_results(&mut self) {
        // 使用 try_recv() 绝不阻塞主线程，一直收直到信箱空了（Err(Empty)）为止
        while let Ok(Ok(result)) = self.flush_rx.try_recv() {
            // 1. 加载刚刷盘写好的 SSTable
            if let Ok(sstable) = SSTable::open(&result.sstable_path) {
                self.sstables.push(sstable);
            }

            // 2. 将对应的旧 MemTable 从只读队列里剔除
            self.imm_memtables
                .retain(|imm| imm.id != result.memtable_id);

            // 3. 垃圾回收：删除掉这部分数据对应的原始 WAL 文件 (如 log_000000.log)
            let wal_path = std::path::Path::new(&self.wal_dir)
                .join(format!("log_{:06}.log", result.memtable_id));
            let _ = std::fs::remove_file(wal_path);
        }
    }

    /// Executes a batch of write operations atomically.
    pub fn write(&mut self, batch: &WriteBatch) -> Result<()> {
        self.try_sync_flush_results();

        for op in &batch.ops {
            let record = match op {
                WriteOP::Put(k, v) => LogRecord {
                    r_type: RecordType::Put,
                    key: k.clone(),
                    value: Some(v.clone()),
                },
                WriteOP::Delete(k) => LogRecord {
                    r_type: RecordType::Delete,
                    key: k.clone(),
                    value: None,
                },
            };
            self.wal.append(&record)?;
        }
        self.wal.sync()?;

        for op in &batch.ops {
            match op {
                WriteOP::Put(k, v) => self.memtable.put(k.clone(), Some(v.clone())),
                WriteOP::Delete(k) => self.memtable.put(k.clone(), None),
            }
        }

        if self.memtable.approx_size >= MEM_TABLE_MAX_SIZE {
            self.next_file_id += 1;
            let old_memtable =
                std::mem::replace(&mut self.memtable, MemTable::new(self.next_file_id));
            let imm_memtable = Arc::new(old_memtable);
            self.imm_memtables.push(imm_memtable.clone());
            self.flush_tx.send(FlushTask::Task(imm_memtable)).unwrap();

            self.wal.rotate(self.next_file_id).unwrap();
        }

        Ok(())
    }

    /// Inserts a key-value pair into the database.
    pub fn put(&mut self, k: Key, v: Value) -> Result<()> {
        self.write(&WriteBatch {
            ops: vec![WriteOP::Put(k, v)],
        })
    }

    /// Deletes a key from the database using a Tombstone.
    pub fn delete(&mut self, k: Key) -> Result<()> {
        self.write(&WriteBatch {
            ops: vec![WriteOP::Delete(k)],
        })
    }

    /// Retrieves a value by its key.
    /// Searches in MemTable -> Immutable MemTables -> SSTables in order.
    pub fn get(&self, k: &Key) -> Result<Value> {
        match self.memtable.get(k) {
            GetResult::Found(v) => return Ok(v.clone()),
            GetResult::Deleted => return Err(DbError::NotFound),
            GetResult::NotFound => {} // 继续向下查找
        }

        for imm in self.imm_memtables.iter().rev() {
            match imm.get(k) {
                GetResult::Found(v) => return Ok(v.clone()),
                GetResult::Deleted => return Err(DbError::NotFound),
                GetResult::NotFound => continue,
            }
        }

        for sst in self.sstables.iter().rev() {
            match sst.get(k) {
                GetResult::Found(v) => return Ok(v),
                GetResult::Deleted => return Err(DbError::NotFound),
                GetResult::NotFound => continue,
            }
        }

        Err(DbError::NotFound)
    }
}

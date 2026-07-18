use std::sync::{Arc, mpsc};

use crate::{
    error::{DbError, Result},
    memtable::{MEM_TABLE_MAX_SIZE, MemTable},
    model::{GetResult, Key, LogRecord, RecordType, Value},
    wal::WalWriter,
};

pub enum WriteOP {
    Put(Key, Value),
    Delete(Key),
}

pub struct WriteBatch {
    pub ops: Vec<WriteOP>,
}

pub enum FlushTask {
    Task(Arc<MemTable>),
    Shutdown,
}

pub struct DbKernel {
    memtable: MemTable,
    imm_memtables: Vec<Arc<MemTable>>,
    wal: WalWriter,
    flush_tx: mpsc::Sender<FlushTask>,
    next_file_id: u64,
    wal_dir: String,
}

impl DbKernel {
    pub fn new(wal_path: &str, flush_tx: mpsc::Sender<FlushTask>, dir: &str) -> Result<Self> {
        Ok(Self {
            memtable: MemTable::new(0),
            imm_memtables: Vec::new(),
            wal: WalWriter::new(wal_path, 0)?,
            flush_tx,
            next_file_id: 0,
            wal_dir: dir.to_string(),
        })
    }

    pub fn write(&mut self, batch: &WriteBatch) -> Result<()> {
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

    pub fn put(&mut self, k: Key, v: Value) -> Result<()> {
        self.write(&WriteBatch {
            ops: vec![WriteOP::Put(k, v)],
        })
    }

    pub fn delete(&mut self, k: Key) -> Result<()> {
        self.write(&WriteBatch {
            ops: vec![WriteOP::Delete(k)],
        })
    }

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

        Err(DbError::NotFound) // TODO: fallback to SSTables
    }
}

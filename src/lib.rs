pub mod error;
pub mod flush;
pub mod memtable;
pub mod model;
pub mod sstable;
pub mod wal;

use std::{
    path::Path,
    sync::{Arc, RwLock},
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    error::{DbError, Result},
    flush::{FlushResult, FlushTask, Flusher},
    memtable::{MEM_TABLE_MAX_SIZE, MemTable},
    model::{GetResult, Key, LogRecord, RecordType, Value},
    sstable::sstable::SSTable,
    wal::WalWriter,
};

enum WriteOP {
    Put(Key, Value),
    Delete(Key),
}

struct WriteMessage {
    op: WriteOP,
    responder: oneshot::Sender<Result<()>>,
}

/// The core state of the LSM-Tree database.
struct Core {
    memtable: Arc<RwLock<MemTable>>,
    imm_memtables: Vec<Arc<RwLock<MemTable>>>,
    sstables: Vec<Arc<SSTable>>,
    wal_dir: String,
}

impl Core {
    fn try_sync_flush_results(&mut self, result: FlushResult) {
        if let Ok(sstable) = SSTable::open(&result.sstable_path) {
            self.sstables.push(Arc::new(sstable));
        }

        self.imm_memtables
            .retain(|imm| imm.read().unwrap().id != result.memtable_id);

        let wal_path = Path::new(&self.wal_dir)
            .join(format!("log_{:06}.log", result.memtable_id));
        let _ = std::fs::remove_file(wal_path);
    }
}

/// A high-performance, thread-safe LSM-Tree database library interface.
#[derive(Clone)]
pub struct LsmKv {
    core: Arc<RwLock<Core>>,
    write_tx: mpsc::Sender<WriteMessage>,
}

impl LsmKv {
    /// Opens or creates a new `LsmKv` instance, recovering from the WAL log if it exists.
    pub async fn open(dir: &str) -> Result<Self> {
        let dir_path = Path::new(dir);
        if !dir_path.exists() {
            tokio::fs::create_dir_all(dir_path)
                .await
                .map_err(|e| DbError::Corruption(e.to_string()))?;
        }

        let mut wal_files = Vec::new();
        // Since we are reading dir synchronously here during startup, we can just use std::fs for startup
        for entry in std::fs::read_dir(dir_path).unwrap() {
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

        let (flush_tx, flush_rx) = mpsc::channel(100);
        let (result_tx, mut result_rx) = mpsc::channel(100);

        let flusher = Flusher::new(result_tx, flush_rx, dir);
        flusher.spawn();

        let mut imm_memtables = Vec::new();
        let mut active_memtable = None;
        let mut next_file_id = 0;

        if wal_files.is_empty() {
            active_memtable = Some(Arc::new(RwLock::new(MemTable::new(0))));
        } else {
            let last_idx = wal_files.len() - 1;
            for (i, (id, path)) in wal_files.iter().enumerate() {
                let records = WalWriter::read_all_records(path).await?;
                let mut memtable = MemTable::new(*id);

                for rec in records {
                    match rec.r_type {
                        RecordType::Put => memtable.put(rec.key, rec.value),
                        RecordType::Delete => memtable.delete(&rec.key)?,
                    }
                }

                let memtable_arc = Arc::new(RwLock::new(memtable));

                if i == last_idx {
                    active_memtable = Some(memtable_arc);
                    next_file_id = *id;
                } else {
                    imm_memtables.push(memtable_arc.clone());
                    flush_tx.send(FlushTask::Task(memtable_arc)).await.unwrap();
                }
            }
        }

        let wal = WalWriter::new(dir, next_file_id).await?;

        let core = Arc::new(RwLock::new(Core {
            memtable: active_memtable.unwrap(),
            imm_memtables,
            sstables: Vec::new(),
            wal_dir: dir.to_string(),
        }));

        let (write_tx, write_rx) = mpsc::channel(10000);

        // Spawn writer task
        let writer_core = core.clone();
        tokio::spawn(Self::writer_task(writer_core, wal, write_rx, flush_tx));

        // Spawn flush result receiver task
        let result_core = core.clone();
        tokio::spawn(async move {
            while let Some(Ok(result)) = result_rx.recv().await {
                let mut core_lock = result_core.write().unwrap();
                core_lock.try_sync_flush_results(result);
            }
        });

        Ok(Self { core, write_tx })
    }

    /// Retrieves a value by its key.
    /// This method leverages a shared read lock for fast concurrent queries without blocking async writes.
    pub fn get(&self, k: &Key) -> Result<Value> {
        let core = self.core.read().unwrap();

        let mem = core.memtable.read().unwrap();
        match mem.get(k) {
            GetResult::Found(v) => return Ok(v.clone()),
            GetResult::Deleted => return Err(DbError::NotFound),
            GetResult::NotFound => {}
        }
        drop(mem);

        for imm in core.imm_memtables.iter().rev() {
            let mem = imm.read().unwrap();
            match mem.get(k) {
                GetResult::Found(v) => return Ok(v.clone()),
                GetResult::Deleted => return Err(DbError::NotFound),
                GetResult::NotFound => continue,
            }
        }

        for sst in core.sstables.iter().rev() {
            match sst.get(k) {
                GetResult::Found(v) => return Ok(v.clone()),
                GetResult::Deleted => return Err(DbError::NotFound),
                GetResult::NotFound => continue,
            }
        }

        Err(DbError::NotFound)
    }

    /// Asynchronously puts a key-value pair into the database.
    /// Uses Group Commit under the hood to ensure extreme write performance.
    pub async fn put(&self, key: Key, value: Value) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let msg = WriteMessage {
            op: WriteOP::Put(key, value),
            responder: tx,
        };
        if self.write_tx.send(msg).await.is_err() {
            return Err(DbError::Corruption("Writer task dropped".into()));
        }
        rx.await.unwrap_or(Err(DbError::Corruption("Writer task dropped".into())))
    }

    /// Asynchronously deletes a key-value pair from the database.
    pub async fn delete(&self, key: Key) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let msg = WriteMessage {
            op: WriteOP::Delete(key),
            responder: tx,
        };
        if self.write_tx.send(msg).await.is_err() {
            return Err(DbError::Corruption("Writer task dropped".into()));
        }
        rx.await.unwrap_or(Err(DbError::Corruption("Writer task dropped".into())))
    }

    async fn writer_task(
        core: Arc<RwLock<Core>>,
        mut wal: WalWriter,
        mut rx: mpsc::Receiver<WriteMessage>,
        flush_tx: mpsc::Sender<FlushTask>,
    ) {
        let mut next_file_id = {
            let core_read = core.read().unwrap();
            core_read.memtable.read().unwrap().id
        };

        while let Some(first_msg) = rx.recv().await {
            let mut ops = vec![first_msg.op];
            let mut responders = vec![first_msg.responder];

            // Aggressive batching for Group Commit
            while let Ok(msg) = rx.try_recv() {
                ops.push(msg.op);
                responders.push(msg.responder);
                if ops.len() >= 10000 {
                    break;
                }
            }

            let mut err = None;
            for op in &ops {
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
                if let Err(e) = wal.append(&record).await {
                    err = Some(e);
                    break;
                }
            }

            if err.is_none() {
                if let Err(e) = wal.sync().await {
                    err = Some(e);
                }
            }

            if let Some(e) = err {
                for responder in responders {
                    let _ = responder.send(Err(DbError::Corruption(e.to_string())));
                }
                continue;
            }

            let mut should_rotate = false;
            {
                let memtable_arc = { core.read().unwrap().memtable.clone() };
                let mut memtable = memtable_arc.write().unwrap();
                
                for op in &ops {
                    match op {
                        WriteOP::Put(k, v) => memtable.put(k.clone(), Some(v.clone())),
                        WriteOP::Delete(k) => memtable.put(k.clone(), None),
                    }
                }
                if memtable.approx_size >= MEM_TABLE_MAX_SIZE {
                    should_rotate = true;
                }
            }

            if should_rotate {
                next_file_id += 1;
                
                let old_memtable_arc = {
                    let mut core_write = core.write().unwrap();
                    let old = core_write.memtable.clone();
                    core_write.imm_memtables.push(old.clone());
                    
                    let new_memtable = Arc::new(RwLock::new(MemTable::new(next_file_id)));
                    core_write.memtable = new_memtable;
                    old
                };

                if let Err(e) = wal.rotate(next_file_id).await {
                    eprintln!("Error rotating WAL: {}", e);
                }

                if let Err(e) = flush_tx.send(FlushTask::Task(old_memtable_arc)).await {
                    eprintln!("Error sending flush task: {}", e);
                }
            }

            for responder in responders {
                let _ = responder.send(Ok(()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_lsmkv_basic_put_get() {
        let dir = tempdir().unwrap();
        let kv = LsmKv::open(dir.path().to_str().unwrap()).await.unwrap();

        kv.put(b"key1".to_vec(), b"value1".to_vec()).await.unwrap();
        let val = kv.get(&b"key1".to_vec()).unwrap();
        assert_eq!(val, b"value1".to_vec());

        kv.put(b"key2".to_vec(), b"value2".to_vec()).await.unwrap();
        let val = kv.get(&b"key2".to_vec()).unwrap();
        assert_eq!(val, b"value2".to_vec());

        kv.delete(b"key1".to_vec()).await.unwrap();
        assert!(kv.get(&b"key1".to_vec()).is_err());
    }

    #[tokio::test]
    async fn test_lsmkv_concurrent_writes() {
        let dir = tempdir().unwrap();
        let kv = LsmKv::open(dir.path().to_str().unwrap()).await.unwrap();

        let mut handles = vec![];
        for i in 0..100 {
            let kv_clone = kv.clone();
            let handle = tokio::spawn(async move {
                let k = format!("key{}", i).into_bytes();
                let v = format!("value{}", i).into_bytes();
                kv_clone.put(k, v).await.unwrap();
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        for i in 0..100 {
            let k = format!("key{}", i).into_bytes();
            let expected_v = format!("value{}", i).into_bytes();
            let val = kv.get(&k).unwrap();
            assert_eq!(val, expected_v);
        }
    }
}

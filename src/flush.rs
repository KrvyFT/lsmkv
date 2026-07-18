use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;

use crate::{error::Result, memtable::MemTable, sstable::sstable_builder::SSTableBuilder};

/// Result of a successful background flush operation.
pub struct FlushResult {
    /// ID of the MemTable that was flushed.
    pub memtable_id: u64,
    /// Absolute or relative path to the generated SSTable file.
    pub sstable_path: String,
}

/// A task dispatched to the background flusher.
pub enum FlushTask {
    /// Instructs the flusher to write the given Immutable MemTable to disk.
    Task(Arc<MemTable>),
    /// Signals the background flusher to shutdown.
    Shutdown,
}

use lake::thread_pool::ThreadPool;

/// Background task orchestrator for flushing Immutable MemTables to disk.
/// Uses a thread pool (`lake::ThreadPool`) to execute I/O concurrently.
pub struct Flusher {
    task_rx: mpsc::Receiver<FlushTask>,
    result_tx: mpsc::Sender<Result<FlushResult>>,
    sst_dir: PathBuf,
    pool: ThreadPool,
}

impl Flusher {
    /// Creates a new `Flusher` instance with a dedicated thread pool.
    pub fn new(
        result_tx: mpsc::Sender<Result<FlushResult>>,
        task_rx: mpsc::Receiver<FlushTask>,
        sst_dir: impl Into<PathBuf>,
        pool_size: usize,
    ) -> Self {
        Self {
            task_rx,
            result_tx,
            sst_dir: sst_dir.into(),
            pool: ThreadPool::new(pool_size),
        }
    }

    /// Spawns the main dispatch loop in a background thread.
    /// Listens for `FlushTask`s and delegates them to the `lake` thread pool.
    pub fn spawn(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            while let Ok(task) = self.task_rx.recv() {
                match task {
                    FlushTask::Task(mem_table) => {
                        let id = mem_table.id;
                        let sst_name = format!("sst_{:06}.sst", id);
                        let sst_path = self.sst_dir.join(&sst_name);
                        let result_tx = self.result_tx.clone();

                        self.pool.execute(move || {
                            let builder = SSTableBuilder::new(sst_path.to_str().unwrap());

                            let iter = mem_table.iter().map(|(k, v)| (k.clone(), v.clone()));
                            if let Err(e) = builder.build(iter) {
                                let _ = result_tx.send(Err(e));
                                return;
                            }

                            let _ = result_tx.send(Ok(FlushResult {
                                memtable_id: id,
                                sstable_path: sst_path.to_string_lossy().into_owned(),
                            }));
                        });
                    }
                    FlushTask::Shutdown => break,
                }
            }
        })
    }
}

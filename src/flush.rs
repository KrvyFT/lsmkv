use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task;

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
    Task(Arc<std::sync::RwLock<MemTable>>),
}

/// Background task orchestrator for flushing Immutable MemTables to disk.
/// Uses tokio's `spawn_blocking` to execute I/O concurrently.
pub struct Flusher {
    task_rx: mpsc::Receiver<FlushTask>,
    result_tx: mpsc::Sender<Result<FlushResult>>,
    sst_dir: PathBuf,
}

impl Flusher {
    /// Creates a new `Flusher` instance.
    pub fn new(
        result_tx: mpsc::Sender<Result<FlushResult>>,
        task_rx: mpsc::Receiver<FlushTask>,
        sst_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            task_rx,
            result_tx,
            sst_dir: sst_dir.into(),
        }
    }

    /// Spawns the main dispatch loop in an async tokio task.
    /// Listens for `FlushTask`s and delegates them to the blocking thread pool.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(task_item) = self.task_rx.recv().await {
                match task_item {
                    FlushTask::Task(mem_table_lock) => {
                        let result_tx = self.result_tx.clone();
                        let sst_dir = self.sst_dir.clone();

                        task::spawn_blocking(move || {
                            // Obtain read lock for the memtable
                            let mem_table = mem_table_lock.read().unwrap();
                            let id = mem_table.id;
                            let sst_name = format!("sst_{:06}.sst", id);
                            let sst_path = sst_dir.join(&sst_name);

                            let builder = SSTableBuilder::new(sst_path.to_str().unwrap());

                            let iter = mem_table.iter().map(|(k, v)| (k.clone(), v.clone()));
                            if let Err(e) = builder.build(iter) {
                                // Blockingly send the result back (channel is unbounded or large enough).
                                // To call async send in spawn_blocking, we use blocking_send if possible.
                                // Actually, tokio mpsc Sender can only be used with `.await` or `try_send`.
                                // Since we need to send, we can use `blocking_send()`.
                                let _ = result_tx.blocking_send(Err(e));
                                return;
                            }

                            let _ = result_tx.blocking_send(Ok(FlushResult {
                                memtable_id: id,
                                sstable_path: sst_path.to_string_lossy().into_owned(),
                            }));
                        });
                    }
                }
            }
        })
    }
}

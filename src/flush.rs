use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;

use crate::{error::Result, memtable::MemTable, sstable::sstable_builder::SSTableBuilder};

pub struct FlushResult {
    pub memtable_id: u64,
    pub sstable_path: String,
}

pub enum FlushTask {
    Task(Arc<MemTable>),
    Shutdown,
}

use lake::thread_pool::ThreadPool;

pub struct Flusher {
    task_rx: mpsc::Receiver<FlushTask>,
    result_tx: mpsc::Sender<Result<FlushResult>>,
    sst_dir: PathBuf,
    pool: ThreadPool,
}

impl Flusher {
    pub fn new(
        task_rx: mpsc::Receiver<FlushTask>,
        result_tx: mpsc::Sender<Result<FlushResult>>,
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

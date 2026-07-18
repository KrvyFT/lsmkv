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

    /// 启动后台分发线程，使用 lake 线程池进行实际刷盘
    pub fn spawn(self) -> thread::JoinHandle<()> {
        thread::spawn(move || {
            while let Ok(task) = self.task_rx.recv() {
                match task {
                    FlushTask::Task(imm) => {
                        let id = imm.id;
                        let sst_name = format!("sst_{:06}.sst", id);
                        let sst_path = self.sst_dir.join(&sst_name);
                        let result_tx = self.result_tx.clone();

                        self.pool.execute(move || {
                            // 初始化 SSTable 构建器
                            let builder = SSTableBuilder::new(sst_path.to_str().unwrap());

                            // 将 MemTable 的数据克隆出来传入构建器 (底层写入硬盘)
                            let iter = imm.iter().map(|(k, v)| (k.clone(), v.clone()));

                            // 执行落盘
                            if let Err(e) = builder.build(iter) {
                                let _ = result_tx.send(Err(e));
                                return;
                            }

                            // 将成功落盘的消息发回主线程
                            let _ = result_tx.send(Ok(FlushResult {
                                memtable_id: id,
                                sstable_path: sst_path.to_string_lossy().into_owned(),
                            }));
                        });
                    }
                    FlushTask::Shutdown => {
                        break;
                    }
                }
            }
        })
    }
}

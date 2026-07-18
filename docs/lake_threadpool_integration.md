# 后台并发刷盘：Lake 线程池集成架构设计

本阶段的开发文档记录了如何将本地的 `lake` 线程池集成到 LSM-Tree 的后台落盘 (Flush) 任务中，以解决单线程阻塞导致写入吞吐量下降的瓶颈问题。

---

## 1. 为什么需要线程池？ (Background)

在 LSM-Tree 中，当内存表 (`MemTable`) 写满时，需要将其转化为只读的 `Immutable MemTable`，并由后台异步写入磁盘成为 SSTable。
早期的设计是：通过 `std::thread::spawn` 启动一个单例的工作线程，通过 `mpsc::Receiver` 串行处理刷盘请求。

- **痛点**：由于磁盘 I/O 较慢，如果在高吞吐量下连续产生多个满的 `MemTable`，单线程串行刷盘会形成任务积压。如果 `Immutable MemTables` 过多，将导致巨大的内存压力并拖慢查询性能。

**解决方案**：利用多线程并发写入不同的 SSTable，极大提升 I/O 效率。我们采用了定制开发的 `lake::thread_pool::ThreadPool`。

---

## 2. 架构设计：Dispatcher 与 Worker 协作模型

目前的 `Flusher` 采用的是 `Dispatcher-Worker` (分发-工作) 模型：

```mermaid
graph TD
    A[DbKernel (主线程)] -->|mpsc::Sender<FlushTask>| B[Flusher 分发线程]
    B -->|pool.execute()| C(Lake ThreadPool Worker 1)
    B -->|pool.execute()| D(Lake ThreadPool Worker 2)
    B -->|pool.execute()| E(Lake ThreadPool Worker 3)
    
    C -.->|mpsc::Sender<Result<FlushResult>>| A
    D -.->|mpsc::Sender<Result<FlushResult>>| A
    E -.->|mpsc::Sender<Result<FlushResult>>| A
```

1. **主线程 (`DbKernel`)**：非阻塞地把需要刷盘的 `Arc<MemTable>` 扔进 `task_rx` 管道。
2. **分发线程 (`Dispatcher`)**：`Flusher::spawn` 启动的唯一的常驻后台线程，只负责 `while let Ok(task) = task_rx.recv()` 接单，不负责实际干活。
3. **线程池 (`Lake ThreadPool`)**：分发线程接到任务后，将其包装为闭包通过 `pool.execute` 扔给底层的 Worker 线程并发执行昂贵的 SSTable I/O。

---

## 3. 具体代码实现 (Implementation Details)

### 3.1 引入本地依赖

在 `Cargo.toml` 中引入了工作区本地自研的 `lake` 库，作为引擎的基础设施：

```toml
[dependencies]
lake = { path = "src/lake" }
```

### 3.2 改造 `Flusher` 结构体

我们在 `Flusher` 中持有 `lake::thread_pool::ThreadPool` 的实例。

```rust
use lake::thread_pool::ThreadPool;

pub struct Flusher {
    task_rx: mpsc::Receiver<FlushTask>,
    result_tx: mpsc::Sender<Result<FlushResult>>, // 用于向主线程回传结果
    sst_dir: PathBuf,
    pool: ThreadPool, // 持有 lake 线程池
}
```

### 3.3 闭包与环境捕获 (Ownership & Closure)

并发带来的挑战是变量的生命周期与所有权转移。
由于 `pool.execute` 需要 `move` 环境参数，我们必须克隆相关的路径和消息发送句柄，并将 `MemTable` (通过 `Arc`) 借给闭包。

```rust
// 摘自 src/flush_task.rs
self.pool.execute(move || {
    let builder = SSTableBuilder::new(sst_path.to_str().unwrap());
    
    // 把 Arc<MemTable> 内部数据 clone 出来生成可以转移的迭代器
    let iter = imm.iter().map(|(k, v)| (k.clone(), v.clone()));
    
    if let Err(e) = builder.build(iter) {
        let _ = result_tx.send(Err(e));
        return;
    }

    // Worker 线程将成功落盘的消息发回主线程
    let _ = result_tx.send(Ok(FlushResult {
        memtable_id: id,
        sstable_path: sst_path.to_string_lossy().into_owned(),
    }));
});
```

---

## 4. 后续任务 (Next Steps)

现在并发刷盘已经完全打通，但闭环尚未完成：**主线程 (`DbKernel`) 还没有消费 `FlushResult`。**

在接下来的开发中，需要：

1. 在主线程读写操作的间隙，轮询 `result_tx` 通道。
2. 一旦收到 `FlushResult`，实例化对应的 `SSTable` 加载到读取路径中。
3. 从 `imm_memtables` 中删除已经被持久化的 `MemTable`。
4. 删除对应该 `MemTable` 编号的旧 `.log` 文件，完成磁盘空间的垃圾回收。

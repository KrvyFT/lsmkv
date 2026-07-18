# 阶段 3 与阶段 4：WAL 轮转与后台 Flush 同步机制设计

本设计文档详细阐述了如何在当前 LSM-KV 架构中实现 Write-Ahead Log (WAL) 的文件轮转、崩溃恢复，以及如何优雅地处理后台 MemTable 落盘后的状态同步。

---

## 阶段 3：WAL 轮转与恢复 (WAL Rotation & Recovery)

为了保证系统能够在意外崩溃后无损恢复数据，并且防止 WAL 文件无限膨胀，我们需要引入文件轮转机制。

### 3.1 核心状态标识 `File ID`

引入一个单调递增的 `file_id` (通常是一个 `u64`)。

- 每个 `MemTable` 都有一个对应的 `file_id`。
- 对应的 WAL 文件命名规范为：`wal_{file_id:06}.log` (例如 `wal_000001.log`)。

### 3.2 轮转逻辑 (Rotation)

在 `DbKernel::write` 中，当活跃的 `MemTable` 达到容量上限触发 Flush 时：

1. `DbKernel` 递增本地的 `next_file_id` 计数器。
2. 调用 `self.wal.rotate(next_file_id)`，这会：
   - 强制调用底层文件的 `sync_all` 确保旧数据安全落盘。
   - 关闭旧的 WAL 文件句柄。
   - 创建并打开新的 `wal_{next_file_id:06}.log` 文件供接下来的写入使用。

### 3.3 崩溃恢复机制 (Crash Recovery)

系统启动 (`DbKernel::open`) 时的流程：

1. **扫描目录**：扫描数据库目录下的所有 `wal_*.log` 文件，并按 `file_id` 从小到大排序。
2. **重放日志 (Replay)**：
   - 读取较旧的 WAL 文件，将其重放 (Replay) 到临时的 MemTable 中。如果该 MemTable 写满了，直接压入 `imm_memtables` 并排队等待 Flush。
   - 读取最新（最后一个）WAL 文件，将其重放到活跃的 `self.memtable` 中。
3. **记录状态**：将 `next_file_id` 恢复为 `最新 WAL ID + 1`。

---

## 阶段 4：后台 Flush 的结果同步 (Background Flush Sync)

当后台线程或者异步任务将 `imm_memtable` 成功写入到磁盘成为 SSTable 后，我们需要一种线程安全的机制来通知主线程 (`DbKernel`)，以更新读取路径 (Read Path)。

### 4.1 引入 `FlushResult` 与 回调/通知 Channel

由于主线程不能被阻塞，我们需要一个用于接收后台成功消息的 Channel：

```rust
pub struct FlushResult {
    pub memtable_id: u64,           // 用于定位是哪个 MemTable 被成功落盘了
    pub sstable_path: String,       // 生成的 SSTable 文件路径
}

pub struct DbKernel {
    // ... 其他字段
    flush_rx: mpsc::Receiver<Result<FlushResult, DbError>>, 
    sstables: Vec<SSTable>,         // 新增：维护当前的 SSTable 列表
}
```

### 4.2 状态流转机制 (State Machine Transition)

主线程需要定期（或者在每次读/写请求之前）轮询 `flush_rx`：

```rust
// 这是一个内部的同步函数，可以叫 tick() 或者 try_sync_flush_results()
fn try_sync_flush_results(&mut self) {
    while let Ok(result) = self.flush_rx.try_recv() {
        let flush_res = result.unwrap();
        
        // 1. 将新生成的 SSTable 挂载到读取路径中
        let sst = SSTable::open(&flush_res.sstable_path).unwrap();
        self.sstables.push(sst);

        // 2. 从 imm_memtables 中安全移除对应的旧 MemTable
        self.imm_memtables.retain(|imm| imm.get_id() != flush_res.memtable_id);

        // 3. 删除对应的旧 WAL 文件
        std::fs::remove_file(format!("wal_{:06}.log", flush_res.memtable_id)).unwrap();
    }
}
```

### 4.3 读取路径更新与竞争避免

有了上述机制后，`DbKernel::get` 的逻辑将被扩展：

1. `try_sync_flush_results(&mut self)` 确保状态最新。
2. 查活跃 `memtable`。
3. 查 `imm_memtables`。
4. 查 `sstables` (从最新生成的 SSTable 开始查起)。

**为什么这样设计能避免竞争？**
因为从 `imm_memtables` 移除和向 `sstables` 添加的操作是在主线程同一个函数 (`try_sync_flush_results`) 中同步完成的。对于任何一个查询请求，数据要么在 `imm_memtable` 里，要么已经无缝转移到了 `sstables` 里，绝对不会出现数据在那一瞬间被漏查的真空期。

---

## 下一步建议 (Action Items)

如果要实操这两部分，建议的开发顺序如下：

1. **先做阶段 3 (部分)**：改造 `MemTable` 增加 `id` 属性，改造 `WalWriter` 支持接收 `id` 生成文件和 `rotate` 方法。
2. **打通后台反馈机制 (阶段 4)**：在 `FlushTask` 发送时，传入一个专门的反馈 channel。后台线程通过该 channel 把完成的 SSTable 元数据回传。
3. **完成垃圾回收**：在主线程收到反馈后，补充删除旧 WAL 的代码，完成闭环。

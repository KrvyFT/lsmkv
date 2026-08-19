# LSM-Tree KV Database TODO List & Roadmap

## 核心架构 (Core Architecture)

- [x] **Actor 写入模型**: 使用 MPSC + Oneshot 实现基于单线程任务的无锁化写入 (`writer_task`)。
- [x] **组提交 (Group Commit) 双水位线限制**: 引入 `1MB` 和 `1000条` 的双重限制，防止 `MemTable` 击穿，缓解尾延迟 (Tail Latency) 飙升。
- [ ] **背压管理 (Backpressure)**: 监控 `writer_task` 压力，或引入带有超时和显式阻断的更优背压机制。
- [ ] **配置化抽离 (Configuration)**: 提取硬编码的配置（如 `MEM_TABLE_MAX_SIZE`, `MAX_BATCH_BYTES`, `MAX_BATCH_COUNT` 等）至 `DbOptions`，以便根据硬件差异调整。

## WAL & Recovery

- [x] 支持在系统启动时，按序列号依次加载 WAL 恢复 `MemTable` 状态。
- [ ] 优化 WAL 并行恢复，或引入 Checkpoint 机制以截断过旧的 WAL，加速启动过程。

## 内存管理 (Memory Management)

- [ ] 为 `MemTable` 引入自定义分配器 (Arena Allocator) 降低碎片的产生，提高 GC 性能。

## 测试与监控 (Testing & Metrics)

- [ ] 补充高并发持续写入且带有随机大小 Payload 的极端场景 (Fuzz Testing) 压测。
- [ ] 增加 Prometheus Metrics 埋点 (监控 QPS, P99 Latency, Compaction 耗时等)。

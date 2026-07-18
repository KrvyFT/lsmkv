# 湖 (Lake) - Rust 线程池核心架构与开发文档

> [!NOTE]
> 本文档旨在指导一个用于学习目的但具备生产级架构思维的 Rust 线程池库的设计。库名暂定为 `lake`。

## 1. 系统架构与模块划分 (Architecture & Modules)

线程池的本质是一个经典的**生产者-消费者 (Producer-Consumer)** 模型。在 Rust 中，我们需要通过安全的数据流和生命周期管理来实现这一点。

系统主要分为三个核心组件：

1. **`ThreadPool` (生产者)**：对外暴露 API，负责接收闭包任务并将其发送到通道。管理 Worker 的生命周期。
2. **`Worker` (消费者)**：驻留的系统线程，循环监听并从通道接收任务进行执行。
3. **`Channel` (通信机制)**：用于协调任务分发的 MPSC (Multiple Producer, Single Consumer) 通道。在多 Worker 场景下，接收端 (Receiver) 需要被共享。

---

## 2. 核心数据结构与命名规范 (Data Structures)

### 2.1 任务类型 (`Job`)

由于任务是跨线程传递并在未来执行的闭包，我们需要满足 `Send` 和 `'static` 约束。为了避免装箱 (Boxing) 开销，我们在极致性能下可以考虑泛型，但为了通用性和避免单态化导致的编译体积膨胀，动态分发 (Trait Object) 是首选。

```rust
/// 表示一个可被线程池执行的无参无返回值的任务闭包。
pub type Job = Box<dyn FnOnce() + Send + 'static>;
```

### 2.2 通信消息 (`Message`)

使用 `enum` 来明确表达控制流。这避免了依赖特殊标志位（如 `null`）来退出线程的 bad smell。

```rust
pub enum Message {
    /// 包含需要执行的新任务
    NewJob(Job),
    /// 通知 Worker 终止执行
    Terminate,
}
```

### 2.3 线程池与工作者 (`ThreadPool` & `Worker`)

```rust
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

pub struct ThreadPool {
    /// 存储所有活跃的工作线程
    workers: Vec<Worker>,
    /// 任务发送端，使用 Option 以便于在 Drop 时安全地 take 出来并 drop
    sender: Option<mpsc::Sender<Message>>,
}

struct Worker {
    /// 工作线程的唯一标识符
    id: usize,
    /// 线程句柄。使用 Option 以便在优雅停机时使用 take() 获取所有权并 join
    thread: Option<thread::JoinHandle<()>>,
}
```

---

## 3. 内存语义与并发安全 (Memory Semantics & Concurrency)

### 3.1 共享接收端 (Shared Receiver)

`mpsc` 通道的接收端不支持多消费者并发拉取。我们需要将其封装为 `Arc<Mutex<mpsc::Receiver<Message>>>`：

- **`Arc`**：提供引用计数，允许多个 Worker 线程安全地拥有 Receiver 的所有权。
- **`Mutex`**：确保同一时刻只有一个 Worker 能从通道中 `recv()` 获取任务，避免数据竞争。

> [!WARNING]
> **死锁防御 (Deadlock Prevention)**
> 在 Worker 内部，**严禁**使用 `while let Ok(job) = receiver.lock().unwrap().recv()`。
> 因为 `lock()` 产生的 `MutexGuard` 的生命周期会延续到整个 block 结束，导致其他线程永远无法获取锁。
> **正确做法**：必须让 `MutexGuard` 在 `recv()` 返回后立即被丢弃：
>
> ```rust
> let message = receiver.lock().unwrap().recv().unwrap();
> ```

### 3.2 优雅闭环与异常处理 (Graceful Shutdown & Error Handling)

- **拒绝 `unwrap()` 滥用**：在创建线程时，使用 `std::thread::Builder::new()` 而不是 `spawn()`，以便捕获操作系统级线程创建失败（如 OOM 或系统限制），并返回 `Result`，拒绝直接 Panic。
- **实现 `Drop` 特征**：当 `ThreadPool` 离开作用域时，必须向所有 Worker 发送 `Message::Terminate`，并显式调用 `join()`，确保所有正在执行的任务安全落盘。

---

## 4. 技术选型权衡 (Trade-offs)

### 4.1 任务调度 (MPSC vs. Work Stealing)

- **当前方案 (MPSC + Mutex)**：所有线程竞争同一个锁。
  - *优点*：实现极其简单，适合学习基础的并发原语。
  - *缺点*：锁竞争激烈（Contention），在核心数极高时，`Mutex` 成为明显的性能瓶颈。
- **演进方案 (Work Stealing)**：每个线程拥有自己的本地队列，空闲时从其他线程队列“窃取”任务（如 `crossbeam-deque` 或 `rayon` 的实现）。
  - *建议*：第一版保持 MPSC，后续作为性能优化课题引入 `crossbeam-channel` 替代标准库。

### 4.2 Panic 恢复机制 (Resilience)

- **风险暴露**：如果用户提交的闭包发生 Panic，会导致执行该任务的 Worker 线程崩溃退出。长此以往，线程池的可用线程数会归零。
- **防御策略**：
  1. 捕获 Panic：使用 `std::panic::catch_unwind` 包裹任务执行，防止线程崩溃。
  2. 监控与替补：如果线程意外退出，`ThreadPool` 应该能感知并重新生成新的 `Worker`。

---

## 5. 验收标准与测试驱动 (TDD)

为了确保 API 的健壮性，请编写以下测试用例：

1. **Happy Path**：提交 100 个简单的计数任务，验证执行结果完整。
2. **并发数据竞争测试**：多个任务并发累加同一个 `Arc<AtomicUsize>`，验证最终值无丢失。
3. **Panic 恢复测试**（高阶）：提交一个必定 Panic 的任务，验证线程池后续仍能正常处理其他任务。

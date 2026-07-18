# LSM-KV 系统逻辑审查与后续设计指南

在最近的代码变更中，我们在 `DbKernel` 与 `MemTable` 的整合及刷盘（Flush）触发机制上取得了进展。但深入审查目前的逻辑，发现了几处关键的**一致性与正确性问题**，这些问题在典型的 LSM-Tree 架构中是致命的。

为了保证下一步开发的稳健性，以下是逻辑错误分析及修复路径。

## 一、 当前核心逻辑错误分析 (Logic Errors)

### 1. `MemTable` 查询语义丢失 (Tombstone Semantics Loss)

**问题代码位置**：`src/memtable.rs` 中的 `get` 方法。

```rust
pub fn get(&self, key: &Key) -> Option<&Value> {
    self.map.get(key).and_then(|v| v.as_ref())
}
```

**错误剖析**：
在 LSM 结构中，如果用户删除了一个 Key，我们会插入一个墓碑（Tombstone，在你的代码中表示为 `None`）。如果 `memtable.get` 遇到墓碑，它**必须明确告知调用方该 Key 已被删除**，以便上层（`DbKernel`）停止向下层的 SSTables 继续搜索。
而当前的实现中，如果 Key 是 Tombstone（`Some(None)`），它会返回 `None`；如果 Key 完全不存在于此 MemTable，它也会返回 `None`。这导致 `DbKernel` 无法区分“已删除”和“未命中”，从而引发**已删除数据复活**的幽灵读取（Ghost Reads）问题。

### 2. `DbKernel::get` 中空值处理错误 (Empty Value Handling)

**问题代码位置**：`src/db_kernel.rs` 中的 `get` 方法。

```rust
if let Some(v) = self.memtable.get(k) {
    if v.is_empty() { // 严重逻辑错误！
        return Err(DbError::NotFound);
    }
    return Ok(v.clone());
}
```

**错误剖析**：
在 KV 存储中，`Key -> 空字节数组 (Vec::new())` 是一个完全合法的状态！`v.is_empty()` 只是意味着这个 Value 长度为 0，这并不等同于 `NotFound`。真正判定 NotFound 应该依赖 MemTable 明确返回“未命中”或者“墓碑”。

### 3. 读取路径的并发一致性问题 (Read-Path Inconsistency)

**问题代码位置**：`src/db_kernel.rs` 中的 `write` 方法。

```rust
if self.memtable.approx_size >= MEM_TABLE_MAX_SIZE {
    let old_memtable = std::mem::replace(&mut self.memtable, MemTable::new());
    self.flush_tx.send(FlushTask::Task(Arc::new(old_memtable))).unwrap();
}
```

**错误剖析**：
当活跃 MemTable 被取出并发送到 `flush_tx` 后台线程时，它立即从 `DbKernel` 的上下文中消失了。
如果此时（后台尚未将其写成 SSTable 时）发生一个 `get` 请求，因为 `self.memtable` 已被重置为空，且系统没有记录正在被 Flush 的 MemTable（通常称为 `Immutable MemTable`），**该请求将查不到刚刚写入的数据**，导致明显的读写不一致。

### 4. Write-Ahead Log (WAL) 缺失轮转机制 (Missing WAL Rotation)

**错误剖析**：
每次我们创建一个新的 MemTable，都应该伴随着生成一个新的 WAL 文件，同时封闭（Seal）旧的 WAL 文件。现在的 `self.wal` 没有被重置，它会无限膨胀。更危险的是，在崩溃恢复（Crash Recovery）时，系统将无法对应哪一段 WAL 数据属于已经落盘的 SSTable，哪一段属于丢失的 MemTable。

---

## 二、 后续步骤与架构修正方案 (Next Steps)

针对上述问题，以下是接下来几步需要着手修复和实现的任务清单。

### 阶段 1：修复 MemTable 与 Get 的查询语义

1. **重构查询返回值**：引入显式的枚举以表达查询状态。

   ```rust
   // src/model.rs 或者 src/memtable.rs
   pub enum ValueOption {
       Some(Value),
       Deleted,   // 碰到了墓碑
       NotFound,  // 当前层级/表未命中，需要继续向下查
   }
   ```

2. **修改 `MemTable::get`**：使其返回上述枚举，而不是标准库的 `Option`。
3. **修正 `DbKernel::get`**：去除 `v.is_empty()` 判断，根据新的枚举决定是返回 `Ok(v)`、`Err(NotFound)` 还是去下一层级查找。

### 阶段 2：引入 Immutable MemTables 维护读一致性

1. **扩展 `DbKernel` 结构体**：

   ```rust
   pub struct DbKernel {
       memtable: MemTable,
       imm_memtables: Vec<Arc<MemTable>>, // 正在等待或正在进行 Flush 的 MemTables
       // ... 其他字段
   }
   ```

2. **更新 Flush 逻辑**：
   当达到阈值时，`old_memtable` 包装进 `Arc`，不但要发给 `flush_tx`，还要 `clone` 一份压入 `imm_memtables` 中。
3. **更新读取路由**：
   `DbKernel::get` 的查询顺序应该是：`活跃 memtable` -> `反向遍历 imm_memtables` -> `SSTables`。

### 阶段 3：WAL 轮转与恢复 (WAL Rotation & Recovery)

1. 设计一个命名规范，比如 `wal_0001.log`, `wal_0002.log` 等，通过一个单调递增的文件 ID 标识。
2. 在触发 `MemTable` Flush 时，调用 `self.wal.rotate(new_file_id)`。

### 阶段 4：后台 Flush 的结果同步

1. 此时后台虽然能接收 `FlushTask`，但 Flush 成功后怎么通知 `DbKernel`？
2. 需要引入某种机制（如接收 Channel，或者共享的 `Manifest/VersionSet` 状态机），让主线程能够将新生成的 SSTable 登记到读取路径中，同时从 `imm_memtables` 中移除对应的旧 MemTable。

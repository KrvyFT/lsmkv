use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[path = "../protocol.rs"]
mod protocol;
use protocol::{Request, Response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("==================================================");
    println!("🚀 LSM-Tree Rust 极致并发压测工具 (Benchmark)");
    println!("==================================================");

    // 压测参数配置
    let total_requests = 100_000_0; // 总请求数 10 万
    let concurrency = 1000; // 维持 100 个并发长连接
    let requests_per_conn = total_requests / concurrency;

    println!("📊 测试参数:");
    println!("   总写入量: {} 条", total_requests);
    println!("   并发连接数: {} 个", concurrency);
    println!("   单连接任务: {} 条", requests_per_conn);
    println!("--------------------------------------------------");
    println!("⏳ 正在压测中，请稍候...\n");

    // 记录开始时间
    let start_time = Instant::now();

    // 统计成功和失败数量
    let success_count = Arc::new(Mutex::new(0usize));
    let fail_count = Arc::new(Mutex::new(0usize));

    let mut tasks = vec![];

    for conn_id in 0..concurrency {
        let success_clone = Arc::clone(&success_count);
        let fail_clone = Arc::clone(&fail_count);

        let task = tokio::spawn(async move {
            let addr = "127.0.0.1:8080";

            // 建立持久连接（长连接）
            let stream = match TcpStream::connect(addr).await {
                Ok(s) => s,
                Err(_) => {
                    *fail_clone.lock().await += requests_per_conn;
                    return;
                }
            };

            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

            let mut local_success = 0;
            let mut local_fail = 0;

            for i in 0..requests_per_conn {
                // 构造 Key 和 Value
                let key_str = format!("bench_key_{}_{}", conn_id, i);
                let val_str = format!("bench_value_{}_{}", conn_id, i);

                let req = Request::Put {
                    key: key_str.into_bytes(),
                    value: val_str.into_bytes(),
                };

                let req_bytes = bincode::serialize(&req).unwrap();

                // 发送请求
                if framed.send(Bytes::from(req_bytes)).await.is_err() {
                    local_fail += 1;
                    continue;
                }

                // 接收响应
                match framed.next().await {
                    Some(Ok(resp_bytes)) => {
                        if let Ok(Response::Ok(_)) = bincode::deserialize::<Response>(&resp_bytes) {
                            local_success += 1;
                        } else {
                            local_fail += 1;
                        }
                    }
                    _ => {
                        local_fail += 1;
                    }
                }
            }

            // 汇总当前协程的结果
            *success_clone.lock().await += local_success;
            *fail_clone.lock().await += local_fail;
        });

        tasks.push(task);
    }

    // 等待所有并发协程完成
    for task in tasks {
        let _ = task.await;
    }

    let elapsed = start_time.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let qps = total_requests as f64 / elapsed_secs;

    let final_success = *success_count.lock().await;
    let final_fail = *fail_count.lock().await;

    println!("==================================================");
    println!("🎉 压测完成!");
    println!("⏱️  总耗时:   {:.2} 秒", elapsed_secs);
    println!("🚀 QPS:      {:.2} 次写入/秒", qps);
    println!("✅ 成功写入: {}", final_success);
    if final_fail > 0 {
        println!("❌ 失败写入: {}", final_fail);
    }
    println!("==================================================");

    Ok(())
}

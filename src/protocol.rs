use serde::{Deserialize, Serialize};

/// 客户端请求格式
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// 插入一条数据
    Put { key: Vec<u8>, value: Vec<u8> },
    /// 查询一条数据
    Get { key: Vec<u8> },
    /// 删除一条数据
    Delete { key: Vec<u8> },
}

/// 服务端响应格式
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// 成功，Get 可能会返回数据
    Ok(Option<Vec<u8>>),
    /// 失败并返回错误信息
    Err(String),
}

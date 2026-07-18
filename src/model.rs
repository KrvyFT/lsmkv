use serde::{Deserialize, Serialize};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Serialize, Deserialize, Debug)]
pub enum RecordType {
    Put,
    Delete,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogRecord {
    pub r_type: RecordType,
    pub key: Key,
    pub value: Option<Value>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum GetResult<T> {
    Found(T),
    Deleted,
    NotFound,
}

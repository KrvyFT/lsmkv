use serde::{Deserialize, Serialize};

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// Represents the type of a WAL log record or an SSTable entry.
#[derive(Serialize, Deserialize, Debug)]
pub enum RecordType {
    /// A normal put/update operation.
    Put,
    /// A tombstone representing a deletion.
    Delete,
}

/// A single entry in the WAL or SSTable.
#[derive(Serialize, Deserialize, Debug)]
pub struct LogRecord {
    /// Whether this is a Put or a Delete.
    pub r_type: RecordType,
    /// The key.
    pub key: Key,
    /// The value (None if it is a Delete).
    pub value: Option<Value>,
}

/// The result of a key lookup.
/// Distinguishes between successfully finding a value, encountering a Tombstone, and not finding the key at all.
#[derive(Debug, PartialEq, Eq)]
pub enum GetResult<T> {
    /// Key was found and a value is returned.
    Found(T),
    /// Key was explicitly deleted (Tombstone found).
    Deleted,
    /// Key was not found in the current component.
    NotFound,
}

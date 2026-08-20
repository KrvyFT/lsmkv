//! Core data structures and type definitions.
//!
//! This module defines the common vocabulary used throughout the LSM-Tree,
//! including representations for Keys, Values, and the result types of database queries.

use serde::{Deserialize, Serialize};

/// Type alias for keys, which are arbitrary byte arrays.
pub type Key = Vec<u8>;
/// Type alias for values, which are arbitrary byte arrays.
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

    /// An error occurred during retrieval (e.g. underlying file corruption or decompression failure).
    Error(String),
}

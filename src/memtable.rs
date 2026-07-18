use std::collections::BTreeMap;

use crate::{
    error::DbError,
    model::{GetResult, Key, Value},
};

/// Maximum approximate size of a MemTable before it gets flushed to disk.
pub static MEM_TABLE_MAX_SIZE: usize = 4 * 1024 * 1024;

/// An in-memory key-value store backed by a BTreeMap.
/// This is the first level of storage in the LSM-Tree where all recent writes are buffered.
pub struct MemTable {
    /// A unique identifier for the MemTable, usually corresponding to its WAL file ID.
    pub id: u64,
    map: BTreeMap<Key, Option<Value>>,
    /// Approximate memory footprint of the keys and values in bytes.
    pub approx_size: usize,
}

impl MemTable {
    /// Creates a new, empty MemTable.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    /// Inserts or updates a key-value pair in the MemTable.
    /// A `None` value represents a Tombstone (deletion).
    pub fn put(&mut self, key: Key, value: Option<Value>) {
        let key_len = key.len();
        self.approx_size += key_len + value.as_ref().map_or(0, |v| v.len());
        if let Some(old_value) = self.map.insert(key, value) {
            self.approx_size -= key_len + old_value.map_or(0, |v| v.len());
        }
    }

    /// Retrieves the value associated with the given key.
    pub fn get(&self, key: &Key) -> GetResult<&Value> {
        match self.map.get(key) {
            Some(Some(v)) => GetResult::Found(v),
            Some(None) => GetResult::Deleted,
            None => GetResult::NotFound,
        }
    }

    /// Inserts a Tombstone for the given key, effectively deleting it.
    pub fn delete(&mut self, key: &Key) -> Result<(), DbError> {
        self.approx_size += key.len();
        if let Some(old_opt) = self.map.insert(key.clone(), None) {
            self.approx_size -= key.len() + old_opt.map_or(0, |v| v.len());
        }
        Ok(())
    }

    /// Returns an iterator over the key-value pairs in the MemTable.
    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Option<Value>)> {
        self.map.iter()
    }
}

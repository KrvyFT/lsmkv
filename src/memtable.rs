use std::collections::BTreeMap;

use crate::{
    error::DbError,
    model::{Key, RecordType, Value},
};

pub struct MemTable {
    map: BTreeMap<Key, Option<Value>>,
    pub approx_size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    pub fn put(&mut self, key: Key, value: Value) {
        let key_len = key.len();
        self.approx_size += key_len + value.len();
        if let Some(old_value) = self.map.insert(key, Some(value)) {
            self.approx_size -= key_len + old_value.map_or(0, |v| v.len());
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Option<Value>> {
        self.map.get(key)
    }

    pub fn delete(&mut self, key: &Key) -> Result<(), DbError> {
        self.approx_size += key.len();
        if let Some(old_opt) = self.map.insert(key.clone(), None) {
            self.approx_size -= key.len() + old_opt.map_or(0, |v| v.len());
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.approx_size = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Option<Value>)> {
        self.map.iter()
    }
}

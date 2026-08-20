use std::path::PathBuf;
use crate::sstable::sstable_builder::CompressionType;

#[derive(Clone, Debug)]
pub struct DbOptions {
    pub dir: PathBuf,
    pub mem_table_max_size: usize,
    pub max_batch_bytes: usize,
    pub max_batch_count: usize,
    pub sstable_block_size: usize,
    pub compression_type: CompressionType,
    pub compression_level: i32,
    pub max_write_queue_size: usize,
    pub flush_queue_size: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("lsmkv_db"),
            mem_table_max_size: 4 * 1024 * 1024,
            max_batch_bytes: 1024 * 1024,
            max_batch_count: 1000,
            sstable_block_size: 4096,
            compression_type: CompressionType::Zstd,
            compression_level: 3,
            max_write_queue_size: 1024,
            flush_queue_size: 100,
        }
    }
}

impl DbOptions {
    pub fn builder() -> DbOptionsBuilder {
        DbOptionsBuilder::default()
    }
}

pub struct DbOptionsBuilder {
    options: DbOptions,
}

impl Default for DbOptionsBuilder {
    fn default() -> Self {
        Self {
            options: DbOptions::default(),
        }
    }
}

impl DbOptionsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.options.dir = path.into();
        self
    }

    pub fn mem_table_max_size(mut self, size: usize) -> Self {
        self.options.mem_table_max_size = size;
        self
    }

    pub fn max_batch_bytes(mut self, bytes: usize) -> Self {
        self.options.max_batch_bytes = bytes;
        self
    }

    pub fn max_batch_count(mut self, count: usize) -> Self {
        self.options.max_batch_count = count;
        self
    }

    pub fn sstable_block_size(mut self, size: usize) -> Self {
        self.options.sstable_block_size = size;
        self
    }

    pub fn compression_type(mut self, c_type: CompressionType) -> Self {
        self.options.compression_type = c_type;
        self
    }

    pub fn compression_level(mut self, level: i32) -> Self {
        self.options.compression_level = level;
        self
    }

    pub fn max_write_queue_size(mut self, size: usize) -> Self {
        self.options.max_write_queue_size = size;
        self
    }

    pub fn flush_queue_size(mut self, size: usize) -> Self {
        self.options.flush_queue_size = size;
        self
    }

    pub fn build(self) -> DbOptions {
        self.options
    }
}

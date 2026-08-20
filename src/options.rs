use std::path::PathBuf;
use crate::sstable::sstable_builder::CompressionType;

/// Global configuration options for the LSM-Tree database.
///
/// It is recommended to construct `DbOptions` using [`DbOptionsBuilder`].
#[derive(Clone, Debug)]
pub struct DbOptions {
    /// The directory where WAL and SSTable files are stored.
    pub dir: PathBuf,
    /// Maximum approximate size of the MemTable before it is flushed to disk (in bytes).
    pub mem_table_max_size: usize,
    /// Maximum total size of a write batch (in bytes) before forcing a WAL sync.
    pub max_batch_bytes: usize,
    /// Maximum number of operations in a write batch before forcing a WAL sync.
    pub max_batch_count: usize,
    /// Target size of an SSTable block (in bytes).
    pub sstable_block_size: usize,
    /// The compression algorithm to use for SSTable blocks.
    pub compression_type: CompressionType,
    /// The compression level for the selected compression algorithm.
    pub compression_level: i32,
    /// Maximum number of pending write requests in the queue before backpressure occurs.
    pub max_write_queue_size: usize,
    /// Maximum number of pending MemTables waiting to be flushed to disk.
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

/// A builder for constructing [`DbOptions`] with custom parameters.
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

    /// Sets the directory path for the database files.
    pub fn dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.options.dir = path.into();
        self
    }

    /// Sets the maximum approximate size in bytes of the MemTable before triggering a flush.
    pub fn mem_table_max_size(mut self, size: usize) -> Self {
        self.options.mem_table_max_size = size;
        self
    }

    /// Sets the maximum number of bytes to accumulate in a write batch before syncing to WAL.
    pub fn max_batch_bytes(mut self, bytes: usize) -> Self {
        self.options.max_batch_bytes = bytes;
        self
    }

    /// Sets the maximum number of operations to accumulate in a write batch before syncing to WAL.
    pub fn max_batch_count(mut self, count: usize) -> Self {
        self.options.max_batch_count = count;
        self
    }

    /// Sets the target size of data blocks within an SSTable in bytes.
    pub fn sstable_block_size(mut self, size: usize) -> Self {
        self.options.sstable_block_size = size;
        self
    }

    /// Sets the compression algorithm used for SSTable data blocks.
    pub fn compression_type(mut self, c_type: CompressionType) -> Self {
        self.options.compression_type = c_type;
        self
    }

    /// Sets the compression level (applicable for algorithms like Zstd).
    pub fn compression_level(mut self, level: i32) -> Self {
        self.options.compression_level = level;
        self
    }

    /// Sets the capacity of the channel used to queue concurrent write requests.
    pub fn max_write_queue_size(mut self, size: usize) -> Self {
        self.options.max_write_queue_size = size;
        self
    }

    /// Sets the capacity of the channel used for background flush tasks.
    pub fn flush_queue_size(mut self, size: usize) -> Self {
        self.options.flush_queue_size = size;
        self
    }

    /// Builds the `DbOptions` based on the configured parameters.
    pub fn build(self) -> DbOptions {
        self.options
    }
}

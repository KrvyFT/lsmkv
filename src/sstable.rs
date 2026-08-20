pub mod sstable_builder {
    use std::{
        collections::BTreeMap,
        fs::{File, OpenOptions},
        io::{BufWriter, Write},
    };

    use crate::error::DbError;
    use crate::{
        error::Result,
        model::{Key, LogRecord, RecordType, Value},
    };

    /// Supported compression algorithms for SSTable blocks.
    #[repr(u8)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub enum CompressionType {
        /// No compression. Fast but uses more disk space.
        None = 0,
        /// Snappy compression. Extremely fast, moderate compression ratio.
        Snappy = 1,
        /// Zstd compression. Good balance of speed and high compression ratio.
        Zstd = 2,
    }

    impl TryFrom<u8> for CompressionType {
        type Error = DbError;
        /// Parses a raw byte into a `CompressionType`.
        fn try_from(val: u8) -> Result<Self> {
            match val {
                0 => Ok(Self::None),
                1 => Ok(Self::Snappy),
                2 => Ok(Self::Zstd),
                _ => Err(DbError::Corruption(format!(
                    "Unknown compression type: {}",
                    val
                ))),
            }
        }
    }
    /// Builder for constructing Sorted String Tables (SSTables).
    /// Used by the background flusher to write MemTables to disk.
    pub struct SSTableBuilder {
        writer: BufWriter<File>,
        index: BTreeMap<Key, u64>,
        current_offset: u64,
        block_size: usize,
        compression_type: CompressionType,
        compression_level: i32,
    }

    impl SSTableBuilder {
        /// Creates a new `SSTableBuilder` targeting the specified file path.
        pub fn new(
            path: &str,
            block_size: usize,
            compression_type: CompressionType,
            compression_level: i32,
        ) -> Self {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .unwrap();
            Self {
                writer: BufWriter::new(file),
                index: BTreeMap::new(),
                current_offset: 0,
                block_size,
                compression_type,
                compression_level,
            }
        }

        /// Builds the SSTable by writing all key-value pairs from the given iterator.
        /// Flushes the index and footer at the end of the file.
        ///
        /// # Errors
        /// Returns `DbError::IO` if any underlying file write or zstd compression operation fails.
        /// Returns `DbError::Serialize` if `bincode` fails to serialize blocks or the index.
        /// Returns `DbError::Corruption` if an unsupported compression type (e.g. Snappy) is used.
        pub fn build(mut self, mem_iter: impl Iterator<Item = (Key, Option<Value>)>) -> Result<()> {
            let mut current_block_records = Vec::new();
            let mut current_block_size = 0;
            let mut current_block_start_key = None;

            for (k, v) in mem_iter {
                // Record the starting key of the block for the sparse index
                if current_block_records.is_empty() {
                    current_block_start_key = Some(k.clone());
                }

                // Construct a LogRecord to reuse serialization logic
                let record = LogRecord {
                    r_type: if v.is_some() {
                        RecordType::Put
                    } else {
                        RecordType::Delete
                    },
                    key: k,
                    value: v,
                };

                let record_size = bincode::serialized_size(&record)? as usize;
                current_block_size += record_size;
                current_block_records.push(record);

                // If the block is full, compress and write it to disk
                if current_block_size >= self.block_size {
                    let encode = bincode::serialize(&current_block_records)?;
                    let payload = match self.compression_type {
                        CompressionType::None => encode,
                        CompressionType::Snappy => return Err(DbError::Corruption("Snappy not implemented".to_string())),
                        CompressionType::Zstd => zstd::encode_all(encode.as_slice(), self.compression_level)
                            .map_err(|e| DbError::IO(e.into()))?,
                    };

                    let len = payload.len() as u64;

                    // Update the sparse index with the block's starting key and its offset
                    self.index
                        .insert(current_block_start_key.take().unwrap(), self.current_offset);
                    self.writer.write_all(&len.to_le_bytes())?;
                    self.writer.write_all(&payload)?;

                    // Move the offset forward (8 bytes for the length prefix)
                    self.current_offset += len + 8;
                    current_block_size = 0;
                    current_block_records.clear();
                }
            }

            // Flush any remaining records that didn't fill a complete block
            if !current_block_records.is_empty() {
                let encode = bincode::serialize(&current_block_records)?;
                let payload = match self.compression_type {
                    CompressionType::None => encode,
                    CompressionType::Snappy => return Err(DbError::Corruption("Snappy not implemented".to_string())),
                    CompressionType::Zstd => zstd::encode_all(encode.as_slice(), self.compression_level)
                        .map_err(|e| crate::error::DbError::IO(e))?,
                };
                let len = payload.len() as u64;
                self.index
                    .insert(current_block_start_key.take().unwrap(), self.current_offset);

                self.writer.write_all(&len.to_le_bytes())?;
                self.writer.write_all(&payload)?;

                self.current_offset += len + 8;
            }
            
            // Serialize and write the sparse index itself to the end of the file
            let index_offset = self.current_offset;
            let encode = bincode::serialize(&self.index)?;
            self.writer.write_all(&encode)?;

            // Write footer: Index Offset (8 bytes)
            self.writer.write_all(&index_offset.to_le_bytes())?;

            // Write footer: Compression Type (1 byte) + Padding (7 bytes)
            self.writer.write_all(&[self.compression_type as u8])?;
            self.writer.write_all(&[0u8; 7])?;

            // Write footer: Magic Number for integrity check (8 bytes)
            self.writer.write_all(&0x888A_u64.to_le_bytes())?;

            self.writer.flush()?;
            Ok(())
        }
    }
}

pub mod sstable {
    use std::{borrow::Cow, collections::BTreeMap, fs::File, sync::Arc};

    use memmap2::Mmap;

    use crate::{
        error::{DbError, Result},
        model::{GetResult, Key, LogRecord, RecordType, Value},
        sstable::sstable_builder::CompressionType,
    };

    /// A Sorted String Table (SSTable) residing on disk.
    /// Uses memory mapping (mmap) for zero-copy, efficient reads.
    pub struct SSTable {
        mmap: Arc<Mmap>,
        index: BTreeMap<Key, u64>,
        compression: CompressionType,
    }

    impl SSTable {
        /// Opens an existing SSTable from disk, verifying its magic number and loading its index.
        ///
        /// # Errors
        /// Returns `DbError::IO` if opening the file or memory mapping fails.
        /// Returns `DbError::Corruption` if the file is too short, the magic number is invalid,
        /// or the sparse index cannot be deserialized correctly.
        pub fn open(path: &str) -> Result<Self> {
            let file = File::open(path)?;
            // Use Mmap for zero-copy memory access to the SSTable file
            let mmap = unsafe { Mmap::map(&file)? };

            let len = mmap.len();
            // A valid SSTable must have at least a 24-byte footer
            if len < 24 {
                return Err(DbError::Corruption("SSTable file is too short.".into()));
            }

            // Verify the magic number at the very end of the file
            let mut magic_bytes = [0u8; 8];
            magic_bytes.copy_from_slice(&mmap[(len - 8)..]);

            if u64::from_le_bytes(magic_bytes) != 0x888A {
                return Err(DbError::Corruption("Bad Magic".into()));
            }

            // Read the sparse index offset
            let mut offset_bytes = [0u8; 8];
            offset_bytes.copy_from_slice(&mmap[(len - 24)..(len - 16)]);
            let index_offset = u64::from_le_bytes(offset_bytes);

            // Read the compression type
            let compression = CompressionType::try_from(mmap[len - 16])?;

            // Deserialize the sparse index into memory
            let index_data = &mmap[index_offset as usize..(len - 24)];

            let index: BTreeMap<Key, u64> = bincode::deserialize(index_data)?;
            Ok(Self {
                mmap: Arc::new(mmap),
                index,
                compression,
            })
        }

        /// Retrieves a value by key directly from the mmap-backed file.
        /// Uses the loaded BTreeMap sparse index to find the block, decompresses it,
        /// and binary searches within the block.
        pub fn get(&self, key: &Key) -> GetResult<Value> {
            // Sparse Index lookup: Find the block whose starting key is the largest one <= the target key
            let block_offset = match self.index.range(..=key.clone()).next_back() {
                Some((_, &offset)) => offset,
                None => return GetResult::NotFound,
            };

            // Read the length of the block payload
            let mut current = block_offset as usize;
            let mut len_bytes = [0u8; 8];
            len_bytes.copy_from_slice(&self.mmap[current..current + 8]);
            let block_len = u64::from_le_bytes(len_bytes) as usize;
            current += 8;

            // Extract and conditionally decompress the payload
            let raw_payload = &self.mmap[current..current + block_len];
            let payload = match self.compression {
                CompressionType::None => Cow::Borrowed(raw_payload),
                CompressionType::Zstd => match zstd::decode_all(raw_payload) {
                    Ok(decoded) => Cow::Owned(decoded),
                    Err(e) => return GetResult::Error(format!("Zstd decompression failed: {}", e)),
                },
                _ => unimplemented!("尚未支持其他解压算法"),
            };

            // Deserialize the entire block of records
            let records: Vec<LogRecord> = match bincode::deserialize(&payload) {
                Ok(r) => r,
                Err(e) => return GetResult::Error(e.to_string()),
            };

            // Perform binary search within the block for the exact key
            match records.binary_search_by(|r| r.key.cmp(key)) {
                Ok(idx) => {
                    let record = &records[idx];
                    match record.r_type {
                        RecordType::Put => GetResult::Found(record.value.clone().unwrap()),
                        RecordType::Delete => GetResult::Deleted,
                    }
                }
                Err(_) => GetResult::NotFound,
            }
        }
    }
}

pub mod sstable_builder {
    use std::{
        collections::BTreeMap,
        fs::{File, OpenOptions},
        io::{BufWriter, Write},
    };

    use crate::{
        error::Result,
        model::{Key, LogRecord, RecordType, Value},
    };

    pub struct SSTableBuilder {
        writer: BufWriter<File>,
        index: BTreeMap<Key, u64>,
        current_offset: u64,
    }

    impl SSTableBuilder {
        pub fn new(path: &str) -> Self {
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
            }
        }

        pub fn build(mut self, mem_iter: impl Iterator<Item = (Key, Option<Value>)>) -> Result<()> {
            for (k, v) in mem_iter {
                let record = LogRecord {
                    r_type: if v.is_some() {
                        RecordType::Put
                    } else {
                        RecordType::Delete
                    },
                    key: k.clone(),
                    value: v.clone(),
                };

                let encode = bincode::serialize(&record)?;

                self.index.insert(k, self.current_offset);
                let len = encode.len() as u32;

                self.writer.write(&len.to_le_bytes())?;
                self.writer.write_all(&encode)?;

                self.current_offset += (len as u64) + 4;
            }
            let index_offset = self.current_offset;
            let encode = bincode::serialize(&self.index)?;
            self.writer.write_all(&encode)?;

            self.writer.write_all(&index_offset.to_le_bytes())?;
            self.writer.write_all(&0x8888_u64.to_le_bytes())?;

            self.writer.flush()?;
            Ok(())
        }
    }
}

pub mod sstable {
    use std::{collections::BTreeMap, fs::File, sync::Arc};

    use memmap2::Mmap;

    use crate::{
        error::{DbError, Result},
        model::{GetResult, Key, LogRecord, RecordType, Value},
    };

    pub struct SSTable {
        mmap: Arc<Mmap>,
        index: BTreeMap<Key, u64>,
    }

    impl SSTable {
        pub fn open(path: &str) -> Result<Self> {
            let file = File::open(path)?;
            let mmap = unsafe { Mmap::map(&file)? };

            let len = mmap.len();
            let footer = &mmap[(len - 16)..];

            let mut magic_bytes = [0u8; 8];
            magic_bytes.copy_from_slice(&footer[8..]);

            if u64::from_le_bytes(magic_bytes) != 0x8888 {
                return Err(DbError::Corruption("Bad Magic".into()));
            }

            let mut offset_bytes = [0u8; 8];
            offset_bytes.copy_from_slice(&footer[0..8]);
            let index_offset = u64::from_le_bytes(offset_bytes);

            let index_data = &mmap[index_offset as usize..(len - 16)];
            let index: BTreeMap<Key, u64> = bincode::deserialize(index_data)?;
            Ok(Self {
                mmap: Arc::new(mmap),
                index,
            })
        }

        pub fn get(&self, key: &Key) -> GetResult<Value> {
            if let Some(&offset) = self.index.get(key) {
                let mut current = offset as usize;

                let mut len_bytes = [0u8; 4];
                len_bytes.copy_from_slice(&self.mmap[current..current + 4]);
                let len = u32::from_le_bytes(len_bytes) as usize;
                current += 4;

                let payload = &self.mmap[current..current + len];
                let record: LogRecord = bincode::deserialize(payload).unwrap();

                match record.r_type {
                    RecordType::Put => GetResult::Found(record.value.unwrap()),
                    RecordType::Delete => GetResult::Deleted,
                }
            } else {
                GetResult::NotFound
            }
        }
    }
}

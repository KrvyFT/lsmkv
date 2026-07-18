use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

use crate::{error::DbError, model::LogRecord};

/// Write-Ahead Log (WAL) writer.
/// Ensures durability by appending all operations to a sequential log file before applying them to the MemTable.
pub struct WalWriter {
    writer: BufWriter<File>,
    dir: PathBuf,
}

impl WalWriter {
    /// Creates a new `WalWriter` opening a `.log` file in the specified directory.
    pub fn new(dir: impl AsRef<Path>, initial_id: u64) -> Result<Self, DbError> {
        let dir_path = dir.as_ref().to_path_buf();

        if !dir_path.exists() {
            std::fs::create_dir_all(&dir_path).map_err(|e| DbError::Corruption(e.to_string()))?;
        }

        let file_path = Self::build_path(&dir_path, initial_id);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .map_err(|e| DbError::Corruption(e.to_string()))?;

        Ok(Self {
            writer: BufWriter::new(file),
            dir: dir_path,
        })
    }

    /// Rotates the WAL to a new file with the specified `next_id`.
    /// Flushes and syncs the old file before closing it.
    pub fn rotate(&mut self, next_id: u64) -> Result<(), DbError> {
        self.writer
            .flush()
            .map_err(|e| DbError::Corruption(e.to_string()))?;

        self.writer
            .get_mut()
            .sync_all()
            .map_err(|e| DbError::Corruption(e.to_string()))?;

        let new_path = Self::build_path(&self.dir, next_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(new_path)
            .map_err(|e| DbError::Corruption(e.to_string()))?;

        self.writer = BufWriter::new(file);

        Ok(())
    }

    /// Reads and deserializes all records from a WAL file.
    /// Used for crash recovery during database startup.
    pub fn read_all_records(path: &Path) -> Result<Vec<LogRecord>, DbError> {
        let mut file = File::open(path).map_err(|e| DbError::Corruption(e.to_string()))?;
        let mut records = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            if let Err(e) = file.read_exact(&mut len_buf) {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                eprintln!("Warning: WAL file may be truncated: {}", e);
                break;
            }

            let len = u32::from_le_bytes(len_buf);
            let mut encode = vec![0u8; len as usize];

            if let Err(e) = file.read_exact(&mut encode) {
                eprintln!("Warning: WAL file may be truncated: {}", e);
                break;
            }

            let record: LogRecord =
                bincode::deserialize(&encode).map_err(|e| DbError::Corruption(e.to_string()))?;
            records.push(record);
        }
        Ok(records)
    }

    /// Appends a new `LogRecord` to the WAL in memory buffer.
    pub fn append(&mut self, record: &LogRecord) -> Result<(), DbError> {
        let encode: Vec<u8> = bincode::serialize(record)?;
        let len = encode.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&encode)?;

        Ok(())
    }

    /// Flushes the internal buffer and forces an fsync to ensure durability.
    pub fn sync(&mut self) -> Result<(), DbError> {
        self.writer.get_mut().sync_data()?;

        Ok(())
    }

    fn build_path(dir_path: &PathBuf, id: u64) -> PathBuf {
        dir_path.join(format!("log_{:06}.log", id))
    }
}

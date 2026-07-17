use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
};

use crate::{error::DbError, model::LogRecord};

pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    pub fn new(path: &str) -> Result<Self, DbError> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, record: &LogRecord) -> Result<(), DbError> {
        let encode: Vec<u8> = bincode::serialize(record)?;
        let len = encode.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&encode)?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), DbError> {
        self.writer.get_mut().sync_data()?;

        Ok(())
    }
}

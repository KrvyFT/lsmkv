//! Global error definitions for the LSM-Tree database.
//!
//! We follow a Fail-Fast philosophy: standard CRUD operations return `DbError::NotFound`
//! for missing data, but critical storage anomalies (like I/O failures on WAL or corrupted magic numbers)
//! yield `DbError::Corruption`, triggering panics or graceful shutdowns to protect data integrity.

use thiserror::Error;

/// Global error type for the LSM-Tree database.
#[derive(Error, Debug)]
pub enum DbError {
    /// Underlying standard library IO errors.
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),
    /// Errors occurring during binary serialization or deserialization.
    #[error("Serialize Error: {0}")]
    Serialize(#[from] bincode::Error),
    /// Data corruption detected (e.g. invalid magic number, truncated log).
    #[error("Data corruption: {0}")]
    Corruption(String),
    /// The requested key was not found in the database.
    #[error("Key not found")]
    NotFound,
}

/// A specialized Result type for the database.
pub type Result<T> = std::result::Result<T, DbError>;

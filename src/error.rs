use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),
    #[error("Serialize Error: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("Data corruption: {0}")]
    Corruption(String),
}

pub type Result<T> = std::result::Result<T, DbError>;

//! Table definitions.

use std::{
    error::Error,
    fmt::{Debug, Display},
};

/// Column definition.
pub struct ColumnDef<T: AsRef<str>> {
    pub name: T,
    pub column_type: u8,
    pub size: u16,
}

/// Error type when creating table.
#[derive(Debug)]
pub enum CreateTableError {
    HeaderTableFull,
    TableExists,
    TableNameTooLong,
    StorageFull,
    ColumnNameTooLong,
}

impl Display for CreateTableError {
    /// This is just Debug::fmt now.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

/// Empty impl.
impl Error for CreateTableError {}

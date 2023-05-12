//! Table definitions.

use std::{
    cmp::Ordering,
    error::Error,
    fmt::{Debug, Display},
    ops::Range,
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
    TableNameInvalid,
    StorageFull,
    ColumnNameTooLong,
    TooManyColumns,
    ColumnTooBig,
}

pub struct Condition<T: AsRef<[u8]>> {
    pub range: Range<usize>,
    pub data: T,
    /// Ord between accepted data and condition data.
    pub ord: Ordering,
}

impl Display for CreateTableError {
    /// This is just Debug::fmt now.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

/// Empty impl.
impl Error for CreateTableError {}

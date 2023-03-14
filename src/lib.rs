#![doc = include_str!("../README.md")]

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
};

use table::{ColumnDef, CreateTableError};

pub mod table;

const PAGE_SIZE: u64 = 4 * 1024;
const TABLE_NAME_MAX_LEN: u8 = 23;
const COLUMN_NAME_MAX_LEN: u8 = 28;
const HEADER_TABLE_ROW_LEN: u8 = 32;
const HEADER_TABLE_RECORD_COUNT: u8 = (PAGE_SIZE / HEADER_TABLE_ROW_LEN as u64) as u8;
const META_TABLE_ROW_LEN: u8 = 32;
const META_TABLE_RECORD_COUNT: u8 = (PAGE_SIZE / META_TABLE_ROW_LEN as u64) as u8;

/// The struct used to operate with the underlying file system.
pub struct Database {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    header_table: HashMap<String, HeaderMeta>,
    // absolute offset
    in_use_pages: HashSet<i32>,
}

struct HeaderMeta {
    col_def_offset: i32,
    meta_offset: i32,
    table_offsets: Vec<i32>,
    record_offset: u64,
}

// fn read_str(reader: &mut impl BufRead, max_len: usize) -> io::Result<String> {
//     let mut len = [0];
//     reader.read_exact(&mut len)?;
//     let len = len[0] as usize;
//     if len > max_len {
//         return Err(io::Error::new(ErrorKind::InvalidData, "str overflow"));
//     }
//     let mut s = vec![0; len];
//     reader.read_exact(&mut s)?;
//     Ok(String::from_utf8_lossy(&s).to_string())
// }

impl Database {
    /// Open a database file.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Database> {
        let mut reader = BufReader::new(File::open(&path)?);
        let mut header_table = HashMap::new();
        let mut name = [0; TABLE_NAME_MAX_LEN as usize];
        let mut int32 = [0; 4];
        let mut in_use_pages = HashSet::from([0]);
        for _ in 0..HEADER_TABLE_RECORD_COUNT {
            let mut len = [0];
            let position = reader.stream_position()?;
            reader.read_exact(&mut len)?;
            let len = len[0];
            if len == 0 {
                reader.seek_relative(HEADER_TABLE_ROW_LEN as i64 - 1)?;
            } else {
                if len > TABLE_NAME_MAX_LEN {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "table name overflow",
                    ));
                }
                let len = len as usize;
                reader.read_exact(&mut name[0..len])?;
                // implicit transform
                let name = String::from_utf8_lossy(&mut name[0..len]);
                reader.read_exact(&mut int32)?;
                let col_def_offset = i32::from_be_bytes(int32);
                reader.read_exact(&mut int32)?;
                let meta_offset = i32::from_be_bytes(int32);
                header_table.insert(
                    name.into_owned(),
                    HeaderMeta {
                        col_def_offset,
                        meta_offset,
                        record_offset: position,
                        table_offsets: Vec::new(),
                    },
                );
                in_use_pages.insert(col_def_offset);
            }
        }
        // now reader should be at 4096
        for HeaderMeta {
            meta_offset,
            table_offsets,
            ..
        } in header_table.values_mut()
        {
            let meta_offset = *meta_offset;
            set_reader_to_head(&mut reader)?;
            reader.seek_relative(PAGE_SIZE as i64 * meta_offset as i64)?;
            for _ in 0..META_TABLE_RECORD_COUNT {
                let mut int32 = [0; 4];
                reader.read_exact(&mut int32)?;
                if int32 == [0; 4] {
                    continue;
                }
                let table_offset = i32::from_be_bytes(int32) + meta_offset;
                in_use_pages.insert(table_offset);
                table_offsets.push(table_offset);
            }
            in_use_pages.insert(meta_offset);
        }

        let writer = BufWriter::new(File::options().write(true).open(path)?);
        Ok(Database {
            reader,
            writer,
            header_table,
            in_use_pages,
        })
    }

    /// This function will create a file if it does not exist,
    /// and will truncate it if it does.
    ///
    /// Depending on the platform,
    /// this function may fail
    /// if the full directory path does not exist.
    pub fn create_database(path: impl AsRef<Path>) -> io::Result<()> {
        let file = File::create(path)?;
        file.set_len(PAGE_SIZE as u64)?;
        file.sync_all()
    }

    /// This function will create a table in the database.
    ///
    /// # Errors
    ///
    /// It's an error to create a table
    /// whose name already exists in the database.
    /// However, this function does not check
    /// the table column definition parameter,
    /// as it just appends it to
    /// the definition table,
    /// thus does not consider it an error.
    pub fn create_table(
        &mut self,
        table_name: &str,
        table_def: &[ColumnDef<impl AsRef<str>>],
    ) -> io::Result<()> {
        let name_len = table_name.len();
        if name_len > 23 || name_len == 0 {
            return Err(io::Error::new(
                ErrorKind::Other,
                CreateTableError::TableNameInvalid,
            ));
        }
        let name_len = name_len as u8;
        if self.header_table.contains_key(table_name) {
            return Err(io::Error::new(
                ErrorKind::Other,
                CreateTableError::TableExists,
            ));
        }
        let reader = &mut self.reader;
        let mut cursor = 0;
        set_reader_to_head(reader)?;
        while cursor < PAGE_SIZE {
            let position = reader.stream_position()?;
            let mut len = [0];
            reader.read_exact(&mut len)?;
            let len = len[0];
            if len == 0 {
                let writer = &mut self.writer;
                writer.seek(SeekFrom::Start(cursor))?;
                writer.write_all(&[name_len])?;
                writer.write_all(table_name.as_bytes())?;
                // TODO: full scan
                let mut offset = 0;
                while offset < i32::MAX {
                    if !self.in_use_pages.contains(&offset) {
                        break;
                    }
                    offset += 1;
                }
                if offset == i32::MAX {
                    return Err(io::Error::new(
                        ErrorKind::Other,
                        CreateTableError::StorageFull,
                    ));
                }
                // def table offset
                writer.write_all(&offset.to_be_bytes())?;
                // meta table offset
                writer.write_all(&[0; 4])?;
                writer.seek(SeekFrom::Start(offset as u64))?;
                for def in table_def {
                    let name = def.name.as_ref();
                    let len = name.len();
                    if len > COLUMN_NAME_MAX_LEN as usize {
                        return Err(io::Error::new(
                            ErrorKind::Other,
                            CreateTableError::ColumnNameTooLong,
                        ));
                    }
                    writer.write_all(&[len as u8])?;
                    writer.write_all(name.as_bytes())?;
                    writer.write_all(&[def.column_type])?;
                    writer.write_all(&def.size.to_be_bytes())?;
                }
                writer.flush()?;
                // add to header metadata
                self.header_table.insert(
                    table_name.to_string(),
                    HeaderMeta {
                        col_def_offset: offset,
                        meta_offset: 0,
                        record_offset: position,
                        table_offsets: Vec::new(),
                    },
                );
                return Ok(());
            }
            cursor += 32;
        }

        // header table full
        Err(io::Error::new(
            ErrorKind::Other,
            CreateTableError::HeaderTableFull,
        ))
    }

    pub fn delete_table(&mut self, table_name: &str) -> io::Result<()> {
        if let Some(meta) = self.header_table.get(table_name) {
            let writer = &mut self.writer;
            writer.seek(SeekFrom::Start(meta.record_offset))?;
            writer.write_all(&[0; HEADER_TABLE_ROW_LEN as usize])?;
            writer.flush()?;
            self.header_table.remove(table_name);
            Ok(())
        } else {
            Err(io::Error::new(ErrorKind::Other, "table not found"))
        }
    }
}

fn set_reader_to_head<T: Read + Seek>(reader: &mut BufReader<T>) -> io::Result<()> {
    let pos = reader.stream_position()? as i64;
    if pos < 0 {
        reader.rewind()?;
    } else {
        reader.seek_relative(-pos)?;
    }
    Ok(())
}

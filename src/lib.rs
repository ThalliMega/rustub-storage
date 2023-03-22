#![doc = include_str!("../README.md")]

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::Path,
};

use table::{ColumnDef, Condition, CreateTableError};

pub mod table;

const PAGE_SIZE: u32 = 4 * 1024;
const TABLE_NAME_MAX_LEN: u8 = 23;
const COLUMN_NAME_MAX_LEN: u8 = 28;
const HEADER_TABLE_ROW_LEN: u8 = 32;
const HEADER_TABLE_RECORD_COUNT: u8 = (PAGE_SIZE / HEADER_TABLE_ROW_LEN as u32) as u8;
const META_TABLE_ROW_LEN: u8 = 32;
const META_TABLE_RECORD_COUNT: u8 = (PAGE_SIZE / META_TABLE_ROW_LEN as u32) as u8;
const DEF_TABLE_ROW_LEN: u8 = 32;
const DEF_TABLE_RECORD_COUNT: u8 = (PAGE_SIZE / DEF_TABLE_ROW_LEN as u32) as u8;

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
    // relative to meta table
    table_offsets: Vec<i32>,
    header_record_offset: u8,
    row_len: u16,
}

impl Database {
    /// Open a database file.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Database> {
        let mut reader = BufReader::new(File::open(&path)?);
        let mut header_table = HashMap::new();
        let mut name = [0; TABLE_NAME_MAX_LEN as usize];
        let mut int32 = [0; 4];
        let mut in_use_pages = HashSet::from([0]);
        for header_record_offset in 0..HEADER_TABLE_RECORD_COUNT {
            let mut table_name_len = [0];
            reader.read_exact(&mut table_name_len)?;
            let table_name_len = table_name_len[0];
            if table_name_len == 0 {
                reader.seek_relative(HEADER_TABLE_ROW_LEN as i64 - 1)?;
            } else {
                if table_name_len > TABLE_NAME_MAX_LEN {
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "table name overflow",
                    ));
                }
                let table_name_len = table_name_len as usize;
                reader.read_exact(&mut name[..table_name_len])?;
                // implicit transform
                let name = String::from_utf8_lossy(&name[0..table_name_len]);
                reader.read_exact(&mut int32)?;
                let col_def_offset = i32::from_be_bytes(int32);
                reader.read_exact(&mut int32)?;
                let meta_offset = i32::from_be_bytes(int32);
                header_table.insert(
                    name.into_owned(),
                    HeaderMeta {
                        col_def_offset,
                        meta_offset,
                        header_record_offset,
                        table_offsets: Vec::new(),
                        row_len: 0,
                    },
                );
                in_use_pages.insert(col_def_offset);
                in_use_pages.insert(meta_offset);
            }
        }
        // now reader should be at 4096
        for HeaderMeta {
            meta_offset,
            table_offsets,
            col_def_offset,
            row_len,
            ..
        } in header_table.values_mut()
        {
            let meta_offset = *meta_offset;
            reader.seek(SeekFrom::Start(PAGE_SIZE as u64 * meta_offset as u64))?;
            for _ in 0..META_TABLE_RECORD_COUNT {
                let mut int32 = [0; 4];
                reader.read_exact(&mut int32)?;
                if int32 == [0; 4] {
                    continue;
                }
                let table_offset = i32::from_be_bytes(int32);
                let table_absolute_offset = table_offset + meta_offset;
                in_use_pages.insert(table_absolute_offset);
                table_offsets.push(table_offset);
            }

            let col_def_offset = *col_def_offset;
            reader.seek(SeekFrom::Start(PAGE_SIZE as u64 * col_def_offset as u64))?;
            for _ in 0..DEF_TABLE_RECORD_COUNT {
                let mut size = [0; 2];
                reader.seek_relative(DEF_TABLE_ROW_LEN as i64 - 2)?;
                reader.read_exact(&mut size)?;
                let size = u16::from_be_bytes(size);
                if size == 0 {
                    break;
                }
                *row_len += size;
            }
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
        if table_def.len() > DEF_TABLE_RECORD_COUNT as usize {
            return Err(io::Error::new(
                ErrorKind::Other,
                CreateTableError::TooManyColumns,
            ));
        }
        if self.header_table.contains_key(table_name) {
            return Err(io::Error::new(
                ErrorKind::Other,
                CreateTableError::TableExists,
            ));
        }
        for def in table_def {
            if def.name.as_ref().len() > COLUMN_NAME_MAX_LEN as usize {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    CreateTableError::ColumnNameTooLong,
                ));
            }
        }
        let row_size: u16 = table_def.iter().map(|d| d.size).sum();
        if row_size as u32 > PAGE_SIZE {
            return Err(io::Error::new(
                ErrorKind::Other,
                CreateTableError::ColumnTooBig,
            ));
        }

        let name_len = name_len as u8;
        let reader = &mut self.reader;

        set_reader_to_head(reader)?;
        for header_record_offset in 0..HEADER_TABLE_RECORD_COUNT {
            let mut len = [0];
            reader.read_exact(&mut len)?;
            let len = len[0];
            if len == 0 {
                let writer = &mut self.writer;
                writer.seek(SeekFrom::Start(
                    header_record_offset as u64 * HEADER_TABLE_ROW_LEN as u64,
                ))?;
                writer.write_all(&[name_len])?;
                writer.write_all(table_name.as_bytes())?;
                // TODO: full scan
                let def_offset_page = find_spare_page(&self.in_use_pages)?;
                // def table offset
                writer.write_all(&def_offset_page.to_be_bytes())?;
                let mut meta_offset_page = def_offset_page;
                while meta_offset_page < i32::MAX {
                    if !self.in_use_pages.contains(&meta_offset_page) {
                        break;
                    }
                    meta_offset_page += 1;
                }
                if meta_offset_page == i32::MAX {
                    return Err(io::Error::new(
                        ErrorKind::Other,
                        CreateTableError::StorageFull,
                    ));
                }
                // meta table offset
                writer.write_all(&meta_offset_page.to_be_bytes())?;
                writer.seek(SeekFrom::Start(def_offset_page as u64 * PAGE_SIZE as u64))?;
                self.in_use_pages
                    .extend([meta_offset_page, def_offset_page]);

                let rest_len = PAGE_SIZE as usize - table_def.len() * DEF_TABLE_ROW_LEN as usize;
                for def in table_def {
                    let name = def.name.as_ref();
                    let len = name.len();

                    writer.write_all(&[len as u8])?;
                    writer.write_all(name.as_bytes())?;
                    writer.write_all(&[def.column_type])?;
                    writer.write_all(&def.size.to_be_bytes())?;
                }
                writer.write_all(&vec![0; rest_len])?;

                writer.seek(SeekFrom::Start(meta_offset_page as u64 * PAGE_SIZE as u64))?;
                writer.write_all(&[0; PAGE_SIZE as usize])?;

                writer.flush()?;
                // add to header metadata
                self.header_table.insert(
                    table_name.to_string(),
                    HeaderMeta {
                        col_def_offset: def_offset_page,
                        meta_offset: meta_offset_page,
                        header_record_offset,
                        table_offsets: Vec::new(),
                        row_len: row_size,
                    },
                );
                return Ok(());
            }
        }

        // header table full
        Err(io::Error::new(
            ErrorKind::Other,
            CreateTableError::HeaderTableFull,
        ))
    }

    pub fn drop(&mut self, table_name: &str) -> io::Result<()> {
        if let Some(meta) = self.header_table.remove(table_name) {
            let writer = &mut self.writer;
            writer.seek(SeekFrom::Start(
                (meta.header_record_offset * HEADER_TABLE_ROW_LEN) as u64,
            ))?;
            writer.write_all(&[0; HEADER_TABLE_ROW_LEN as usize])?;
            writer.flush()?;

            self.in_use_pages.remove(&meta.col_def_offset);
            self.in_use_pages.remove(&meta.meta_offset);
            for table_offset in meta.table_offsets {
                self.in_use_pages.remove(&(table_offset + meta.meta_offset));
            }
            Ok(())
        } else {
            Err(io::Error::new(ErrorKind::Other, "table not found"))
        }
    }

    pub fn get_table_def(&mut self, table_name: &str) -> io::Result<Vec<ColumnDef<String>>> {
        if let Some(meta) = self.header_table.get(table_name) {
            let def_offset = meta.col_def_offset;
            let reader = &mut self.reader;

            reader.seek(SeekFrom::Start(def_offset as u64 * PAGE_SIZE as u64))?;
            let mut defs = Vec::new();
            for _ in 0..DEF_TABLE_RECORD_COUNT {
                let mut buf = [0; u8::MAX as usize];
                reader.read_exact(&mut buf[..1])?;
                let len = buf[0] as usize;
                if len == 0 {
                    // reader.seek_relative(DEF_TABLE_ROW_LEN as i64 - 1)?;
                    // continue;
                    break;
                }
                reader.read_exact(&mut buf[..len])?;
                let name = String::from_utf8_lossy(&buf[..len]).to_string();
                reader.read_exact(&mut buf[..1])?;
                let column_type = buf[0];
                reader.read_exact(&mut buf[..2])?;
                let size = u16::from_be_bytes(buf[..2].try_into().unwrap());
                defs.push(ColumnDef {
                    name,
                    column_type,
                    size,
                })
            }
            Ok(defs)
        } else {
            Err(io::Error::new(ErrorKind::Other, "table not found"))
        }
    }

    /// Data can contain only one row.
    pub fn insert(&mut self, table_name: &str, data: &[u8]) -> io::Result<()> {
        let meta = check_table_exists(&self.header_table, table_name)?;
        if data.len() != meta.row_len as usize {
            return Err(io::Error::new(
                ErrorKind::Other,
                "data len mismatch: only one row at a time is accepted",
            ));
        }

        let reader = &mut self.reader;

        for record_offset in 0..META_TABLE_RECORD_COUNT {
            let mut buf = [0; 4];
            reader.seek(SeekFrom::Start(
                meta.meta_offset as u64 * PAGE_SIZE as u64
                    + record_offset as u64 * META_TABLE_ROW_LEN as u64,
            ))?;
            reader.read_exact(&mut buf)?;
            let table_offset = i32::from_be_bytes(buf);
            if table_offset == 0 {
                let new_table = find_spare_page(&self.in_use_pages)?;
                let writer = &mut self.writer;
                writer.seek(SeekFrom::Start(reader.stream_position()? - 4))?;
                writer.write_all(&new_table.to_be_bytes())?;
                writer.seek(SeekFrom::Start(new_table as u64 * PAGE_SIZE as u64))?;
                self.in_use_pages.insert(new_table);
                writer.write_all(data)?;
                writer.write_all(&vec![0; PAGE_SIZE as usize - data.len()])?;
                writer.flush()?;
                return Ok(());
            }

            let mut table_record_offset_byte = 0;
            let row_len = data.len();
            reader.seek(SeekFrom::Start(table_offset as u64))?;
            while table_record_offset_byte < PAGE_SIZE {
                let mut buf = vec![0; data.len()];
                reader.read_exact(&mut buf)?;
                if buf.into_iter().all(|b| b == 0) {
                    let writer = &mut self.writer;
                    writer.seek(SeekFrom::Start(
                        reader.stream_position()? - data.len() as u64,
                    ))?;
                    writer.write_all(data)?;
                    writer.flush()?;
                    return Ok(());
                }
                table_record_offset_byte += row_len as u32;
            }
        }

        Err(io::Error::new(
            ErrorKind::Other,
            CreateTableError::StorageFull,
        ))
    }

    pub fn select<T: AsRef<[u8]>>(
        &mut self,
        table_name: &str,
        conditions: &[Condition<T>],
    ) -> io::Result<Vec<Vec<u8>>> {
        let meta = check_table_exists(&self.header_table, table_name)?;
        let reader = &mut self.reader;
        let mut res = Vec::new();

        for record_number in 0..META_TABLE_RECORD_COUNT {
            reader.seek(SeekFrom::Start(
                meta.meta_offset as u64 * PAGE_SIZE as u64
                    + record_number as u64 * META_TABLE_ROW_LEN as u64,
            ))?;
            let mut table_offset = [0; 4];
            reader.read_exact(&mut table_offset)?;
            let table_offset = i32::from_be_bytes(table_offset);
            if table_offset == 0 {
                continue;
            }

            reader.seek(SeekFrom::Start(table_offset as u64 * PAGE_SIZE as u64))?;
            let mut cursor = 0;
            let mut buf = vec![0; meta.row_len as usize];
            while cursor < PAGE_SIZE {
                reader.read_exact(&mut buf)?;
                if conditions
                    .iter()
                    .all(|c| &buf[c.range.clone()] == c.eq_to.as_ref())
                {
                    res.push(buf.clone());
                }
                cursor += meta.row_len as u32;
            }
        }

        Ok(res)
    }

    pub fn update<C: AsRef<[u8]>, N: AsRef<[u8]>>(
        &mut self,
        table_name: &str,
        conditions: &[Condition<C>],
        new_value: &[Condition<N>],
    ) -> io::Result<usize> {
        let meta = check_table_exists(&self.header_table, table_name)?;
        let reader = &mut self.reader;
        let mut res = 0;

        for record_number in 0..META_TABLE_RECORD_COUNT {
            reader.seek(SeekFrom::Start(
                meta.meta_offset as u64 * PAGE_SIZE as u64
                    + record_number as u64 * META_TABLE_ROW_LEN as u64,
            ))?;
            let mut table_offset = [0; 4];
            reader.read_exact(&mut table_offset)?;
            let table_offset = i32::from_be_bytes(table_offset);
            if table_offset == 0 {
                continue;
            }

            reader.seek(SeekFrom::Start(table_offset as u64 * PAGE_SIZE as u64))?;
            let mut cursor = 0;
            let mut buf = vec![0; meta.row_len as usize];
            while cursor < PAGE_SIZE {
                reader.read_exact(&mut buf)?;
                if conditions
                    .iter()
                    .all(|c| &buf[c.range.clone()] == c.eq_to.as_ref())
                {
                    let writer = &mut self.writer;
                    let start = reader.stream_position()? - meta.row_len as u64;

                    for field in new_value {
                        writer.seek(SeekFrom::Start(start + field.range.start as u64))?;
                        writer.write_all(field.eq_to.as_ref())?;
                    }
                    res += 1;
                }
                cursor += meta.row_len as u32;
            }
        }
        self.writer.flush()?;

        Ok(res)
    }

    pub fn delete<T: AsRef<[u8]>>(
        &mut self,
        table_name: &str,
        conditions: &[Condition<T>],
    ) -> io::Result<usize> {
        let meta = check_table_exists(&self.header_table, table_name)?;
        let reader = &mut self.reader;
        let mut res = 0;

        for record_number in 0..META_TABLE_RECORD_COUNT {
            reader.seek(SeekFrom::Start(
                meta.meta_offset as u64 * PAGE_SIZE as u64
                    + record_number as u64 * META_TABLE_ROW_LEN as u64,
            ))?;
            let mut table_offset = [0; 4];
            reader.read_exact(&mut table_offset)?;
            let table_offset = i32::from_be_bytes(table_offset);
            if table_offset == 0 {
                continue;
            }

            reader.seek(SeekFrom::Start(table_offset as u64 * PAGE_SIZE as u64))?;
            let mut cursor = 0;
            let mut buf = vec![0; meta.row_len as usize];
            let mut empty_page = true;
            while cursor < PAGE_SIZE {
                reader.read_exact(&mut buf)?;
                if conditions
                    .iter()
                    .all(|c| &buf[c.range.clone()] == c.eq_to.as_ref())
                {
                    res += 1;
                } else if buf.iter().any(|b| *b != 0) {
                    empty_page = false;
                }
                cursor += meta.row_len as u32;
            }
            if empty_page {
                let writer = &mut self.writer;
                writer.seek(SeekFrom::Start(
                    meta.meta_offset as u64 * PAGE_SIZE as u64
                        + record_number as u64 * META_TABLE_ROW_LEN as u64,
                ))?;
                writer.write_all(&[0; META_TABLE_ROW_LEN as usize])?;
                self.in_use_pages.remove(&table_offset);
            }
        }
        self.writer.flush()?;

        Ok(res)
    }
}

fn check_table_exists<'h>(
    header_table: &'h HashMap<String, HeaderMeta>,
    table_name: &str,
) -> io::Result<&'h HeaderMeta> {
    if let Some(meta) = header_table.get(table_name) {
        Ok(meta)
    } else {
        Err(io::Error::new(ErrorKind::Other, "table not found"))
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

fn find_spare_page(in_use_pages: &HashSet<i32>) -> io::Result<i32> {
    let mut offset_page = 0;
    while offset_page < i32::MAX {
        if !in_use_pages.contains(&offset_page) {
            break;
        }
        offset_page += 1;
    }
    if offset_page == i32::MAX {
        Err(io::Error::new(
            ErrorKind::Other,
            CreateTableError::StorageFull,
        ))
    } else {
        Ok(offset_page)
    }
}

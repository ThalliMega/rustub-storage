# rustub-storage

An experiment project of educational database storage system.

Due to lack of wasi support from tokio/async-std, this package is currently un-async.

Use big-endian.

## database file structure

1. header meta table
2. column def table
3. meta table
4. table

Currently full zeroed rows are considered uninitialized.

Offset from current page, based on page.

### header meta table

| column name | type def | size |
| -- | -- | -- |
| name | varchar(23) | 24 |
| def_table_offset | i32 | 4 |
| meta_table_offset | i32 | 4 |

### column def table

| column name | type def | size |
| -- | -- | -- |
| name | varchar(28) | 29 |
| type | u8 | 1 |
| size | u16 | 2 |

### meta table

| column name | type def | size |
| -- | -- | -- |
| table_offset | i32 | 4 |

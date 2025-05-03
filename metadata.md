# ParquetMetaDataReader

parquet::file::metadata::ParquetMetaDataReader

Reads the ParquetMetaData from a byte stream.

See crate::file::metadata::ParquetMetaDataWriter for a description of the Parquet metadata.

Parquet metadata is not necessarily contiguous in the files: part is stored in the footer (the last bytes of the file), but other portions (such as the PageIndex) can be stored elsewhere.

This reader handles reading the footer as well as the non contiguous parts of the metadata such as the page indexes; excluding Bloom Filters.

## Example

```rust
// read parquet metadata including page indexes from a file
let file = open_parquet_file("some_path.parquet");
let mut reader = ParquetMetaDataReader::new()
.with_page_indexes(true);
reader.try_parse(&file).unwrap();
let metadata = reader.finish().unwrap();
assert!(metadata.column_index().is_some());
assert!(metadata.offset_index().is_some());
```

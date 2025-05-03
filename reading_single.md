## Example of reading an existing file

```rust
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::Path};

let path = Path::new("/path/to/sample.parquet");
if let Ok(file) = File::open(&path) {
let reader = SerializedFileReader::new(file).unwrap();

    let parquet_metadata = reader.metadata();
    assert_eq!(parquet_metadata.num_row_groups(), 1);

    let row_group_reader = reader.get_row_group(0).unwrap();
    assert_eq!(row_group_reader.num_columns(), 1);

}
```

## Example of reading multiple files

```rust
use parquet::file::reader::SerializedFileReader;
use std::convert::TryFrom;

let paths = vec![
    "/path/to/sample.parquet/part-1.snappy.parquet",
    "/path/to/sample.parquet/part-2.snappy.parquet"
];
// Create a reader for each file and flat map rows
let rows = paths.iter()
    .map(|p| SerializedFileReader::try_from(*p).unwrap())
    .flat_map(|r| r.into_iter());

for row in rows {
    println!("{}", row.unwrap());
}
```

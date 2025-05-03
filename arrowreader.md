# ParquetRecordBatchStreamBuilder

```rust
pub async fn new(input: T) -> Result<Self>
```

Create a new ParquetRecordBatchStreamBuilder for reading from the specified source.

```rust
// Use tokio::fs::File to read data using an async I/O. This can be replaced with
// another async I/O reader such as a reader from an object store.
let file = tokio::fs::File::open(path).await.unwrap();

// Configure options for reading from the async souce
let builder = ParquetRecordBatchStreamBuilder::new(file)
    .await
    .unwrap();
// Building the stream opens the parquet file (reads metadata, etc) and returns
// a stream that can be used to incrementally read the data in batches
let stream = builder.build().unwrap();
// In this example, we collect the stream into a Vec<RecordBatch>
// but real applications would likely process the batches as they are read
let results = stream.try_collect::<Vec<_>>().await.unwrap();
// Demonstrate the results are as expected
assert_batches_eq(
    &results,
    &[
      "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
      "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
      "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
      "| 4  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30332f30312f3039 | 30         | 2009-03-01T00:00:00 |",
      "| 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01T00:01:00 |",
      "| 6  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30342f30312f3039 | 30         | 2009-04-01T00:00:00 |",
      "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01T00:01:00 |",
      "| 2  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30322f30312f3039 | 30         | 2009-02-01T00:00:00 |",
      "| 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01T00:01:00 |",
      "| 0  | true     | 0           | 0            | 0       | 0          | 0.0       | 0.0        | 30312f30312f3039 | 30         | 2009-01-01T00:00:00 |",
      "| 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01T00:01:00 |",
      "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
     ],
 );
```

# Example configuring options and reading metadata

There are many options that control the behavior of the reader, such as with_batch_size, with_projection, with_filter, etc…

```rust
// As before, use tokio::fs::File to read data using an async I/O.
let file = tokio::fs::File::open(path).await.unwrap();

// Configure options for reading from the async source, in this case we set the batch size
// to 3 which produces 3 rows at a time.
let builder = ParquetRecordBatchStreamBuilder::new(file)
    .await
    .unwrap()
    .with_batch_size(3);

// We can also read the metadata to inspect the schema and other metadata
// before actually reading the data
let file_metadata = builder.metadata().file_metadata();
// Specify that we only want to read the 1st, 2nd, and 6th columns
let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1, 2, 6]);

let stream = builder.with_projection(mask).build().unwrap();
let results = stream.try_collect::<Vec<_>>().await.unwrap();
// Print out the results
assert_batches_eq(
    &results,
    &[
        "+----------+-------------+-----------+",
        "| bool_col | tinyint_col | float_col |",
        "+----------+-------------+-----------+",
        "| true     | 0           | 0.0       |",
        "| false    | 1           | 1.1       |",
        "| true     | 0           | 0.0       |",
        "| false    | 1           | 1.1       |",
        "| true     | 0           | 0.0       |",
        "| false    | 1           | 1.1       |",
        "| true     | 0           | 0.0       |",
        "| false    | 1           | 1.1       |",
        "+----------+-------------+-----------+",
     ],
 );

// The results has 8 rows, so since we set the batch size to 3, we expect
// 3 batches, two with 3 rows each and the last batch with 2 rows.
assert_eq!(results.len(), 3);
```

# Example of reading from multiple streams in parallel

```rust
// open file with parquet data
let mut file = tokio::fs::File::from_std(file);
// load metadata once
let meta = ArrowReaderMetadata::load_async(&mut file, Default::default()).await.unwrap();
// create two readers, a and b, from the same underlying file
// without reading the metadata again
let mut a = ParquetRecordBatchStreamBuilder::new_with_metadata(
    file.try_clone().await.unwrap(),
    meta.clone()
).build().unwrap();
let mut b = ParquetRecordBatchStreamBuilder::new_with_metadata(file, meta).build().unwrap();

// Can read batches from both readers in parallel
assert_eq!(
  a.next().await.unwrap().unwrap(),
  b.next().await.unwrap().unwrap(),
);
```

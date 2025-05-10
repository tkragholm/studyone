Compiling par-reader v0.1.0 (/Users/tobiaskragholm/par-reader)
warning: variable `total_filtered_rows` is assigned to, but never used
--> src/examples/conversion_example.rs:57:13
|
57 | let mut total_filtered_rows = 0;
| ^^^^^^^^^^^^^^^^^^^
|
= note: consider using `_total_filtered_rows` instead
= note: `#[warn(unused_variables)]` on by default

warning: `par-reader` (lib) generated 1 warning
Finished `dev` profile [unoptimized + debuginfo] target(s) in 2.13s
Running `target/debug/par-reader`
Loading BEF data from: /Users/tobiaskragholm/generated_data/parquet/bef
[2025-05-10T12:04:55Z INFO par_reader::examples::parrallel_loader] Found 35 parquet files in /Users/tobiaskragholm/generated_data/parquet/bef, applying filter expression
Loaded 5858 record batches with 5991661 total rows
Filtered record batches by date: 1390946 individuals with birth dates between 1995-01-01 and 2018-12-31

Sample Individuals (first 5):
Individual 1: PNR=190797-3993, Gender=Female, Birth date=Some(1997-07-19), Origin=Unknown
Individual 2: PNR=020806-9631, Gender=Female, Birth date=Some(2006-08-02), Origin=Unknown
Individual 3: PNR=250898-1194, Gender=Male, Birth date=Some(1998-08-25), Origin=Unknown
Individual 4: PNR=021295-5589, Gender=Male, Birth date=Some(1995-12-02), Origin=Unknown
Individual 5: PNR=290709-6560, Gender=Male, Birth date=Some(2009-07-29), Origin=Unknown
Successfully processed 1390946 Individual records

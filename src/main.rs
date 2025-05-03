use par_reader::{
    ParquetReader, ParquetReaderConfig, ParquetReaderError, Result,
    schema::SchemaCompatibilityReport,
};
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;

fn main() -> Result<()> {
    // Create reader config
    let config = ParquetReaderConfig {
        read_page_indexes: true,
        validate_schema: true,
        fail_on_schema_incompatibility: false,
        ..Default::default()
    };

    // Real files for testing
    let paths = vec![
        "/Users/tobiaskragholm/generated_data/parquet/akm/2020.parquet",
        "/Users/tobiaskragholm/generated_data/parquet/akm/2021.parquet",
        "/Users/tobiaskragholm/generated_data/parquet/akm/2022.parquet",
    ];

    let mut reader = ParquetReader::new();

    // Use string slices directly
    let path_refs: Vec<&str> = paths.iter().map(|s| s.as_ref()).collect();

    // Preload all files to cache their metadata
    println!("Preloading files to cache metadata...");
    for path in &path_refs {
        match reader.read_file(path) {
            Ok(_) => println!("  Loaded {}", path),
            Err(e) => println!("  Failed to load {}: {}", path, e),
        }
    }

    // Simplified approach: just read files directly
    println!("\nReading files individually:");

    for path in &paths {
        println!("\nFile: {}", path);

        match File::open(path) {
            Ok(file) => {
                match SerializedFileReader::new(file) {
                    Ok(reader) => {
                        let metadata = reader.metadata();
                        println!("  Number of rows: {}", metadata.file_metadata().num_rows());
                        println!("  Number of row groups: {}", metadata.num_row_groups());

                        // Print column names
                        let schema = metadata.file_metadata().schema();
                        println!("  Columns:");
                        for field in schema.get_fields() {
                            println!("    - {}", field.name());
                        }

                        // Print a few rows
                        println!("  Sample rows:");
                        let mut row_iter = reader.into_iter();
                        for i in 0..3 {
                            match row_iter.next() {
                                Some(Ok(row)) => println!("    Row {}: {}", i, row),
                                Some(Err(e)) => println!("    Error: {}", e),
                                None => break,
                            }
                        }
                    }
                    Err(e) => println!("  Error reading parquet file: {}", e),
                }
            }
            Err(e) => println!("  Error opening file: {}", e),
        }
    }

    // Simple sequential reading
    println!("\nSimple sequential multi-file reading:");
    let mut total_rows = 0;

    for path in &paths {
        if let Ok(file) = File::open(path) {
            if let Ok(reader) = SerializedFileReader::new(file) {
                let file_rows = reader.metadata().file_metadata().num_rows();
                total_rows += file_rows;
                println!("  Read {} rows from {}", file_rows, path);
            }
        }
    }

    println!("  Total rows: {}", total_rows);

    // Example 3: Read metadata with page indexes
    println!("\nReading metadata with page indexes:");
    if let Some(path) = paths.first() {
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Error opening file {}: {}", path, e);
                return Err(e.into());
            }
        };

        let mut metadata_reader = ParquetMetaDataReader::new().with_page_indexes(true);

        match metadata_reader.try_parse(&file) {
            Ok(_) => {
                let metadata = metadata_reader.finish().unwrap();
                println!("Successfully read metadata with page indexes");
                println!("  Number of row groups: {}", metadata.num_row_groups());
                println!(
                    "  Number of columns: {}",
                    metadata.file_metadata().schema().get_fields().len()
                );
                println!("  Has column index: {}", metadata.column_index().is_some());
                println!("  Has offset index: {}", metadata.offset_index().is_some());
            }
            Err(e) => eprintln!("Error reading metadata: {}", e),
        }
    }

    Ok(())
}
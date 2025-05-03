use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::types::Type;
use std::collections::HashMap;
use std::sync::Arc;
use std::{fs::File, io, path::Path};

/// A struct that represents the compatibility between parquet file schemas
#[derive(Debug)]
pub struct SchemaCompatibilityReport {
    /// Whether all schemas are compatible
    pub compatible: bool,
    /// List of incompatibility issues, if any
    pub issues: Vec<SchemaIssue>,
}

/// A schema compatibility issue
#[derive(Debug)]
pub struct SchemaIssue {
    /// The path of the file that has incompatible schema
    pub file_path: String,
    /// The reference file path being compared to
    pub reference_path: String,
    /// Description of the incompatibility
    pub description: String,
}

/// A struct for reading Parquet files with schema validation
pub struct ParquetReader {
    metadata_cache: HashMap<String, Arc<parquet::file::metadata::ParquetMetaData>>,
}

impl Default for ParquetReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetReader {
    /// Creates a new ParquetReader
    pub fn new() -> Self {
        ParquetReader {
            metadata_cache: HashMap::new(),
        }
    }

    /// Reads a single Parquet file and returns the reader
    pub fn read_file(&mut self, path: &str) -> ParquetResult<SerializedFileReader<File>> {
        let path = Path::new(path);
        let file = File::open(path).map_err(|e| {
            parquet::errors::ParquetError::General(format!(
                "Failed to open file {}: {}",
                path.display(),
                e
            ))
        })?;

        let reader = SerializedFileReader::new(file)?;

        // Cache metadata for later schema comparison
        let file_path = path.to_string_lossy().to_string();
        self.metadata_cache.entry(file_path).or_insert_with(|| {
            let metadata = reader.metadata().clone();
            Arc::new(metadata)
        });

        Ok(reader)
    }

    /// Validates that all files in the list have compatible schemas
    pub fn validate_schemas(&self, paths: &[&str]) -> ParquetResult<()> {
        if paths.is_empty() || paths.len() == 1 {
            return Ok(());
        }

        // Read the first file's schema to compare with others
        let first_path = paths[0];
        let first_metadata = match self.metadata_cache.get(first_path) {
            Some(metadata) => metadata,
            None => {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Metadata for {} not found in cache",
                    first_path
                )));
            }
        };

        let first_schema = first_metadata.file_metadata().schema();
        let first_num_columns = first_metadata.file_metadata().schema().get_fields().len();

        // Compare with all other files
        for path in &paths[1..] {
            let metadata = match self.metadata_cache.get(*path) {
                Some(metadata) => metadata,
                None => {
                    return Err(parquet::errors::ParquetError::General(format!(
                        "Metadata for {} not found in cache",
                        path
                    )));
                }
            };

            let current_schema = metadata.file_metadata().schema();
            let current_num_columns = metadata.file_metadata().schema().get_fields().len();

            // First check if number of columns match
            if first_num_columns != current_num_columns {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Number of columns in {} ({}) doesn't match {} ({})",
                    path, current_num_columns, first_path, first_num_columns
                )));
            }

            // Then do detailed schema comparison
            if !schemas_compatible(first_schema, current_schema) {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Schema for {} is incompatible with {}",
                    path, first_path
                )));
            }
        }

        Ok(())
    }

    /// Returns detailed schema compatibility report
    pub fn get_schema_compatibility_report(
        &self,
        paths: &[&str],
    ) -> ParquetResult<SchemaCompatibilityReport> {
        if paths.is_empty() || paths.len() == 1 {
            return Ok(SchemaCompatibilityReport {
                compatible: true,
                issues: vec![],
            });
        }

        let mut report = SchemaCompatibilityReport {
            compatible: true,
            issues: vec![],
        };

        // Read the first file's schema
        let first_path = paths[0];
        let first_metadata = match self.metadata_cache.get(first_path) {
            Some(metadata) => metadata,
            None => {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Metadata for {} not found in cache",
                    first_path
                )));
            }
        };

        let first_schema = first_metadata.file_metadata().schema();

        // Compare with all other files
        for path in &paths[1..] {
            let metadata = match self.metadata_cache.get(*path) {
                Some(metadata) => metadata,
                None => {
                    return Err(parquet::errors::ParquetError::General(format!(
                        "Metadata for {} not found in cache",
                        path
                    )));
                }
            };

            let current_schema = metadata.file_metadata().schema();

            // Do detailed schema comparison and collect issues
            let issues =
                find_schema_incompatibilities(first_schema, current_schema, first_path, path);

            if !issues.is_empty() {
                report.compatible = false;
                report.issues.extend(issues);
            }
        }

        Ok(report)
    }

    /// Reads multiple Parquet files and returns their rows as an iterator
    pub fn read_files<'a>(
        &'a mut self,
        paths: &'a [&'a str],
    ) -> ParquetResult<ParquetRowIterator<'a>> {
        // First validate schemas
        self.validate_schemas(paths)?;

        // Pre-cache metadata for all files
        for path in paths {
            if !self.metadata_cache.contains_key(*path) {
                let _ = self.read_file(path)?;
            }
        }

        Ok(ParquetRowIterator {
            reader: self,
            paths,
            current_path_idx: 0,
            current_iter: None,
        })
    }
}

/// Iterator over rows from multiple Parquet files
pub struct ParquetRowIterator<'a> {
    reader: &'a mut ParquetReader,
    paths: &'a [&'a str],
    current_path_idx: usize,
    current_iter: Option<Box<dyn Iterator<Item = ParquetResult<parquet::record::Row>> + 'a>>,
}

impl Iterator for ParquetRowIterator<'_> {
    type Item = ParquetResult<parquet::record::Row>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current iterator, try to get the next row
            if let Some(iter) = &mut self.current_iter {
                if let Some(row) = iter.next() {
                    return Some(row);
                }
            }

            // If we're out of rows in the current file, move to the next file
            if self.current_path_idx < self.paths.len() {
                let path = self.paths[self.current_path_idx];
                match self.reader.read_file(path) {
                    Ok(reader) => {
                        self.current_iter = Some(Box::new(reader.into_iter()));
                        self.current_path_idx += 1;
                    }
                    Err(e) => {
                        // Return the error and advance to the next file
                        self.current_path_idx += 1;
                        return Some(Err(e));
                    }
                };
            } else {
                // We've processed all files
                return None;
            }
        }
    }
}

/// Checks if two schemas are compatible for merging datasets
fn schemas_compatible(schema1: &Type, schema2: &Type) -> bool {
    // For simplicity, we check that the schema names and structures are identical
    // In a real-world scenario, you might want to implement a more sophisticated
    // compatibility check depending on your use case

    // Check name
    if schema1.name() != schema2.name() {
        return false;
    }

    // Check repetition
    if schema1.get_basic_info().repetition() != schema2.get_basic_info().repetition() {
        return false;
    }

    // Check physical type only for primitive types
    if schema1.is_primitive() && schema2.is_primitive() {
        if schema1.get_physical_type() != schema2.get_physical_type() {
            return false;
        }
    } else if schema1.is_primitive() != schema2.is_primitive() {
        // One is primitive and one isn't
        return false;
    }

    // For group types, check children
    if schema1.is_group() && schema2.is_group() {
        let fields1 = schema1.get_fields();
        let fields2 = schema2.get_fields();

        if fields1.len() != fields2.len() {
            return false;
        }

        for (f1, f2) in fields1.iter().zip(fields2.iter()) {
            if !schemas_compatible(f1, f2) {
                return false;
            }
        }
    }

    true
}

/// Finds and returns detailed incompatibilities between two schemas
fn find_schema_incompatibilities(
    schema1: &Type,
    schema2: &Type,
    reference_path: &str,
    file_path: &str,
) -> Vec<SchemaIssue> {
    let mut issues = Vec::new();

    if schema1.name() != schema2.name() {
        issues.push(SchemaIssue {
            file_path: file_path.to_string(),
            reference_path: reference_path.to_string(),
            description: format!(
                "Schema name mismatch: '{}' vs '{}'",
                schema1.name(),
                schema2.name()
            ),
        });
    }

    // Compare field types and names for struct types (most common case)
    let fields1 = schema1.get_fields();
    let fields2 = schema2.get_fields();
    if !fields1.is_empty() && !fields2.is_empty() {
        if fields1.len() != fields2.len() {
            issues.push(SchemaIssue {
                file_path: file_path.to_string(),
                reference_path: reference_path.to_string(),
                description: format!(
                    "Different number of fields: {} vs {}",
                    fields1.len(),
                    fields2.len()
                ),
            });
            return issues; // Early return as field count mismatch makes further comparisons difficult
        }

        // Compare each field
        for (i, (f1, f2)) in fields1.iter().zip(fields2.iter()).enumerate() {
            if f1.name() != f2.name() {
                issues.push(SchemaIssue {
                    file_path: file_path.to_string(),
                    reference_path: reference_path.to_string(),
                    description: format!(
                        "Field name mismatch at position {}: '{}' vs '{}'",
                        i,
                        f1.name(),
                        f2.name()
                    ),
                });
            }

            // Check for type compatibility
            if !types_compatible(f1, f2) {
                let type_description = if f1.is_primitive() && f2.is_primitive() {
                    format!(
                        "Field type mismatch for '{}': {:?} vs {:?}",
                        f1.name(),
                        f1.get_physical_type(),
                        f2.get_physical_type()
                    )
                } else if f1.is_primitive() {
                    format!(
                        "Field type mismatch for '{}': primitive vs group",
                        f1.name()
                    )
                } else {
                    format!(
                        "Field type mismatch for '{}': group vs primitive",
                        f1.name()
                    )
                };

                issues.push(SchemaIssue {
                    file_path: file_path.to_string(),
                    reference_path: reference_path.to_string(),
                    description: type_description,
                });
            }

            // Recursively check nested fields
            if f1.is_group() && f2.is_group() {
                let nested_issues =
                    find_schema_incompatibilities(f1, f2, reference_path, file_path);
                issues.extend(nested_issues);
            }
        }
    }

    issues
}

/// Checks if two field types are compatible
fn types_compatible(field1: &Type, field2: &Type) -> bool {
    // Check repetition - nullable vs required could matter depending on your use case
    if field1.get_basic_info().repetition() != field2.get_basic_info().repetition() {
        return false;
    }

    // Check if both are the same kind (primitive or group)
    if field1.is_primitive() != field2.is_primitive() {
        return false;
    }

    // For primitive types, check physical type
    if field1.is_primitive()
        && field2.is_primitive()
        && field1.get_physical_type() != field2.get_physical_type()
    {
        return false;
    }

    // For group types, we'd check children structure,
    // but that's handled by the recursive schema comparison

    true
}

/// Specialized error type for the ParquetReader
#[derive(Debug)]
pub enum ParquetReaderError {
    /// Error opening or reading a file
    IoError(io::Error),
    /// Error processing Parquet data
    ParquetError(parquet::errors::ParquetError),
    /// Error with schema compatibility
    SchemaError(String),
    /// Error with file metadata
    MetadataError(String),
}

impl From<io::Error> for ParquetReaderError {
    fn from(error: io::Error) -> Self {
        ParquetReaderError::IoError(error)
    }
}

impl From<parquet::errors::ParquetError> for ParquetReaderError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        ParquetReaderError::ParquetError(error)
    }
}

impl std::fmt::Display for ParquetReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetReaderError::IoError(e) => write!(f, "IO error: {}", e),
            ParquetReaderError::ParquetError(e) => write!(f, "Parquet error: {}", e),
            ParquetReaderError::SchemaError(msg) => write!(f, "Schema error: {}", msg),
            ParquetReaderError::MetadataError(msg) => write!(f, "Metadata error: {}", msg),
        }
    }
}

impl std::error::Error for ParquetReaderError {}

/// Result type for ParquetReader operations
pub type Result<T> = std::result::Result<T, ParquetReaderError>;

/// Configuration for the ParquetReader
#[derive(Debug, Clone)]
pub struct ParquetReaderConfig {
    /// Whether to read page indexes
    pub read_page_indexes: bool,
    /// Whether to perform schema validation
    pub validate_schema: bool,
    /// Whether to fail on schema incompatibility
    pub fail_on_schema_incompatibility: bool,
    /// Buffer size for reading files
    pub buffer_size: usize,
}

impl Default for ParquetReaderConfig {
    fn default() -> Self {
        ParquetReaderConfig {
            read_page_indexes: false,
            validate_schema: true,
            fail_on_schema_incompatibility: true,
            buffer_size: 8192,
        }
    }
}

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

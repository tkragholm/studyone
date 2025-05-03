//! Module for reading Parquet files with schema validation.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use parquet::errors::Result as ParquetResult;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::schema::{find_schema_incompatibilities, schemas_compatible, SchemaCompatibilityReport};

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
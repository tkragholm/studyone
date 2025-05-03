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
    // Maximum number of entries to keep in the metadata cache
    max_cache_size: usize,
}

impl Default for ParquetReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ParquetReader {
    /// Creates a new `ParquetReader` with default settings
    #[must_use] pub fn new() -> Self {
        // Default to caching metadata for up to 100 files
        Self::with_cache_size(100)
    }
    
    /// Creates a new `ParquetReader` with a specific cache size
    #[must_use] pub fn with_cache_size(max_cache_size: usize) -> Self {
        Self {
            metadata_cache: HashMap::with_capacity(max_cache_size),
            max_cache_size,
        }
    }

    /// Reads a single Parquet file and returns the reader
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or if the Parquet file is invalid
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
        
        // Check if we need to evict some cache entries
        if self.metadata_cache.len() >= self.max_cache_size {
            // Evict least recently used entries (simple approach: just remove 20% of entries)
            let num_to_remove = self.max_cache_size / 5;
            if num_to_remove > 0 {
                let keys_to_remove: Vec<String> = self.metadata_cache
                    .keys()
                    .take(num_to_remove)
                    .cloned()
                    .collect();
                    
                for key in keys_to_remove {
                    self.metadata_cache.remove(&key);
                }
            }
        }
        
        self.metadata_cache.entry(file_path).or_insert_with(|| {
            let metadata = reader.metadata().clone();
            Arc::new(metadata)
        });

        Ok(reader)
    }

    /// Validates that all files in the list have compatible schemas
    ///
    /// # Errors
    /// Returns an error if any of the file schemas are incompatible or if metadata cannot be found
    pub fn validate_schemas(&self, paths: &[&str]) -> ParquetResult<()> {
        if paths.is_empty() || paths.len() == 1 {
            return Ok(());
        }

        // Read the first file's schema to compare with others
        let first_path = paths[0];
        let Some(first_metadata) = self.metadata_cache.get(first_path) else {
            return Err(parquet::errors::ParquetError::General(format!(
                "Metadata for {first_path} not found in cache"
            )));
        };

        let first_schema = first_metadata.file_metadata().schema();
        let first_num_columns = first_metadata.file_metadata().schema().get_fields().len();

        // Compare with all other files
        for path in &paths[1..] {
            let Some(metadata) = self.metadata_cache.get(*path) else {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Metadata for {path} not found in cache"
                )));
            };

            let current_schema = metadata.file_metadata().schema();
            let current_num_columns = metadata.file_metadata().schema().get_fields().len();

            // First check if number of columns match
            if first_num_columns != current_num_columns {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Number of columns in {path} ({current_num_columns}) doesn't match {first_path} ({first_num_columns})"
                )));
            }

            // Then do detailed schema comparison
            if !schemas_compatible(first_schema, current_schema) {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Schema for {path} is incompatible with {first_path}"
                )));
            }
        }

        Ok(())
    }

    /// Returns detailed schema compatibility report
    ///
    /// # Errors
    /// Returns an error if metadata for any of the files cannot be found in the cache
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
        let Some(first_metadata) = self.metadata_cache.get(first_path) else {
            return Err(parquet::errors::ParquetError::General(format!(
                "Metadata for {first_path} not found in cache"
            )));
        };

        let first_schema = first_metadata.file_metadata().schema();

        // Compare with all other files
        for path in &paths[1..] {
            let Some(metadata) = self.metadata_cache.get(*path) else {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Metadata for {path} not found in cache"
                )));
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
    ///
    /// # Errors
    /// Returns an error if schemas are incompatible or if any file cannot be read
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
                }
            } else {
                // We've processed all files
                return None;
            }
        }
    }
}
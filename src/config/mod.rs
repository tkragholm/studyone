//! Configuration for ParquetReader.

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

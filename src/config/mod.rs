//! Configuration for `ParquetReader`.

use crate::schema::adapters::DateFormatConfig;

/// Configuration for the `ParquetReader`
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
    /// Enable automatic data type adaptation when schemas don't match
    pub adapt_types: bool,
    /// Strict mode for type adaptation (fail on incompatible types)
    pub strict_adaptation: bool,
    /// Log all type adaptations for debugging
    pub log_adaptations: bool,
    /// Date format configuration for string-to-date conversions
    pub date_format_config: DateFormatConfig,
}

impl Default for ParquetReaderConfig {
    fn default() -> Self {
        Self {
            read_page_indexes: false,
            validate_schema: true,
            fail_on_schema_incompatibility: true,
            buffer_size: 8192,
            adapt_types: true,
            strict_adaptation: false,
            log_adaptations: true,
            date_format_config: DateFormatConfig::default(),
        }
    }
}

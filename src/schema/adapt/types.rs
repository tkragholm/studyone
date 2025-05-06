//! Core types and error definitions for schema adaptation.

use arrow::error::ArrowError;

/// Errors that can occur during type adaptation
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),

    /// Error during type conversion
    #[error("Type conversion error: {0}")]
    ConversionError(String),

    /// Date parsing error
    #[error("Date parsing error: {0}")]
    DateParsingError(String),

    /// Validation error
    #[error("Validation error: {0}")]
    ValidationError(String),
}

/// Alias for Result with `AdapterError`
pub type Result<T> = std::result::Result<T, AdapterError>;

/// Types of data type compatibility
#[derive(Debug, PartialEq, Eq)]
pub enum TypeCompatibility {
    /// Types match exactly
    Exact,
    /// Types can be automatically converted
    Compatible,
    /// Types are incompatible
    Incompatible,
}

/// Available strategies for type adaptation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptationStrategy {
    /// Automatically cast using Arrow's cast functionality
    AutoCast,
    /// Parse date strings into date types
    DateParsing,
    /// Convert dates/timestamps to strings
    DateFormatting,
    /// Convert to string representation
    StringConversion,
    /// Convert numeric types (widening)
    NumericConversion,
    /// Convert boolean values
    BooleanConversion,
}

/// Configuration for date format handling
#[derive(Debug, Clone)]
pub struct DateFormatConfig {
    /// List of date format strings to try when parsing dates
    pub date_formats: Vec<String>,
    /// Default date format to use when converting dates to strings
    pub default_format: String,
    /// Enable heuristic format detection
    pub enable_format_detection: bool,
}

impl Default for DateFormatConfig {
    fn default() -> Self {
        Self {
            date_formats: vec![
                "%Y-%m-%d".to_string(), // ISO format: 2023-01-15
                "%d-%m-%Y".to_string(), // European: 15-01-2023
                "%m/%d/%Y".to_string(), // US: 01/15/2023
                "%d/%m/%Y".to_string(), // UK: 15/01/2023
                "%d.%m.%Y".to_string(), // German/Danish: 15.01.2023
                "%Y%m%d".to_string(),   // Compact: 20230115
                "%d %b %Y".to_string(), // 15 Jan 2023
                "%d %B %Y".to_string(), // 15 January 2023
            ],
            default_format: "%Y-%m-%d".to_string(),
            enable_format_detection: true,
        }
    }
}
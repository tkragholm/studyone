//! Module for handling data type adaptation between mismatched schemas.

pub mod compatibility;
pub mod conversions;
pub mod date_utils;
pub mod schema_compat;
pub mod types;

// Re-export the main types and functions for easier access
pub use compatibility::{
    check_type_compatibility, determine_adaptation_strategy, is_numeric, is_string, is_temporal,
};
pub use conversions::{convert_array, create_null_array};
pub use date_utils::{detect_date_format, parse_date_string};
pub use schema_compat::{
    EnhancedSchemaCompatibilityReport, SchemaAdaptation, SchemaAdaptationIssue, adapt_record_batch,
    check_schema_with_adaptation,
};
pub use types::{AdaptationStrategy, AdapterError, DateFormatConfig, Result, TypeCompatibility};

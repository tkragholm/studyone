//! Module for handling data type adaptation between mismatched schemas.

pub mod types;
pub mod compatibility;
pub mod schema_compat;
pub mod date_utils;
pub mod conversions;

// Re-export the main types and functions for easier access
pub use types::{
    AdapterError, Result, TypeCompatibility, AdaptationStrategy, DateFormatConfig,
};
pub use compatibility::{
    check_type_compatibility, is_numeric, is_string, is_temporal, determine_adaptation_strategy,
};
pub use schema_compat::{
    EnhancedSchemaCompatibilityReport, SchemaAdaptationIssue, SchemaAdaptation, 
    check_schema_with_adaptation, adapt_record_batch,
};
pub use conversions::{convert_array, create_null_array};
pub use date_utils::{parse_date_string, detect_date_format};
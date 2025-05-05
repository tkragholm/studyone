//! VNDS schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for VNDS data
///
/// The VNDS (Vandringer/Migration) registry contains migration information.
#[must_use] pub fn vnds_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("INDUD_KODE", DataType::Utf8, true),  // Migration code (in/out)
        Field::new("HAEND_DATO", DataType::Utf8, true),  // Event date
    ]))
}

/// Create schema for standardized version of VNDS register data
#[must_use] pub fn vnds_standardized_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("MIGRATION_TYPE", DataType::Utf8, true),  // "IN" or "OUT"
        Field::new("MIGRATION_DATE", DataType::Date32, true),  // Standardized date
    ]))
}
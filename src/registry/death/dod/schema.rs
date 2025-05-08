//! DOD schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for DOD data
///
/// The DOD registry contains death records.
#[must_use] pub fn dod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DODDATO", DataType::Utf8, true),
    ]))
}

/// Create schema for standardized version of DOD register data
#[must_use] pub fn dod_standardized_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DEATH_DATE", DataType::Date32, true),
    ]))
}
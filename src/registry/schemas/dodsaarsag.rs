//! DODSAARSAG schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for DODSAARSAG data
///
/// The DODSAARSAG registry contains cause of death records.
#[must_use] pub fn dodsaarsag_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("C_AARSAG", DataType::Utf8, true),  // Cause of death code (ICD-10)
        Field::new("C_TILSTAND", DataType::Utf8, true),  // Condition code
    ]))
}

/// Create schema for standardized version of DODSAARSAG register data
#[must_use] pub fn dodsaarsag_standardized_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DEATH_CAUSE", DataType::Utf8, true),  // Normalized cause code
        Field::new("DEATH_CONDITION", DataType::Utf8, true),  // Normalized condition code
        Field::new("DEATH_CAUSE_CHAPTER", DataType::Utf8, true),  // ICD-10 chapter of death cause
    ]))
}
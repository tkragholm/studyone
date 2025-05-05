//! DODSAARSAG schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for DODSAARSAG data
///
/// The DODSAARSAG registry contains cause of death records.
#[must_use] pub fn dodsaarsag_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("C_AARSAG", DataType::Utf8, true), // Primary cause of death
        Field::new("D_AARSAG1", DataType::Utf8, true), // Contributing cause of death 1
        Field::new("D_AARSAG2", DataType::Utf8, true), // Contributing cause of death 2
        Field::new("D_AARSAG3", DataType::Utf8, true), // Contributing cause of death 3
        Field::new("D_AARSAG4", DataType::Utf8, true), // Contributing cause of death 4
        Field::new("DOD_DAG", DataType::Date32, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
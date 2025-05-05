//! DOD schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for DOD data
///
/// The DOD registry contains death records.
pub fn dod_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DOD_DAG", DataType::Date32, true),
        Field::new("DOD_KODE", DataType::Utf8, true),
        Field::new("DOD_STED", DataType::Utf8, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
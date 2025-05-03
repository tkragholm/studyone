//! IND schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for IND data
#[must_use] pub fn ind_schema() -> Schema {
    Schema::new(vec![
        Field::new("BESKST13", DataType::Int8, true),
        Field::new("CPRTJEK", DataType::Utf8, true),
        Field::new("CPRTYPE", DataType::Utf8, true),
        Field::new("LOENMV_13", DataType::Float64, true),
        Field::new("PERINDKIALT_13", DataType::Float64, true),
        Field::new("PNR", DataType::Utf8, false),
        Field::new("PRE_SOCIO", DataType::Int8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ])
}

/// Get the Arrow schema for IND data as an Arc
#[must_use] pub fn ind_schema_arc() -> Arc<Schema> {
    Arc::new(ind_schema())
}
//! UDDF schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for UDDF data
#[must_use] pub fn uddf_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("CPRTJEK", DataType::Utf8, true),
        Field::new("CPRTYPE", DataType::Utf8, true),
        Field::new("HFAUDD", DataType::Utf8, true),
        Field::new("HF_KILDE", DataType::Utf8, true),
        Field::new("HF_VFRA", DataType::Utf8, true),
        Field::new("HF_VTIL", DataType::Utf8, true),
        Field::new("INSTNR", DataType::Int8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ])
}

/// Get the Arrow schema for UDDF data as an Arc
#[must_use] pub fn uddf_schema_arc() -> Arc<Schema> {
    Arc::new(uddf_schema())
}
//! UDDF schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for UDDF data
///
/// The UDDF (Uddannelse) registry contains educational information.
#[must_use] pub fn uddf_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("CPRTJEK", DataType::Utf8, true),
        Field::new("CPRTYPE", DataType::Utf8, true),
        Field::new("HFAUDD", DataType::Utf8, true),
        Field::new("HF_KILDE", DataType::Utf8, true),
        Field::new("HF_VFRA", DataType::Utf8, true),
        Field::new("HF_VTIL", DataType::Utf8, true),
        Field::new("INSTNR", DataType::Int8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ]))
}
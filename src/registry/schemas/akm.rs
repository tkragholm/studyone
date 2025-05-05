//! AKM schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for AKM data
///
/// The AKM (Arbejdsklassifikationsmodulet) registry contains employment information.
pub fn akm_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("SOCIO", DataType::Int8, true),
        Field::new("SOCIO02", DataType::Int8, true),
        Field::new("SOCIO13", DataType::Int8, true),
        Field::new("CPRTJEK", DataType::Utf8, true),
        Field::new("CPRTYPE", DataType::Utf8, true),
        Field::new("VERSION", DataType::Utf8, true),
        Field::new("SENR", DataType::Utf8, true),
    ]))
}
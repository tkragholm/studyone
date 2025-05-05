//! BEF schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for BEF data
///
/// The BEF (Befolkning) registry contains population demographic information.
pub fn bef_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("KOEN", DataType::Utf8, true),
        Field::new("FOED_DAG", DataType::Date32, true),
        Field::new("CIVILSTAND", DataType::Utf8, true),
        Field::new("POSTNR", DataType::Utf8, true),
        Field::new("KOMKODE", DataType::Utf8, true),
        Field::new("ALDER", DataType::Int16, true),
        Field::new("IE_TYPE", DataType::Utf8, true),
        Field::new("STATSB", DataType::Utf8, true),
    ]))
}
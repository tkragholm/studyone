//! LPR3_DIAGNOSER schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for LPR3_DIAGNOSER data
///
/// The LPR3_DIAGNOSER registry contains diagnosis records from the Danish National Patient Registry version 3.
pub fn lpr3_diagnoser_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("DW_EK_KONTAKT", DataType::Utf8, false), // Used to join with LPR3_KONTAKTER
        Field::new("ART", DataType::Utf8, true),
        Field::new("KODE", DataType::Utf8, true),
        Field::new("DIAGNOSE", DataType::Utf8, true),
        Field::new("SIDEANGIVELSE", DataType::Utf8, true),
        Field::new("DIAGNOSEDATO", DataType::Date32, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
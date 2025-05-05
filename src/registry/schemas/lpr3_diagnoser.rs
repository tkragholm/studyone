//! `LPR3_DIAGNOSER` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR3_DIAGNOSER` data
///
/// The `LPR3_DIAGNOSER` registry contains diagnosis records from the Danish National Patient Registry version 3.
#[must_use] pub fn lpr3_diagnoser_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("DW_EK_KONTAKT", DataType::Utf8, true),
        Field::new("diagnosekode", DataType::Utf8, true),
        Field::new("diagnosetype", DataType::Utf8, true),
        Field::new("senere_afkraeftet", DataType::Utf8, true),
        Field::new("diagnosekode_parent", DataType::Utf8, true),
        Field::new("diagnosetype_parent", DataType::Utf8, true),
        Field::new("lprindberetningssystem", DataType::Utf8, true),
    ]))
}
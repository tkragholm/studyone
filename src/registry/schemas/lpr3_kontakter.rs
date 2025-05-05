//! `LPR3_KONTAKTER` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR3_KONTAKTER` data
///
/// The `LPR3_KONTAKTER` registry contains contact records from the Danish National Patient Registry version 3.
#[must_use] pub fn lpr3_kontakter_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("DW_EK_KONTAKT", DataType::Utf8, false), // Used to join with LPR3_DIAGNOSER
        Field::new("KONTAKT_TYPE", DataType::Utf8, true),
        Field::new("KONTAKT_AARSAG", DataType::Utf8, true),
        Field::new("HENVISNINGSAARSAG", DataType::Utf8, true),
        Field::new("STARTDATO", DataType::Date32, true),
        Field::new("SLUTDATO", DataType::Date32, true),
        Field::new("SYGEHUS", DataType::Utf8, true),
        Field::new("AFDELING", DataType::Utf8, true),
        Field::new("PRIORITET", DataType::Utf8, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
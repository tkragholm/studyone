//! `LPR_BES` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR_BES` data
///
/// The `LPR_BES` registry contains treatment records from the Danish National Patient Registry.
#[must_use] pub fn lpr_bes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("RECNUM", DataType::Utf8, false), // Used to join with LPR_ADM
        Field::new("BES_TYPE", DataType::Utf8, true),
        Field::new("KODE", DataType::Utf8, true),
        Field::new("TILKODE", DataType::Utf8, true),
        Field::new("BESDAG", DataType::Date32, true),
        Field::new("BESLUT_TIME", DataType::Int32, true),
        Field::new("BESLUT_MIN", DataType::Int32, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
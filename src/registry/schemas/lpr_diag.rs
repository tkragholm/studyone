//! LPR_DIAG schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for LPR_DIAG data
///
/// The LPR_DIAG registry contains diagnosis records from the Danish National Patient Registry.
pub fn lpr_diag_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("RECNUM", DataType::Utf8, false), // Used to join with LPR_ADM
        Field::new("DIAGTYPE", DataType::Utf8, true),
        Field::new("DIAG", DataType::Utf8, true),
        Field::new("TILDIAG", DataType::Utf8, true),
        Field::new("DIAG_TEXT", DataType::Utf8, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
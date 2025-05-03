//! `LPR_DIAG` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR_DIAG` data
#[must_use] pub fn lpr_diag_schema() -> Schema {
    Schema::new(vec![
        Field::new("C_DIAG", DataType::Utf8, true),
        Field::new("C_DIAGTYPE", DataType::Utf8, true),
        Field::new("C_TILDIAG", DataType::Utf8, true),
        Field::new("LEVERANCEDATO", DataType::Date32, true),
        Field::new("RECNUM", DataType::Utf8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ])
}

/// Get the Arrow schema for `LPR_DIAG` data as an Arc
#[must_use] pub fn lpr_diag_schema_arc() -> Arc<Schema> {
    Arc::new(lpr_diag_schema())
}
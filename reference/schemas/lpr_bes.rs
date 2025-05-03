//! `LPR_BES` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR_BES` data
#[must_use] pub fn lpr_bes_schema() -> Schema {
    Schema::new(vec![
        Field::new("D_AMBDTO", DataType::Date32, true),
        Field::new("LEVERANCEDATO", DataType::Date32, true),
        Field::new("RECNUM", DataType::Utf8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ])
}

/// Get the Arrow schema for `LPR_BES` data as an Arc
#[must_use] pub fn lpr_bes_schema_arc() -> Arc<Schema> {
    Arc::new(lpr_bes_schema())
}
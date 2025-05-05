//! `LPR_BES` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR_BES` data
///
/// The `LPR_BES` registry contains treatment records from the Danish National Patient Registry.
#[must_use] pub fn lpr_bes_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("D_AMBDTO", DataType::Date32, true),
        Field::new("LEVERANCEDATO", DataType::Date32, true),
        Field::new("RECNUM", DataType::Utf8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ]))
}
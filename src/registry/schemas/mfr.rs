//! MFR schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for MFR data
///
/// The MFR (Medical Birth Registry) contains birth information.
#[must_use] pub fn mfr_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("CPR_BARN", DataType::Utf8, false),       // Child's CPR number (maps to PNR)
        Field::new("FOEDSELSDATO", DataType::Date32, true),  // Birth date (maps to FOED_DAG)
        Field::new("CPR_MODER", DataType::Utf8, true),       // Mother's CPR number (maps to MOR_ID)
        Field::new("CPR_FADER", DataType::Utf8, true),       // Father's CPR number (maps to FAR_ID)
    ]))
}
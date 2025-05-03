//! MFR schema definitions
//!
//! MFR (Medicinal Fødselsregister) contains information about births.

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for MFR data
#[must_use] pub fn mfr_schema() -> Schema {
    // Only include the fields required for population processing
    Schema::new(vec![
        Field::new("CPR_BARN", DataType::Utf8, false),       // Child's CPR number (maps to PNR)
        Field::new("FOEDSELSDATO", DataType::Date32, true),  // Birth date (maps to FOED_DAG)
        Field::new("CPR_MODER", DataType::Utf8, true),       // Mother's CPR number (maps to MOR_ID)
        Field::new("CPR_FADER", DataType::Utf8, true),       // Father's CPR number (maps to FAR_ID)
    ])
    
    // Optional fields not needed for basic population processing are omitted:
    // - FLERFOLD (Multiple birth indicator)
    // - FOED_VAEGT (Birth weight in grams)
    // - FOED_LAENGDE (Birth length in cm)
    // - GESTATIONSALDER (Gestational age in weeks)
    // - HOVEDOMFANG (Head circumference in cm)
    // - FOEDESTED (Place of birth)
    // - APGAR5 (Apgar score at 5 minutes)
    // - VERSION (Version information)
}

/// Get the Arrow schema for MFR data as an Arc
#[must_use] pub fn mfr_schema_arc() -> Arc<Schema> {
    Arc::new(mfr_schema())
}
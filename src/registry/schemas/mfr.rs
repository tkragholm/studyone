//! MFR schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for MFR data
///
/// The MFR (Medical Birth Registry) contains birth information.
pub fn mfr_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),      // Child's PNR
        Field::new("MOR_PNR", DataType::Utf8, true),   // Mother's PNR
        Field::new("FAR_PNR", DataType::Utf8, true),   // Father's PNR
        Field::new("FOED_DAG", DataType::Date32, true), // Birth date
        Field::new("FLERFOLD", DataType::Int8, true),  // Multiple births
        Field::new("BARSELDAGE", DataType::Int16, true), // Maternity days
        Field::new("FOEDSELSVGT", DataType::Int16, true), // Birth weight
        Field::new("GESTATIONSALDER", DataType::Int16, true), // Gestational age
        Field::new("APGARSCORE", DataType::Int8, true), // Apgar score
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
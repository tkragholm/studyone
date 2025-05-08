//! IDAN schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for IDAN data
///
/// The IDAN (Integrated Database for Labor Market Research) registry contains employment information.
#[must_use] pub fn idan_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("BRANCHE", DataType::Utf8, true),    // Industry
        Field::new("DISCO", DataType::Utf8, true),      // Occupation code
        Field::new("DISCO_TEXT", DataType::Utf8, true), // Occupation text
        Field::new("TIMELOEN", DataType::Float64, true), // Hourly wage
        Field::new("AARSLOEN", DataType::Float64, true), // Annual wage
        Field::new("ARBEJDSTID", DataType::Int16, true), // Working hours
        Field::new("CVR", DataType::Utf8, true),        // Company registration number
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
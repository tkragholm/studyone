//! VNDS schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for VNDS data
///
/// The VNDS (Vandringer/Migration) registry contains migration information.
pub fn vnds_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("INDV_DAG", DataType::Date32, true), // Immigration date
        Field::new("INDV_LAND", DataType::Utf8, true),  // Country of origin
        Field::new("UDVA_DAG", DataType::Date32, true), // Emigration date
        Field::new("UDVA_LAND", DataType::Utf8, true),  // Destination country
        Field::new("STATSB", DataType::Utf8, true),     // Citizenship
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
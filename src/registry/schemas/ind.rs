//! IND schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for IND data
///
/// The IND (Indkomst) registry contains income and tax information.
#[must_use] pub fn ind_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("PERSONINDK", DataType::Float64, true), // Personal income
        Field::new("LOENMV", DataType::Float64, true),     // Wage income
        Field::new("NETKAPINDMV", DataType::Float64, true), // Net capital income
        Field::new("PENSINDMP", DataType::Float64, true),  // Pension income
        Field::new("OVERFORSKAT", DataType::Float64, true), // Transfers to be taxed
        Field::new("SKAT", DataType::Float64, true),       // Tax
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
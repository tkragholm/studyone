//! IND schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

/// Get the Arrow schema for IND data
///
/// The IND (Indkomst) registry contains income and tax information.
#[must_use] pub fn ind_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("BESKST13", DataType::Int8, true),
        Field::new("CPRTJEK", DataType::Utf8, true),
        Field::new("CPRTYPE", DataType::Utf8, true),
        Field::new("LOENMV_13", DataType::Float64, true),
        Field::new("PERINDKIALT_13", DataType::Float64, true),
        Field::new("PNR", DataType::Utf8, false),
        Field::new("PRE_SOCIO", DataType::Int8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ]))
}

/// Field mapping from IND registry to SerdeIndividual
///
/// This function provides a mapping between IND registry field names and
/// the corresponding field names in the SerdeIndividual struct.
#[must_use] pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    mapping.insert("PNR".to_string(), "pnr".to_string());
    mapping.insert("PERINDKIALT_13".to_string(), "annual_income".to_string());
    mapping.insert("DISPON_NY".to_string(), "disposable_income".to_string());
    mapping.insert("LOENMV_13".to_string(), "employment_income".to_string());
    mapping.insert("NETOVSKUD".to_string(), "self_employment_income".to_string());
    mapping.insert("KPITALIND".to_string(), "capital_income".to_string());
    mapping.insert("OFFHJ".to_string(), "transfer_income".to_string());
    mapping.insert("AAR".to_string(), "income_year".to_string());
    mapping.insert("PRE_SOCIO".to_string(), "socioeconomic_status_code".to_string());
    mapping
}
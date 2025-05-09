//! BEF schema definitions
//!
//! The BEF (Befolkning) registry contains population demographic information.

use arrow::datatypes::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

/// Get the Arrow schema for BEF data
#[must_use] pub fn bef_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("FOED_DAG", DataType::Date32, true),
        Field::new("FAR_ID", DataType::Utf8, true),
        Field::new("MOR_ID", DataType::Utf8, true),
        Field::new("FAMILIE_ID", DataType::Utf8, true),
        // Optional fields that may be useful
        Field::new("AEGTE_ID", DataType::Utf8, true),
        Field::new("ALDER", DataType::Int8, true),
        Field::new("ANTBOERNF", DataType::Int8, true),
        Field::new("ANTBOERNH", DataType::Int8, true),
        Field::new("ANTPERSF", DataType::Int8, true),
        Field::new("ANTPERSH", DataType::Int8, true),
        Field::new("BOP_VFRA", DataType::Date32, true),
        Field::new("CIVST", DataType::Utf8, true),
        Field::new("CPRTJEK", DataType::Int8, true),
        Field::new("CPRTYPE", DataType::Int8, true),
        Field::new("E_FAELLE_ID", DataType::Utf8, true),
        Field::new("FAMILIE_TYPE", DataType::Int8, true),
        Field::new("FM_MARK", DataType::Int8, true),
        Field::new("HUSTYPE", DataType::Int8, true),
        Field::new("IE_TYPE", DataType::Utf8, true),
        Field::new("KOEN", DataType::Utf8, true),
        Field::new("KOM", DataType::Int8, true),
        Field::new("OPR_LAND", DataType::Utf8, true),
        Field::new("PLADS", DataType::Int8, true),
        Field::new("REG", DataType::Int8, true),
        Field::new("STATSB", DataType::Int8, true),
        Field::new("VERSION", DataType::Utf8, true),
    ]))
}

/// Field mapping from BEF registry to SerdeIndividual
///
/// This function provides a mapping between BEF registry field names and
/// the corresponding field names in the SerdeIndividual struct.
#[must_use] pub fn field_mapping() -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    mapping.insert("PNR".to_string(), "pnr".to_string());
    mapping.insert("KOEN".to_string(), "gender".to_string());
    mapping.insert("FOED_DAG".to_string(), "birth_date".to_string());
    mapping.insert("MOR_ID".to_string(), "mother_pnr".to_string());
    mapping.insert("FAR_ID".to_string(), "father_pnr".to_string());
    mapping.insert("OPR_LAND".to_string(), "origin_code".to_string());
    mapping.insert("KOM".to_string(), "municipality_code".to_string());
    mapping.insert("CIVST".to_string(), "marital_status".to_string());
    mapping.insert("STATSB".to_string(), "citizenship_status".to_string());
    mapping.insert("HUSTYPE".to_string(), "housing_type".to_string());
    mapping.insert("ANTPERSF".to_string(), "household_size".to_string());
    mapping.insert("FAMILIE_ID".to_string(), "family_id".to_string());
    mapping
}
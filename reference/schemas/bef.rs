//! BEF schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for BEF data
#[must_use] pub fn bef_schema() -> Schema {
    // The required fields for population generation
    let required_fields = vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("FOED_DAG", DataType::Date32, true),
        Field::new("FAR_ID", DataType::Utf8, true),
        Field::new("MOR_ID", DataType::Utf8, true),
        Field::new("FAMILIE_ID", DataType::Utf8, true),
    ];
    
    // Optional fields that may be present (commented out for now)
    // These fields are not currently used but documented for reference
    let _optional_fields = vec![
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
    ];
    
    // Return just the required fields
    Schema::new(required_fields)
}

/// Get the Arrow schema for BEF data as an Arc
#[must_use] pub fn bef_schema_arc() -> Arc<Schema> {
    Arc::new(bef_schema())
}
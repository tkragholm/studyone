//! BEF schema definitions
//!
//! The BEF (Befolkning) registry contains population demographic information.

use arrow::datatypes::{DataType, Field, Schema};
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
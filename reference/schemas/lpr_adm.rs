//! `LPR_ADM` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR_ADM` data
#[must_use] pub fn lpr_adm_schema() -> Schema {
    Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("C_ADIAG", DataType::Utf8, true),
        Field::new("C_AFD", DataType::Utf8, true),
        Field::new("C_HAFD", DataType::Utf8, true),
        Field::new("C_HENM", DataType::Utf8, true),
        Field::new("C_HSGH", DataType::Utf8, true),
        Field::new("C_INDM", DataType::Utf8, true),
        Field::new("C_KOM", DataType::Utf8, true),
        Field::new("C_KONTAARS", DataType::Utf8, true),
        Field::new("C_PATTYPE", DataType::Utf8, true),
        Field::new("C_SGH", DataType::Utf8, true),
        Field::new("C_SPEC", DataType::Utf8, true),
        Field::new("C_UDM", DataType::Utf8, true),
        Field::new("CPRTJEK", DataType::Utf8, true),
        Field::new("CPRTYPE", DataType::Utf8, true),
        Field::new("D_HENDTO", DataType::Date32, true),
        Field::new("D_INDDTO", DataType::Date32, true),
        Field::new("D_UDDTO", DataType::Date32, true),
        Field::new("K_AFD", DataType::Utf8, true),
        Field::new("RECNUM", DataType::Utf8, true),
        Field::new("V_ALDDG", DataType::Int32, true),
        Field::new("V_ALDER", DataType::Int32, true),
        Field::new("V_INDMINUT", DataType::Int32, true),
        Field::new("V_INDTIME", DataType::Int32, true),
        Field::new("V_SENGDAGE", DataType::Int32, true),
        Field::new("V_UDTIME", DataType::Int32, true),
        Field::new("VERSION", DataType::Utf8, true),
    ])
}

/// Get the Arrow schema for `LPR_ADM` data as an Arc
#[must_use] pub fn lpr_adm_schema_arc() -> Arc<Schema> {
    Arc::new(lpr_adm_schema())
}
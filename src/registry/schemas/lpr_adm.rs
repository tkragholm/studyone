//! LPR_ADM schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for LPR_ADM data
///
/// The LPR_ADM registry contains admission records from the Danish National Patient Registry.
pub fn lpr_adm_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("PNR", DataType::Utf8, false),
        Field::new("RECNUM", DataType::Utf8, false), // Used to join with LPR_DIAG and LPR_BES
        Field::new("INDM_DAG", DataType::Date32, true), // Admission date
        Field::new("UDM_DAG", DataType::Date32, true),  // Discharge date
        Field::new("SYGEHUSET", DataType::Utf8, true),
        Field::new("AFD", DataType::Utf8, true),
        Field::new("UDM_MAADE", DataType::Utf8, true),
        Field::new("KONTAKT", DataType::Utf8, true),
        Field::new("INDM_MAADE", DataType::Utf8, true),
        Field::new("PATTYPE", DataType::Utf8, true),
        Field::new("YEAR", DataType::Int16, true),
    ]))
}
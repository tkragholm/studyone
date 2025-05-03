//! `LPR3_KONTAKTER` schema definitions

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Get the Arrow schema for `LPR3_KONTAKTER` data
#[must_use] pub fn lpr3_kontakter_schema() -> Schema {
    Schema::new(vec![
        Field::new("SORENHED_IND", DataType::Utf8, true),
        Field::new("SORENHED_HEN", DataType::Utf8, true),
        Field::new("SORENHED_ANS", DataType::Utf8, true),
        Field::new("DW_EK_KONTAKT", DataType::Utf8, true),
        Field::new("DW_EK_FORLOEB", DataType::Utf8, true),
        Field::new("CPR", DataType::Utf8, false),
        Field::new("dato_start", DataType::Date32, true),
        Field::new("tidspunkt_start", DataType::Time32(arrow::datatypes::TimeUnit::Second), true),
        Field::new("dato_slut", DataType::Date32, true),
        Field::new("tidspunkt_slut", DataType::Time32(arrow::datatypes::TimeUnit::Second), true),
        Field::new("aktionsdiagnose", DataType::Utf8, true),
        Field::new("kontaktaarsag", DataType::Utf8, true),
        Field::new("prioritet", DataType::Utf8, true),
        Field::new("kontakttype", DataType::Utf8, true),
        Field::new("henvisningsaarsag", DataType::Utf8, true),
        Field::new("henvisningsmaade", DataType::Utf8, true),
        Field::new("dato_behandling_start", DataType::Date32, true),
        Field::new("tidspunkt_behandling_start", DataType::Time32(arrow::datatypes::TimeUnit::Second), true),
        Field::new("dato_indberetning", DataType::Date32, true),
        Field::new("lprindberetningssytem", DataType::Utf8, true),
    ])
}

/// Get the Arrow schema for `LPR3_KONTAKTER` data as an Arc
#[must_use] pub fn lpr3_kontakter_schema_arc() -> Arc<Schema> {
    Arc::new(lpr3_kontakter_schema())
}
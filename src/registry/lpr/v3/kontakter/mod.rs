//! `LPR3_KONTAKTER` registry using the macro-based approach
//!
//! The `LPR3_KONTAKTER` registry contains contact records from the Danish National Patient Registry version 3.

use crate::RegistryTrait;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveTime};
use std::sync::Arc;

// Re-export the field_mapping module
pub mod field_mapping;

// Define LPR3 KONTAKTER Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(name = "LPR3_KONTAKTER", description = "LPR v3 Contact Records")]
pub struct Lpr3KontakterRegistry {
    // Core identification fields
    #[field(name = "CPR")]
    pub pnr: String,

    #[field(name = "DW_EK_KONTAKT")]
    pub contact_id: Option<String>,

    #[field(name = "DW_EK_FORLOEB")]
    pub course_id: Option<String>,

    // Organisation fields
    #[field(name = "SORENHED_IND")]
    pub org_unit_admission: Option<String>,

    #[field(name = "SORENHED_HEN")]
    pub org_unit_referral: Option<String>,

    #[field(name = "SORENHED_ANS")]
    pub org_unit_responsible: Option<String>,

    // Date and time fields
    #[field(name = "dato_start")]
    pub start_date: Option<NaiveDate>,

    #[field(name = "tidspunkt_start")]
    pub start_time: Option<NaiveTime>,

    #[field(name = "dato_slut")]
    pub end_date: Option<NaiveDate>,

    #[field(name = "tidspunkt_slut")]
    pub end_time: Option<NaiveTime>,

    #[field(name = "dato_behandling_start")]
    pub treatment_start_date: Option<NaiveDate>,

    #[field(name = "tidspunkt_behandling_start")]
    pub treatment_start_time: Option<NaiveTime>,

    #[field(name = "dato_indberetning")]
    pub reporting_date: Option<NaiveDate>,

    // Diagnosis and contact information
    #[field(name = "aktionsdiagnose")]
    pub primary_diagnosis: Option<String>,

    #[field(name = "kontaktaarsag")]
    pub contact_reason: Option<String>,

    #[field(name = "prioritet")]
    pub priority: Option<String>,

    #[field(name = "kontakttype")]
    pub contact_type: Option<String>,

    #[field(name = "henvisningsaarsag")]
    pub referral_reason: Option<String>,

    #[field(name = "henvisningsmaade")]
    pub referral_method: Option<String>,

    // System information
    #[field(name = "lprindberetningssystem")]
    pub reporting_system: Option<String>,
}

/// Helper function to create a new LPR3 kontakter deserializer
#[must_use] pub fn create_deserializer() -> Lpr3KontakterRegistryDeserializer {
    Lpr3KontakterRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &Lpr3KontakterRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for Lpr3KontakterRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "lpr3_kontakter"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for LPR3_KONTAKTER
        let fields = vec![
            Field::new("CPR", DataType::Utf8, false),
            Field::new("DW_EK_KONTAKT", DataType::Utf8, true),
            Field::new("DW_EK_FORLOEB", DataType::Utf8, true),
            Field::new("SORENHED_IND", DataType::Utf8, true),
            Field::new("SORENHED_HEN", DataType::Utf8, true),
            Field::new("SORENHED_ANS", DataType::Utf8, true),
            Field::new("dato_start", DataType::Date32, true),
            Field::new("tidspunkt_start", DataType::Time32(TimeUnit::Second), true),
            Field::new("dato_slut", DataType::Date32, true),
            Field::new("tidspunkt_slut", DataType::Time32(TimeUnit::Second), true),
            Field::new("dato_behandling_start", DataType::Date32, true),
            Field::new(
                "tidspunkt_behandling_start",
                DataType::Time32(TimeUnit::Second),
                true,
            ),
            Field::new("dato_indberetning", DataType::Date32, true),
            Field::new("aktionsdiagnose", DataType::Utf8, true),
            Field::new("kontaktaarsag", DataType::Utf8, true),
            Field::new("prioritet", DataType::Utf8, true),
            Field::new("kontakttype", DataType::Utf8, true),
            Field::new("henvisningsaarsag", DataType::Utf8, true),
            Field::new("henvisningsmaade", DataType::Utf8, true),
            Field::new("lprindberetningssystem", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// Returns the column name containing the PNR, if any
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("CPR")
    }
}
//! LPR3_DIAGNOSER registry using the macro-based approach
//!
//! The LPR3_DIAGNOSER registry contains diagnosis records from the Danish National Patient Registry version 3.

use crate::RegistryTrait;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

// Define LPR3 DIAGNOSER Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(
    name = "LPR3_DIAGNOSER",
    description = "LPR v3 Diagnosis Records",
    id_field = "DW_EK_KONTAKT"
)]
pub struct Lpr3DiagnoserRegistry {
    // Core identification fields
    #[field(name = "DW_EK_KONTAKT")]
    pub contact_id: String,

    // Diagnosis fields
    #[field(name = "diagnosekode")]
    pub diagnosis_code: Option<String>,

    #[field(name = "diagnosetype")]
    pub diagnosis_type: Option<String>,

    #[field(name = "senere_afkraeftet")]
    pub later_disproved: Option<String>,

    #[field(name = "diagnosekode_parent")]
    pub parent_diagnosis_code: Option<String>,

    #[field(name = "diagnosetype_parent")]
    pub parent_diagnosis_type: Option<String>,

    // System information
    #[field(name = "lprindberetningssystem")]
    pub reporting_system: Option<String>,
}

/// Helper function to create a new LPR3 diagnoses deserializer
pub fn create_deserializer() -> Lpr3DiagnoserRegistryDeserializer {
    Lpr3DiagnoserRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &Lpr3DiagnoserRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for Lpr3DiagnoserRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "lpr3_diagnoser"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for LPR3_DIAGNOSER
        let fields = vec![
            Field::new("DW_EK_KONTAKT", DataType::Utf8, false),
            Field::new("diagnosekode", DataType::Utf8, true),
            Field::new("diagnosetype", DataType::Utf8, true),
            Field::new("senere_afkraeftet", DataType::Utf8, true),
            Field::new("diagnosekode_parent", DataType::Utf8, true),
            Field::new("diagnosetype_parent", DataType::Utf8, true),
            Field::new("lprindberetningssystem", DataType::Utf8, true),
        ];

        Arc::new(Schema::new(fields))
    }

    /// This registry has no PNR column, needs to be joined with KONTAKTER
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        None
    }

    /// Returns the join column name for joining with kontakter
    fn get_join_column_name(&self) -> Option<&'static str> {
        Some("DW_EK_KONTAKT")
    }
}

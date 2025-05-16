//! LPR_DIAG registry using the macro-based approach
//!
//! The LPR_DIAG registry contains diagnosis records from the Danish National Patient Registry.

use crate::RegistryTrait;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;

// Define LPR DIAG Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(
    name = "LPR_DIAG",
    description = "LPR Diagnosis Records",
    id_field = "RECNUM"
)]
pub struct LprDiagRegistry {
    // Core identification fields
    #[field(name = "RECNUM")]
    pub record_number: String,

    #[field(name = "C_DIAG")]
    pub diagnosis_code: Option<String>,

    #[field(name = "C_DIAGTYPE")]
    pub diagnosis_type: Option<String>,

    #[field(name = "C_TILDIAG")]
    pub additional_diagnosis: Option<String>,

    #[field(name = "LEVERANCEDATO")]
    pub delivery_date: Option<NaiveDate>,

    #[field(name = "VERSION")]
    pub version: Option<String>,
}

/// Helper function to create a new LPR diagnosis deserializer
pub fn create_deserializer() -> LprDiagRegistryDeserializer {
    LprDiagRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &LprDiagRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for LprDiagRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "lpr_diag"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for LPR_DIAG
        let fields = vec![
            Field::new("RECNUM", DataType::Utf8, false),
            Field::new("C_DIAG", DataType::Utf8, true),
            Field::new("C_DIAGTYPE", DataType::Utf8, true),
            Field::new("C_TILDIAG", DataType::Utf8, true),
            Field::new("LEVERANCEDATO", DataType::Date32, true),
            Field::new("VERSION", DataType::Utf8, true),
        ];

        std::sync::Arc::new(Schema::new(fields))
    }
}

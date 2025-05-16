//! DOD registry using the macro-based approach
//!
//! The DOD registry contains death records.

use crate::RegistryTrait;
use chrono::NaiveDate;

/// Death registry with death records
#[derive(RegistryTrait, Debug)]
#[registry(name = "DOD", description = "Death registry")]
pub struct DodRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Date of death
    #[field(name = "DODDATO")]
    pub death_date: Option<NaiveDate>,

    /// Cause of death (ICD-10 code)
    #[field(name = "C_AARSAG")]
    pub death_cause: Option<String>,

    /// Underlying cause of death
    #[field(name = "C_TILSTAND")]
    pub underlying_death_cause: Option<String>,
}

/// Helper function to create a new DOD deserializer
#[must_use] pub fn create_deserializer() -> DodRegistryDeserializer {
    DodRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &DodRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for DodRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "DOD"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for DOD
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("DODDATO", arrow::datatypes::DataType::Date32, true),
            arrow::datatypes::Field::new("C_AARSAG", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("C_TILSTAND", arrow::datatypes::DataType::Utf8, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

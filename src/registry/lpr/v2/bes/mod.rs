//! `LPR_BES` registry using the macro-based approach
//!
//! The `LPR_BES` registry contains outpatient visit records from the Danish National Patient Registry.

use crate::RegistryTrait;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;

// Define LPR BES Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(
    name = "LPR_BES",
    description = "LPR Outpatient Visits (besøg)",
    id_field = "RECNUM"
)]
pub struct LprBesRegistry {
    // Core identification fields
    #[field(name = "RECNUM")]
    pub record_number: String,

    // Admission-related fields
    #[field(name = "D_AMBDTO")]
    pub outpatient_visit_date: Option<NaiveDate>,

    #[field(name = "LEVERANCEDATO")]
    pub delivery_date: Option<NaiveDate>,

    #[field(name = "VERSION")]
    pub version: Option<String>,
}

/// Helper function to create a new LPR outpatient visit deserializer
#[must_use] pub fn create_deserializer() -> LprBesRegistryDeserializer {
    LprBesRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &LprBesRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for LprBesRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "lpr_bes"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for LPR_BES
        let fields = vec![
            Field::new("RECNUM", DataType::Utf8, false),
            Field::new("D_AMBDTO", DataType::Date32, true),
            Field::new("LEVERANCEDATO", DataType::Date32, true),
            Field::new("VERSION", DataType::Utf8, true),
        ];

        std::sync::Arc::new(Schema::new(fields))
    }
}

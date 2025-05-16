//! AKM registry using the macro-based approach
//!
//! The AKM (Arbejdsklassifikationsmodulet) registry contains employment information.

use crate::RegistryTrait;

/// Labour register with employment information
#[derive(RegistryTrait, Debug)]
#[registry(name = "AKM", description = "Labour register")]
pub struct AkmRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Socioeconomic status code
    #[field(name = "SOCIO13")]
    pub socioeconomic_status: Option<String>,
}

/// Helper function to create a new AKM deserializer
#[must_use] pub fn create_deserializer() -> AkmRegistryDeserializer {
    AkmRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &AkmRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for AkmRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "AKM"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for AKM
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("SOCIO13", arrow::datatypes::DataType::Utf8, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

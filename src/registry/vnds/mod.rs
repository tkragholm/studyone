//! VNDS registry using the macro-based approach
//!
//! The VNDS (Vandringer/Migration) registry contains migration information.

use crate::RegistryTrait;
use chrono::NaiveDate;

/// Migration registry with migration information
#[derive(RegistryTrait, Debug)]
#[registry(name = "VNDS", description = "Migration registry")]
pub struct VndsRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Migration code (in/out)
    #[field(name = "INDUD_KODE")]
    pub event_type: Option<String>,

    /// Event date
    #[field(name = "HAEND_DATO")]
    pub event_date: Option<NaiveDate>,
}

/// Helper function to create a new VNDS deserializer
#[must_use] pub fn create_deserializer() -> VndsRegistryDeserializer {
    VndsRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &VndsRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for VndsRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "VNDS"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for VNDS
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("INDUD_KODE", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("HAEND_DATO", arrow::datatypes::DataType::Date32, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

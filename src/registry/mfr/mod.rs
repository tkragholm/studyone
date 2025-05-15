//! MFR registry using the macro-based approach
//!
//! The MFR (Medical Birth Registry) registry contains birth information.

use crate::RegistryTrait;
use chrono::NaiveDate;

/// Medical Birth Registry with birth information
#[derive(RegistryTrait, Debug)]
#[registry(name = "MFR", description = "Medical Birth Registry")]
pub struct MfrRegistry {
    /// Child's personal identification number
    #[field(name = "CPR_BARN")]
    pub pnr: String,

    /// Birth date
    #[field(name = "FOEDSELSDATO")]
    pub birth_date: Option<NaiveDate>,

    /// Mother's personal identification number
    #[field(name = "CPR_MODER")]
    pub mother_pnr: Option<String>,

    /// Father's personal identification number
    #[field(name = "CPR_FADER")]
    pub father_pnr: Option<String>,
}

/// Helper function to create a new MFR deserializer
pub fn create_deserializer() -> MfrRegistryDeserializer {
    MfrRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &MfrRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for MfrRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "MFR"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for MFR
        let fields = vec![
            arrow::datatypes::Field::new("CPR_BARN", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("FOEDSELSDATO", arrow::datatypes::DataType::Date32, true),
            arrow::datatypes::Field::new("CPR_MODER", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("CPR_FADER", arrow::datatypes::DataType::Utf8, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }

    /// Returns the column name containing the PNR
    fn get_pnr_column_name(&self) -> Option<&'static str> {
        Some("CPR_BARN")
    }
}

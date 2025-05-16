//! Dodsaarsag (Cause of Death) registry using the macro-based approach
//!
//! The Dodsaarsag registry contains information about cause of death from death certificates.

use crate::RegistryTrait;
use chrono::NaiveDate;

/// Death causes registry with death certificate information
#[derive(RegistryTrait, Debug)]
#[registry(name = "DODSAARSAG", description = "Cause of Death registry")]
pub struct DodsaarsagRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Death cause (ICD-10 code)
    #[field(name = "C_AARSAG")]
    pub death_cause: Option<String>,

    /// Death condition (ICD-10 code)
    #[field(name = "C_TILSTAND")]
    pub death_condition: Option<String>,

    /// Death date
    #[field(name = "D_DATO")]
    pub death_date: Option<NaiveDate>,
}

/// Helper function to create a new Dodsaarsag deserializer
#[must_use] pub fn create_deserializer() -> DodsaarsagRegistryDeserializer {
    DodsaarsagRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &DodsaarsagRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for DodsaarsagRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "DODSAARSAG"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create Arrow schema for Dodsaarsag
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("C_AARSAG", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("C_TILSTAND", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("D_DATO", arrow::datatypes::DataType::Date32, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

// Re-export the standardized schema function for compatibility
pub use schema::dodsaarsag_standardized_schema;

mod schema {
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Returns a standardized schema for the Dodsaarsag register
    ///
    /// This schema provides normalized field names for the cause of death data
    #[must_use] pub fn dodsaarsag_standardized_schema() -> Arc<Schema> {
        let fields = vec![
            Field::new("PNR", DataType::Utf8, false),
            Field::new("DEATH_CAUSE", DataType::Utf8, true),
            Field::new("DEATH_CONDITION", DataType::Utf8, true),
            Field::new("DEATH_CAUSE_CHAPTER", DataType::Utf8, true),
            Field::new("DEATH_DATE", DataType::Date32, true),
        ];

        Arc::new(Schema::new(fields))
    }
}
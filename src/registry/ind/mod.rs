//! IND registry using the macro-based approach
//!
//! The IND (Indkomst) registry contains income and tax information.

use crate::RegistryTrait;

/// Income registry with tax information
#[derive(RegistryTrait, Debug)]
#[registry(name = "IND", description = "Income registry")]
pub struct IndRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Annual income
    #[field(name = "PERINDKIALT_13")]
    pub annual_income: Option<f64>,

    /// Employment income
    #[field(name = "LOENMV_13")]
    pub employment_income: Option<f64>,

    /// Version
    #[field(name = "VERSION")]
    pub version: Option<String>,

    /// Year
    #[field(name = "YEAR")]
    pub year: Option<i32>,
}

/// Helper function to create a new IND deserializer
#[must_use] pub fn create_deserializer() -> IndRegistryDeserializer {
    IndRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &IndRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for IndRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "IND"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for IND
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new(
                "PERINDKIALT_13",
                arrow::datatypes::DataType::Float64,
                true,
            ),
            arrow::datatypes::Field::new("LOENMV_13", arrow::datatypes::DataType::Float64, true),
            arrow::datatypes::Field::new("VERSION", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("YEAR", arrow::datatypes::DataType::Int32, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

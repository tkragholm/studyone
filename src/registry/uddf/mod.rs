//! UDDF registry using the macro-based approach
//!
//! The UDDF (Uddannelse) registry contains educational information.

use crate::RegistryTrait;
use chrono::NaiveDate;

/// Education registry with educational information
#[derive(RegistryTrait, Debug)]
#[registry(name = "UDDF", description = "Education registry")]
pub struct UddfRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// CPR check
    #[field(name = "CPRTJEK")]
    pub cpr_check: Option<String>,

    /// CPR type
    #[field(name = "CPRTYPE")]
    pub cpr_type: Option<String>,

    /// Highest completed education code
    #[field(name = "HFAUDD")]
    pub highest_education: Option<i32>,

    /// Source of education information
    #[field(name = "HF_KILDE")]
    pub education_source: Option<String>,

    /// Valid from date
    #[field(name = "HF_VFRA")]
    pub valid_from: Option<NaiveDate>,

    /// Valid to date
    #[field(name = "HF_VTIL")]
    pub valid_to: Option<NaiveDate>,

    /// Institution number
    #[field(name = "INSTNR")]
    pub institution_number: Option<i32>,

    /// Version
    #[field(name = "VERSION")]
    pub version: Option<String>,
}

/// Helper function to create a new UDDF deserializer
pub fn create_deserializer() -> UddfRegistryDeserializer {
    UddfRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &UddfRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for UddfRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "UDDF"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for UDDF
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("CPRTJEK", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("CPRTYPE", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("HFAUDD", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("HF_KILDE", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("HF_VFRA", arrow::datatypes::DataType::Date32, true),
            arrow::datatypes::Field::new("HF_VTIL", arrow::datatypes::DataType::Date32, true),
            arrow::datatypes::Field::new("INSTNR", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("VERSION", arrow::datatypes::DataType::Utf8, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

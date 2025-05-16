//! BEF registry using the macro-based approach
//!
//! The BEF (Befolkning) registry contains population demographic information.

use crate::RegistryTrait;
use chrono::NaiveDate;

/// Population registry with demographic information
#[derive(RegistryTrait, Debug)]
#[registry(name = "BEF", description = "Population registry")]
pub struct BefRegistry {
    /// Person ID (CPR number)
    #[field(name = "PNR")]
    pub pnr: String,

    /// Gender code
    #[field(name = "KOEN")]
    pub gender: Option<String>,

    /// Birth date
    #[field(name = "FOED_DAG")]
    pub birth_date: Option<NaiveDate>,

    /// Mother's person ID
    #[field(name = "MOR_ID")]
    pub mother_pnr: Option<String>,

    /// Father's person ID
    #[field(name = "FAR_ID")]
    pub father_pnr: Option<String>,

    /// Family ID
    #[field(name = "FAMILIE_ID")]
    pub family_id: Option<String>,

    /// Spouse's person ID
    #[field(name = "AEGTE_ID")]
    pub spouse_pnr: Option<String>,

    /// Age in years
    #[field(name = "ALDER")]
    pub age: Option<i32>,

    /// Number of persons in family
    #[field(name = "ANTPERSF")]
    pub family_size: Option<i32>,

    /// Number of persons in household
    #[field(name = "ANTPERSH")]
    pub household_size: Option<i32>,

    /// Date of residence from
    #[field(name = "BOP_VFRA")]
    pub residence_from: Option<NaiveDate>,

    /// Family type code
    #[field(name = "FAMILIE_TYPE")]
    pub family_type: Option<i32>,

    /// Immigration type
    /// 1: People of danish origin
    /// 2: Immigrants
    /// 3: Descendants
    #[field(name = "IE_TYPE")]
    pub immigration_type: Option<String>,

    /// Position in family
    #[field(name = "PLADS")]
    pub position_in_family: Option<i32>,
}

/// Helper function to create a new BEF deserializer
#[must_use] pub fn create_deserializer() -> BefRegistryDeserializer {
    BefRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &BefRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for BefRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "BEF"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for BEF
        let fields = vec![
            arrow::datatypes::Field::new("PNR", arrow::datatypes::DataType::Utf8, false),
            arrow::datatypes::Field::new("KOEN", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("FOED_DAG", arrow::datatypes::DataType::Date32, true),
            arrow::datatypes::Field::new("MOR_ID", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("FAR_ID", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("FAMILIE_ID", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("AEGTE_ID", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("ALDER", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("ANTPERSF", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("ANTPERSH", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("BOP_VFRA", arrow::datatypes::DataType::Date32, true),
            arrow::datatypes::Field::new("FAMILIE_TYPE", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("IE_TYPE", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("PLADS", arrow::datatypes::DataType::Int32, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}

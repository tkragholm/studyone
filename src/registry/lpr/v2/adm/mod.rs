//! LPR_ADM registry using the macro-based approach
//!
//! The LPR_ADM registry contains administrative records from the Danish National Patient Registry.

use crate::RegistryTrait;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;

// Define LPR ADM Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(name = "LPR_ADM", description = "LPR Administrative registry")]
pub struct LprAdmRegistry {
    // Core identification fields
    #[field(name = "PNR")]
    pub pnr: String,

    // Admission-related fields
    #[field(name = "C_ADIAG")]
    pub action_diagnosis: Option<String>,

    #[field(name = "C_AFD")]
    pub department_code: Option<String>,

    #[field(name = "C_KOM")]
    pub municipality_code: Option<String>,

    #[field(name = "D_INDDTO")]
    pub admission_date: Option<NaiveDate>,

    #[field(name = "D_UDDTO")]
    pub discharge_date: Option<NaiveDate>,

    #[field(name = "V_ALDER")]
    pub age: Option<i32>,

    #[field(name = "V_SENGDAGE")]
    pub length_of_stay: Option<i32>,

    #[field(name = "RECNUM")]
    pub record_number: Option<String>,
}

/// Helper function to create a new LPR admission deserializer
pub fn create_deserializer() -> LprAdmRegistryDeserializer {
    LprAdmRegistryDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &LprAdmRegistryDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for LprAdmRegistryDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "lpr_adm"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for LPR_ADM
        let fields = vec![
            Field::new("PNR", DataType::Utf8, false),
            Field::new("C_ADIAG", DataType::Utf8, true),
            Field::new("C_AFD", DataType::Utf8, true),
            Field::new("C_HAFD", DataType::Utf8, true),
            Field::new("C_HENM", DataType::Utf8, true),
            Field::new("C_HSGH", DataType::Utf8, true),
            Field::new("C_INDM", DataType::Utf8, true),
            Field::new("C_KOM", DataType::Utf8, true),
            Field::new("C_KONTAARS", DataType::Utf8, true),
            Field::new("C_PATTYPE", DataType::Utf8, true),
            Field::new("C_SGH", DataType::Utf8, true),
            Field::new("C_SPEC", DataType::Utf8, true),
            Field::new("C_UDM", DataType::Utf8, true),
            Field::new("CPRTJEK", DataType::Utf8, true),
            Field::new("CPRTYPE", DataType::Utf8, true),
            Field::new("D_HENDTO", DataType::Date32, true),
            Field::new("D_INDDTO", DataType::Date32, true),
            Field::new("D_UDDTO", DataType::Date32, true),
            Field::new("K_AFD", DataType::Utf8, true),
            Field::new("RECNUM", DataType::Utf8, true),
            Field::new("V_ALDDG", DataType::Int32, true),
            Field::new("V_ALDER", DataType::Int32, true),
            Field::new("V_INDMINUT", DataType::Int32, true),
            Field::new("V_INDTIME", DataType::Int32, true),
            Field::new("V_SENGDAGE", DataType::Int32, true),
            Field::new("V_UDTIME", DataType::Int32, true),
            Field::new("VERSION", DataType::Utf8, true),
        ];

        std::sync::Arc::new(Schema::new(fields))
    }
}

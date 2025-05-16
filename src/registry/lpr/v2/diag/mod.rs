use crate::{RegistryTrait, error, models, registry, schema};
use chrono::NaiveDate;

// Define LPR ADM Registry using the derive macro
#[derive(RegistryTrait, Debug)]
#[registry(name = "LPR_BES", description = "LPR Outpatient Visits (besøg)")]
struct LprBesRegister {
    // Core identification fields
    #[field(name = "RECNUM")]
    record_number: String,

    #[field(name = "C_DIAG")]
    outpatient_visit_date: Option<String>,

    #[field(name = "C_DIAGTYPE")]
    delivery_date: Option<String>,

    #[field(name = "C_TILDIAG")]
    version: Option<String>,

    #[field(name = "C_DIAGTYPE")]
    delivery_date: Option<String>,

    #[field(name = "VERSION")]
    version: Option<String>,
}

/// Helper function to create a new DOD deserializer
pub fn create_deserializer() -> LprBesRegisterDeserializer {
    LprBesRegisterDeserializer::new()
}

/// Helper function to deserialize a batch of records
pub fn deserialize_batch(
    deserializer: &LprBesRegisterDeserializer,
    batch: &crate::RecordBatch,
) -> crate::error::Result<Vec<crate::models::core::Individual>> {
    // Use the inner deserializer to deserialize the batch
    deserializer.inner.deserialize_batch(batch)
}

// Implement RegisterLoader for the macro-generated deserializer
impl crate::registry::RegisterLoader for LprBesRegisterDeserializer {
    /// Get the name of the register
    fn get_register_name(&self) -> &'static str {
        "lpr_adm"
    }

    /// Get the schema for this register
    fn get_schema(&self) -> crate::SchemaRef {
        // Create a simple Arrow schema for LPR_ADM
        let fields = vec![
            Field::new("C_DIAG", DataType::Utf8, true),
            Field::new("C_DIAGTYPE", DataType::Utf8, true),
            Field::new("C_TILDIAG", DataType::Utf8, true),
            Field::new("LEVERANCEDATO", DataType::Date32, true),
            Field::new("RECNUM", DataType::Utf8, true),
            Field::new("VERSION", DataType::Utf8, true),
        ];

        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    }
}
